-- Source-owned versions for the normalized one-source-row/one-destination-row model.
-- Install this schema in a fresh lab database.
BEGIN;
SELECT pg_advisory_xact_lock(772401, 2);

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'source_writer') THEN
    CREATE ROLE source_writer NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'source_app') THEN
    CREATE ROLE source_app LOGIN PASSWORD 'source_app'; -- local lab credential only
  END IF;
END $$;
GRANT source_writer TO source_app;
GRANT CONNECT ON DATABASE appdb TO source_app;

CREATE SCHEMA IF NOT EXISTS source_cdc AUTHORIZATION postgres;
ALTER SCHEMA inventory OWNER TO postgres;
REVOKE ALL ON SCHEMA inventory FROM PUBLIC;
GRANT USAGE ON SCHEMA inventory TO dbz, source_writer;
GRANT USAGE ON SCHEMA source_cdc TO dbz, source_writer;

CREATE TABLE IF NOT EXISTS source_cdc.schema_version (
  version INTEGER PRIMARY KEY,
  installed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS source_cdc.owner_state (
  collection TEXT NOT NULL,
  owner_key TEXT NOT NULL,
  version BIGINT NOT NULL CHECK (version > 0),
  presence TEXT NOT NULL CHECK (presence IN ('PRESENT','DELETED')),
  last_source_transaction_id UUID,
  PRIMARY KEY (collection, owner_key)
);
CREATE TABLE IF NOT EXISTS source_cdc.transaction_context (
  full_xid TEXT PRIMARY KEY,
  source_transaction_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
  journal_count BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS source_cdc.revision_journal (
  source_transaction_id UUID NOT NULL REFERENCES source_cdc.transaction_context(source_transaction_id),
  ordinal BIGINT NOT NULL,
  collection TEXT NOT NULL,
  owner_key TEXT NOT NULL,
  before_version BIGINT,
  after_version BIGINT NOT NULL CHECK (after_version > 0),
  before_presence TEXT NOT NULL CHECK (before_presence IN ('ABSENT','PRESENT','DELETED')),
  after_presence TEXT NOT NULL CHECK (after_presence IN ('PRESENT','DELETED')),
  before_image JSONB,
  after_image JSONB,
  PRIMARY KEY (source_transaction_id, ordinal),
  UNIQUE (collection, owner_key, source_transaction_id, after_version)
);
CREATE INDEX IF NOT EXISTS revision_journal_owner
  ON source_cdc.revision_journal(collection, owner_key, after_version);

CREATE OR REPLACE FUNCTION source_cdc.require_context() RETURNS UUID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, source_cdc AS $$
DECLARE v_id UUID;
BEGIN
  SELECT source_transaction_id INTO v_id
    FROM source_cdc.transaction_context WHERE full_xid = pg_current_xact_id()::text;
  IF v_id IS NULL THEN
    RAISE EXCEPTION 'call source_cdc.begin_write() before source DML';
  END IF;
  RETURN v_id;
END $$;

CREATE OR REPLACE FUNCTION source_cdc.begin_write() RETURNS UUID
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, source_cdc AS $$
DECLARE v_id UUID;
BEGIN
  INSERT INTO source_cdc.transaction_context(full_xid)
    VALUES (pg_current_xact_id()::text) ON CONFLICT (full_xid) DO NOTHING;
  SELECT source_transaction_id INTO STRICT v_id
    FROM source_cdc.transaction_context WHERE full_xid = pg_current_xact_id()::text;
  RETURN v_id;
END $$;

CREATE OR REPLACE FUNCTION source_cdc.business_guard() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, source_cdc AS $$
BEGIN
  PERFORM source_cdc.require_context();
  RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION source_cdc.row_key(p_row JSONB, p_columns TEXT[]) RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE v_key JSONB := '{}'::jsonb; v_column TEXT;
BEGIN
  FOREACH v_column IN ARRAY p_columns LOOP
    IF NOT p_row ? v_column OR p_row->v_column IS NULL OR p_row->v_column = 'null'::jsonb THEN
      RAISE EXCEPTION 'source owner key column % is null', v_column;
    END IF;
    v_key := v_key || jsonb_build_object(v_column, p_row->v_column);
  END LOOP;
  RETURN v_key::text;
END $$;

CREATE OR REPLACE FUNCTION source_cdc.record_revision(
  p_collection TEXT, p_key TEXT, p_before_version BIGINT, p_after_presence TEXT,
  p_before_image JSONB, p_after_image JSONB, p_before_presence TEXT DEFAULT 'PRESENT'
) RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, source_cdc AS $$
DECLARE v_tx UUID; v_prior BIGINT; v_presence TEXT; v_next BIGINT; v_ordinal BIGINT;
BEGIN
  v_tx := source_cdc.require_context();
  SELECT version, presence INTO v_prior, v_presence
    FROM source_cdc.owner_state WHERE collection=p_collection AND owner_key=p_key FOR UPDATE;
  IF v_prior IS NULL THEN
    IF p_before_version IS NOT NULL THEN RAISE EXCEPTION 'unknown owner % %',p_collection,p_key; END IF;
    v_next := 1;
  ELSE
    IF p_before_version IS DISTINCT FROM v_prior OR v_presence IS DISTINCT FROM p_before_presence THEN
      RAISE EXCEPTION 'owner version mismatch % % expected %, found % %',p_collection,p_key,p_before_version,v_prior,v_presence;
    END IF;
    IF v_prior = 9223372036854775807 THEN RAISE EXCEPTION 'owner version overflow % %',p_collection,p_key; END IF;
    v_next := v_prior+1;
  END IF;
  INSERT INTO source_cdc.owner_state(collection,owner_key,version,presence,last_source_transaction_id)
    VALUES(p_collection,p_key,v_next,p_after_presence,v_tx)
    ON CONFLICT(collection,owner_key) DO UPDATE SET
      version=EXCLUDED.version,presence=EXCLUDED.presence,last_source_transaction_id=EXCLUDED.last_source_transaction_id;
  UPDATE source_cdc.transaction_context SET journal_count=journal_count+1
    WHERE full_xid=pg_current_xact_id()::text RETURNING journal_count INTO v_ordinal;
  INSERT INTO source_cdc.revision_journal(source_transaction_id,ordinal,collection,owner_key,
    before_version,after_version,before_presence,after_presence,before_image,after_image)
  VALUES(v_tx,v_ordinal,p_collection,p_key,v_prior,v_next,COALESCE(v_presence,'ABSENT'),
    p_after_presence,p_before_image,p_after_image);
  RETURN v_next;
END $$;

CREATE OR REPLACE FUNCTION source_cdc.business_before() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, source_cdc AS $$
DECLARE v_collection TEXT := TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME;
        v_old_key TEXT; v_new_key TEXT; v_version BIGINT; v_presence TEXT;
BEGIN
  PERFORM source_cdc.require_context();
  IF TG_OP <> 'INSERT' THEN
    v_old_key := source_cdc.row_key(to_jsonb(OLD), TG_ARGV);
    SELECT version,presence INTO v_version,v_presence FROM source_cdc.owner_state
      WHERE collection=v_collection AND owner_key=v_old_key FOR UPDATE;
    IF v_version IS DISTINCT FROM OLD.version OR v_presence IS DISTINCT FROM 'PRESENT' THEN
      RAISE EXCEPTION 'business row and registry disagree % %',v_collection,v_old_key;
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  END IF;
  v_new_key := source_cdc.row_key(to_jsonb(NEW), TG_ARGV);
  IF TG_OP = 'UPDATE' AND v_new_key <> v_old_key THEN
    RAISE EXCEPTION 'primary-key changes are not supported; delete and insert explicitly';
  END IF;
  IF TG_OP = 'UPDATE' THEN
    IF OLD.version = 9223372036854775807 THEN RAISE EXCEPTION 'owner version overflow'; END IF;
    NEW.version := OLD.version+1;
  ELSE
    SELECT version INTO v_version FROM source_cdc.owner_state
      WHERE collection=v_collection AND owner_key=v_new_key FOR UPDATE;
    IF v_version = 9223372036854775807 THEN RAISE EXCEPTION 'owner version overflow'; END IF;
    NEW.version := COALESCE(v_version+1,1);
  END IF;
  RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION source_cdc.business_after() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, source_cdc AS $$
DECLARE v_collection TEXT := TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME;
        v_key TEXT; v_prior BIGINT; v_presence TEXT; v_recorded BIGINT;
BEGIN
  IF TG_OP = 'DELETE' THEN
    v_key := source_cdc.row_key(to_jsonb(OLD), TG_ARGV);
    PERFORM source_cdc.record_revision(v_collection,v_key,OLD.version,'DELETED',to_jsonb(OLD),NULL);
    RETURN NULL;
  END IF;
  v_key := source_cdc.row_key(to_jsonb(NEW), TG_ARGV);
  IF TG_OP = 'INSERT' THEN
    SELECT version,presence INTO v_prior,v_presence FROM source_cdc.owner_state
      WHERE collection=v_collection AND owner_key=v_key;
  ELSE
    v_prior := OLD.version;
    v_presence := 'PRESENT';
  END IF;
  v_recorded := source_cdc.record_revision(v_collection,v_key,v_prior,'PRESENT',
    CASE WHEN TG_OP='UPDATE' THEN to_jsonb(OLD) ELSE NULL END,to_jsonb(NEW),COALESCE(v_presence,'ABSENT'));
  IF v_recorded IS DISTINCT FROM NEW.version THEN
    RAISE EXCEPTION 'stored version and journal disagree % %',v_collection,v_key;
  END IF;
  RETURN NULL;
END $$;

DO $$
DECLARE v_table TEXT;
BEGIN
  FOREACH v_table IN ARRAY ARRAY['customers','products','orders','order_items'] LOOP
    EXECUTE format('ALTER TABLE inventory.%I OWNER TO postgres',v_table);
    EXECUTE format('ALTER TABLE inventory.%I ADD COLUMN IF NOT EXISTS version BIGINT',v_table);
    EXECUTE format('UPDATE inventory.%I SET version=1 WHERE version IS NULL',v_table);
    EXECUTE format('ALTER TABLE inventory.%I ALTER COLUMN version SET NOT NULL',v_table);
    EXECUTE format('ALTER TABLE inventory.%I DROP CONSTRAINT IF EXISTS %I',v_table,v_table||'_version_positive');
    EXECUTE format('ALTER TABLE inventory.%I ADD CONSTRAINT %I CHECK (version > 0)',v_table,v_table||'_version_positive');
    EXECUTE format('ALTER TABLE inventory.%I REPLICA IDENTITY FULL',v_table);
  END LOOP;
END $$;

INSERT INTO source_cdc.owner_state(collection,owner_key,version,presence)
SELECT 'inventory.customers',source_cdc.row_key(to_jsonb(t),ARRAY['id']),version,'PRESENT' FROM inventory.customers t ON CONFLICT DO NOTHING;
INSERT INTO source_cdc.owner_state(collection,owner_key,version,presence)
SELECT 'inventory.products',source_cdc.row_key(to_jsonb(t),ARRAY['name']),version,'PRESENT' FROM inventory.products t ON CONFLICT DO NOTHING;
INSERT INTO source_cdc.owner_state(collection,owner_key,version,presence)
SELECT 'inventory.orders',source_cdc.row_key(to_jsonb(t),ARRAY['id']),version,'PRESENT' FROM inventory.orders t ON CONFLICT DO NOTHING;
INSERT INTO source_cdc.owner_state(collection,owner_key,version,presence)
SELECT 'inventory.order_items',source_cdc.row_key(to_jsonb(t),ARRAY['order_id','product_name']),version,'PRESENT' FROM inventory.order_items t ON CONFLICT DO NOTHING;

DO $$
DECLARE v_table TEXT; v_keys TEXT;
BEGIN
  FOR v_table,v_keys IN VALUES
    ('customers','''id'''),('products','''name'''),
    ('orders','''id'''),('order_items','''order_id'',''product_name''')
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS source_guard ON inventory.%I',v_table);
    EXECUTE format('DROP TRIGGER IF EXISTS source_version ON inventory.%I',v_table);
    EXECUTE format('DROP TRIGGER IF EXISTS source_revision ON inventory.%I',v_table);
    EXECUTE format('CREATE TRIGGER source_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON inventory.%I FOR EACH STATEMENT EXECUTE FUNCTION source_cdc.business_guard()',v_table);
    EXECUTE format('CREATE TRIGGER source_version BEFORE INSERT OR UPDATE OR DELETE ON inventory.%I FOR EACH ROW EXECUTE FUNCTION source_cdc.business_before(%s)',v_table,v_keys);
    EXECUTE format('CREATE TRIGGER source_revision AFTER INSERT OR UPDATE OR DELETE ON inventory.%I FOR EACH ROW EXECUTE FUNCTION source_cdc.business_after(%s)',v_table,v_keys);
  END LOOP;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname='source_cdc_v2') THEN
    ALTER PUBLICATION source_cdc_v2 SET TABLE
      inventory.customers,inventory.products,inventory.orders,inventory.order_items,
      source_cdc.transaction_context,source_cdc.revision_journal;
  ELSE
    CREATE PUBLICATION source_cdc_v2 FOR TABLE
      inventory.customers,inventory.products,inventory.orders,inventory.order_items,
      source_cdc.transaction_context,source_cdc.revision_journal;
  END IF;
END $$;

REVOKE ALL ON ALL TABLES IN SCHEMA inventory FROM dbz,source_writer,PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA inventory TO dbz;
GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA inventory TO source_writer;
REVOKE ALL ON ALL TABLES IN SCHEMA source_cdc FROM dbz,source_writer,PUBLIC;
GRANT SELECT ON source_cdc.transaction_context,source_cdc.revision_journal TO dbz;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA source_cdc FROM PUBLIC,dbz,source_writer;
GRANT EXECUTE ON FUNCTION source_cdc.begin_write() TO source_writer;

INSERT INTO source_cdc.schema_version(version) VALUES(3) ON CONFLICT DO NOTHING;
COMMIT;
