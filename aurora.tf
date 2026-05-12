resource "aws_db_subnet_group" "aurora" {
  name       = "${var.project}-aurora-subnets"
  subnet_ids = [aws_subnet.private[0].id, aws_subnet.private[1].id]
}

resource "aws_rds_cluster" "aurora" {
  cluster_identifier = "${var.project}-aurora"

  engine         = "aurora-postgresql"
  engine_version = "16.8" # adjust if region differs

  database_name   = var.db_name
  master_username = var.db_username

  # AWS manages master password in Secrets Manager
  manage_master_user_password = true

  db_subnet_group_name   = aws_db_subnet_group.aurora.name
  vpc_security_group_ids = [aws_security_group.aurora.id]

  # IMPORTANT: Data API OFF for provisioned Aurora
  enable_http_endpoint = false

  backup_retention_period = 1
  skip_final_snapshot     = true
}

resource "aws_rds_cluster_instance" "aurora_instance" {
  identifier         = "${var.project}-aurora-1"
  cluster_identifier = aws_rds_cluster.aurora.id

  # Provisioned instance class
  instance_class = "db.t3.medium"

  engine         = aws_rds_cluster.aurora.engine
  engine_version = aws_rds_cluster.aurora.engine_version

  publicly_accessible = false
}