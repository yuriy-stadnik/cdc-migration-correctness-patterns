resource "aws_db_subnet_group" "aurora" {
  name       = "${var.project}-aurora-subnets"
  subnet_ids = [aws_subnet.private[0].id, aws_subnet.private[1].id]
}

resource "aws_rds_cluster" "aurora_client" {
  cluster_identifier = "${var.project}-aurora-client"

  engine         = "aurora-postgresql"
  engine_version = "16.8"

  database_name               = var.client_db_name
  master_username             = var.db_username
  manage_master_user_password = true
  db_subnet_group_name        = aws_db_subnet_group.aurora.name
  vpc_security_group_ids      = [aws_security_group.aurora.id]
  enable_http_endpoint        = false
  backup_retention_period     = 1
  skip_final_snapshot         = true
}

resource "aws_rds_cluster_instance" "aurora_client_instance" {
  identifier          = "${var.project}-aurora-client-1"
  cluster_identifier  = aws_rds_cluster.aurora_client.id
  instance_class      = var.aurora_instance_class
  engine              = aws_rds_cluster.aurora_client.engine
  engine_version      = aws_rds_cluster.aurora_client.engine_version
  publicly_accessible = false
}

resource "aws_rds_cluster" "aurora_operational" {
  cluster_identifier = "${var.project}-aurora-operational"

  engine         = "aurora-postgresql"
  engine_version = "16.8"

  database_name               = var.operational_db_name
  master_username             = var.db_username
  manage_master_user_password = true
  db_subnet_group_name        = aws_db_subnet_group.aurora.name
  vpc_security_group_ids      = [aws_security_group.aurora.id]
  enable_http_endpoint        = false
  backup_retention_period     = 1
  skip_final_snapshot         = true
}

resource "aws_rds_cluster_instance" "aurora_operational_instance" {
  identifier          = "${var.project}-aurora-operational-1"
  cluster_identifier  = aws_rds_cluster.aurora_operational.id
  instance_class      = var.aurora_instance_class
  engine              = aws_rds_cluster.aurora_operational.engine
  engine_version      = aws_rds_cluster.aurora_operational.engine_version
  publicly_accessible = false
}
