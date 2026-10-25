resource "aws_db_subnet_group" "aurora" {
  name       = "${var.project}-aurora-subnets"
  subnet_ids = [aws_subnet.private[0].id, aws_subnet.private[1].id]
}

resource "aws_rds_cluster" "aurora_destination" {
  cluster_identifier = "${var.project}-aurora-destination"

  engine         = "aurora-postgresql"
  engine_version = "16.8"

  database_name               = var.destination_db_name
  master_username             = var.db_username
  manage_master_user_password = true
  db_subnet_group_name        = aws_db_subnet_group.aurora.name
  vpc_security_group_ids      = [aws_security_group.aurora.id]
  enable_http_endpoint        = false
  backup_retention_period     = 1
  skip_final_snapshot         = true
}

resource "aws_rds_cluster_instance" "aurora_destination_instance" {
  identifier          = "${var.project}-aurora-destination-1"
  cluster_identifier  = aws_rds_cluster.aurora_destination.id
  instance_class      = var.aurora_instance_class
  engine              = aws_rds_cluster.aurora_destination.engine
  engine_version      = aws_rds_cluster.aurora_destination.engine_version
  publicly_accessible = false
}
