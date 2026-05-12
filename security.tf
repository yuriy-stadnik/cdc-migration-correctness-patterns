resource "aws_security_group" "ec2" {
  name        = "${var.project}-ec2-sg"
  description = "EC2 for local Kafka + MM2"
  vpc_id      = aws_vpc.this.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }

  # Optional: expose local Kafka for testing (limit to your IP)
  ingress {
    description = "Local Kafka demo (optional)"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }

  ingress {
    description = "Kafka UI on on-prem emulator"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }

  ingress {
    description = "Flink UI on on-prem emulator"
    from_port   = 8081
    to_port     = 8081
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }

  ingress {
    description = "Debezium Connect REST on on-prem emulator"
    from_port   = 8083
    to_port     = 8083
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "msk" {
  name        = "${var.project}-msk-sg"
  description = "MSK brokers"
  vpc_id      = aws_vpc.this.id

  # MSK IAM port inside AWS is typically 9098; TLS 9094; SCRAM 9096.
  # We enable IAM below, so allow 9098 from EC2 and Lambda.
  ingress {
    description     = "MSK IAM from EC2"
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [aws_security_group.ec2.id]
  }

  ingress {
    description     = "MSK IAM from Lambda"
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }

  ingress {
    description = "MSK IAM from Lambda event source mapping ENIs"
    from_port   = 9098
    to_port     = 9098
    protocol    = "tcp"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "lambda" {
  name        = "${var.project}-lambda-sg"
  description = "Lambda in VPC"
  vpc_id      = aws_vpc.this.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "aurora" {
  name        = "${var.project}-aurora-sg"
  description = "Aurora PostgreSQL"
  vpc_id      = aws_vpc.this.id

  ingress {
    description     = "Postgres from Lambda SG"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda.id]
  }

  # Optional: allow EC2 (for debugging)
  ingress {
    description     = "Postgres from EC2 SG (optional)"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.ec2.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
