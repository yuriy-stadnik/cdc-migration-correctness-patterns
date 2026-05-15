variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "enable_msk" {
  description = "When true, provision MSK Serverless and MSK-triggered Lambda mappings. Keep false for local-only or EC2-only testing to reduce cost."
  type        = bool
  default     = false
}

variable "project" {
  type    = string
  default = "kafka-mm2-msk-lab"
}

variable "ec2_instance_type" {
  type    = string
  default = "t3.large"
}

variable "ec2_root_volume_size" {
  description = "Root EBS volume size in GiB for the Kafka/MM2 EC2 host."
  type        = number
  default     = 80
}

variable "ec2_msk_bootstrap_iam" {
  description = "Optional MSK IAM bootstrap string for MirrorMaker2 on the EC2 host. Leave empty for EC2-only PostgreSQL, CDC, Kafka, and Flink testing."
  type        = string
  default     = ""
}

variable "mm2_topic_allowlist_regex" {
  description = "Regex for MM2 source->target topic replication when MSK replication is enabled."
  type        = string
  default     = "(operational[.].*|client[.].*|pg1[.]transaction)"
}

variable "mm2_group_allowlist_regex" {
  description = "Regex for MM2 source->target consumer group sync when MSK replication is enabled."
  type        = string
  default     = "__no_groups__"
}

variable "ssh_cidr" {
  description = "Your public IP /32 for SSH"
  type        = string
  default     = "0.0.0.0/32"
}

variable "ec2_instance_connect_user_name" {
  description = "IAM user allowed to push temporary SSH public keys with EC2 Instance Connect."
  type        = string
  default     = "terraform"
}

variable "db_name" {
  type    = string
  default = "appdb"
}

variable "db_username" {
  type    = string
  default = "appuser"
}

variable "client_db_name" {
  description = "Aurora database name for client projection data."
  type        = string
  default     = "clientdb"
}

variable "operational_db_name" {
  description = "Aurora database name for operational projection data."
  type        = string
  default     = "operationaldb"
}

variable "aurora_instance_class" {
  description = "Instance class for Aurora PostgreSQL instances."
  type        = string
  default     = "db.t3.medium"
}
