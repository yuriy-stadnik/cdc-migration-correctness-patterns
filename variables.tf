variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project" {
  type    = string
  default = "kafka-mm2-msk-lab"
}

variable "ec2_instance_type" {
  type    = string
  default = "t3.large"
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
