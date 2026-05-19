output "vpc_id" {
  value = aws_vpc.this.id
}

output "ec2_public_ip" {
  description = "Public IP address of the Kafka/MM2 EC2 instance."
  value       = aws_instance.kafka_mm2.public_ip
}

output "ec2_instance_id" {
  description = "Instance ID of the Kafka/MM2 EC2 instance."
  value       = aws_instance.kafka_mm2.id
}

output "ec2_private_ip" {
  description = "Private IP address of the Kafka/MM2 EC2 instance."
  value       = aws_instance.kafka_mm2.private_ip
}

output "sg_ec2_id" {
  description = "Security group ID attached to the Kafka/MM2 EC2 instance."
  value       = aws_security_group.ec2.id
}

output "subnet_public_ids" {
  description = "Public subnet IDs."
  value       = [aws_subnet.public[0].id, aws_subnet.public[1].id]
}

output "lambda_net_test_private_ip" {
  description = "Private IP address of the Lambda network test EC2 instance."
  value       = aws_instance.lambda_net_test.private_ip
}

data "aws_msk_bootstrap_brokers" "this" {
  count       = var.enable_msk ? 1 : 0
  cluster_arn = aws_msk_serverless_cluster.this[0].arn
}

output "msk_bootstrap_sasl_iam" {
  value = try(data.aws_msk_bootstrap_brokers.this[0].bootstrap_brokers_sasl_iam, null)
}

output "aurora_client_endpoint" {
  value = aws_rds_cluster.aurora_client.endpoint
}

output "aurora_operational_endpoint" {
  value = aws_rds_cluster.aurora_operational.endpoint
}

output "aurora_endpoint" {
  description = "Backward-compatible alias for operational Aurora endpoint."
  value       = aws_rds_cluster.aurora_operational.endpoint
}

output "msk_cluster_arn" {
  value = try(aws_msk_serverless_cluster.this[0].arn, null)
}
output "aurora_client_master_secret_arn" {
  value = try(aws_rds_cluster.aurora_client.master_user_secret[0].secret_arn, null)
}

output "aurora_operational_master_secret_arn" {
  value = try(aws_rds_cluster.aurora_operational.master_user_secret[0].secret_arn, null)
}

output "aurora_master_secret_arn" {
  description = "Backward-compatible alias for operational Aurora secret ARN."
  value       = try(aws_rds_cluster.aurora_operational.master_user_secret[0].secret_arn, null)
}

output "aurora_port" {
  value = 5432
}
output "sg_lambda_id" { value = aws_security_group.lambda.id }
output "sg_msk_id" { value = aws_security_group.msk.id }
output "subnet_private_ids" { value = [aws_subnet.private[0].id, aws_subnet.private[1].id] }
