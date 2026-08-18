data "aws_ami" "al2" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }
}
locals {
  msk_cluster_arn  = try(aws_msk_serverless_cluster.this[0].arn, null)
  msk_cluster_name = try(aws_msk_serverless_cluster.this[0].cluster_name, null)
}
data "aws_caller_identity" "current" {}

resource "aws_iam_role" "ec2" {
  name = "${var.project}-ec2-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow",
      Principal = { Service = "ec2.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
}

# NOTE: For a lab, easiest is broad kafka-cluster permissions.
# Tighten later once everything works (least privilege).
resource "aws_iam_role_policy" "ec2_inline" {
  name = "${var.project}-ec2-inline"
  role = aws_iam_role.ec2.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = concat([
      # ----------------------------
      # 1) Control plane (MSK API)
      #    Needed for: list-clusters, get-bootstrap-brokers, describe-cluster
      # ----------------------------
      {
        Sid    = "MskControlPlaneDiscovery"
        Effect = "Allow"
        Action = [
          "kafka:ListClusters",
          "kafka:ListClustersV2",
          "kafka:DescribeCluster",
          "kafka:DescribeClusterV2",
          "kafka:GetBootstrapBrokers",
          "kafka:ListNodes"
        ]
        Resource = "*"
      }
      ], var.enable_msk ? [
      # ----------------------------
      # 2) Data plane (MSK IAM auth)
      #    Needed for: connect + topic/group ops + read/write
      # ----------------------------
      {
        Sid    = "MskIamForMm2Lab",
        Effect = "Allow",
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeCluster",
          "kafka-cluster:DescribeClusterV2",
          "kafka-cluster:DescribeClusterDynamicConfiguration",
          "kafka-cluster:WriteDataIdempotently",
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:DescribeGroup",
          "kafka-cluster:DescribeTransactionalId",
          "kafka-cluster:AlterTransactionalId",

          # Needed for MM2 / Connect internal topics
          "kafka-cluster:CreateTopic",
          "kafka-cluster:AlterTopic",
          "kafka-cluster:DeleteTopic",

          # Data plane
          "kafka-cluster:ReadData",
          "kafka-cluster:WriteData",

          # Consumer group ops (MM2 uses groups)
          "kafka-cluster:AlterGroup"
        ],
        Resource = [
          # Cluster ARN itself (good to include)
          local.msk_cluster_arn,

          # Data-plane resources (topic/group/transactional-id).
          # Use wildcards for <cluster-uuid> and the resource names.
          "arn:aws:kafka:${var.aws_region}:${data.aws_caller_identity.current.account_id}:topic/${local.msk_cluster_name}/*/*",
          "arn:aws:kafka:${var.aws_region}:${data.aws_caller_identity.current.account_id}:group/${local.msk_cluster_name}/*/*",
          "arn:aws:kafka:${var.aws_region}:${data.aws_caller_identity.current.account_id}:transactional-id/${local.msk_cluster_name}/*/*"
        ]
      }
    ] : [])
  })
}

resource "aws_iam_role_policy_attachment" "ec2_ssm" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "ec2" {
  name = "${var.project}-ec2-profile"
  role = aws_iam_role.ec2.name
}

resource "aws_instance" "kafka_mm2" {
  ami                         = data.aws_ami.al2.id
  instance_type               = var.ec2_instance_type
  subnet_id                   = aws_subnet.public[0].id
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.ec2.id]
  iam_instance_profile        = aws_iam_instance_profile.ec2.name
  user_data_base64 = base64gzip(templatefile("${path.module}/user_data.sh.tpl", {
    msk_bootstrap_iam   = var.ec2_msk_bootstrap_iam
    mm2_topic_allowlist = var.mm2_topic_allowlist_regex
    mm2_group_allowlist = var.mm2_group_allowlist_regex
    region              = var.aws_region
  }))

  root_block_device {
    volume_size           = var.ec2_root_volume_size
    volume_type           = "gp3"
    delete_on_termination = true
  }

  tags = { Name = "${var.project}-ec2-kafka-mm2" }
}

resource "aws_instance" "lambda_net_test" {
  ami           = data.aws_ami.al2.id
  instance_type = "t3.micro"

  subnet_id              = aws_subnet.private[0].id
  vpc_security_group_ids = [aws_security_group.lambda.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2.name

  user_data = <<EOF
#!/bin/bash
set -euo pipefail
yum update -y
yum install -y nc java-17-amazon-corretto-headless
EOF

  tags = { Name = "${var.project}-lambda-net-test" }
}
