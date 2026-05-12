data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.project}-lambda-consumer-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "lambda_basic_exec" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_vpc_access" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# Minimum required permissions for MSK event source mappings
resource "aws_iam_role_policy_attachment" "lambda_msk_exec" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaMSKExecutionRole"
}

# Lambda needs to read DB credentials from Secrets Manager
resource "aws_iam_role_policy" "lambda_secrets" {
  name = "${var.project}-lambda-secrets"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = ["secretsmanager:GetSecretValue"],
        Resource = try(aws_rds_cluster.aurora.master_user_secret[0].secret_arn, "*")
      },
      {
        Effect   = "Allow",
        Action   = ["kms:Decrypt"],
        Resource = "*"
      }
    ]
  })
}
resource "aws_iam_role_policy" "lambda_msk_control_plane" {
  name = "${var.project}-lambda-msk-control-plane"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "MskControlPlaneDiscovery",
        Effect = "Allow",
        Action = [
          "kafka:GetBootstrapBrokers",
          "kafka:DescribeCluster",
          "kafka:DescribeClusterV2",
          "kafka:ListNodes"
        ],
        Resource = "*"
      }
    ]
  })
}

resource "aws_lambda_function" "consumer" {
  function_name = "${var.project}-msk-to-aurora"
  role          = aws_iam_role.lambda.arn

  runtime = "java17"
  handler = "com.example.Handler::handleRequest"

  timeout     = 30
  memory_size = 1024

  filename         = "${path.module}/lambda_java/build/libs/msk-to-aurora-lambda-1.0.0.jar"
  source_code_hash = filebase64sha256("${path.module}/lambda_java/build/libs/msk-to-aurora-lambda-1.0.0.jar")

  vpc_config {
    subnet_ids         = [aws_subnet.private[0].id, aws_subnet.private[1].id]
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      DB_HOST       = aws_rds_cluster.aurora.endpoint
      DB_PORT       = "5432"
      DB_NAME       = var.db_name
      DB_USER       = var.db_username
      DB_SECRET_ARN = try(aws_rds_cluster.aurora.master_user_secret[0].secret_arn, "")
    }
  }

  depends_on = [aws_rds_cluster_instance.aurora_instance]
}

locals {
  lambda_msk_topics = toset([
    "operational.products",
    "operational.orders",
    "operational.order_items",
    "operational.addresses",
    "operational.contact_numbers"
  ])
}

# MSK -> Lambda triggers. Lambda supports one Kafka topic per event source mapping.
resource "aws_lambda_event_source_mapping" "msk_operational" {
  for_each = local.lambda_msk_topics

  event_source_arn = aws_msk_serverless_cluster.this.arn
  function_name    = aws_lambda_function.consumer.arn

  topics            = [each.value]
  starting_position = "TRIM_HORIZON"

  # conservative defaults for a DB-writer Lambda
  batch_size                         = 50
  maximum_batching_window_in_seconds = 2

  enabled = true

  depends_on = [
    aws_lambda_function.consumer
  ]
}

resource "aws_iam_role_policy" "lambda_msk_cluster_iam" {
  name = "${var.project}-lambda-msk-cluster-iam"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:ReadData",
          "kafka-cluster:DescribeGroup",
          "kafka-cluster:AlterGroup"
        ],
        Resource = [
          aws_msk_serverless_cluster.this.arn,
          "arn:aws:kafka:${var.aws_region}:${data.aws_caller_identity.current.account_id}:topic/${aws_msk_serverless_cluster.this.cluster_name}/*/*",
          "arn:aws:kafka:${var.aws_region}:${data.aws_caller_identity.current.account_id}:group/${aws_msk_serverless_cluster.this.cluster_name}/*/*",
          "arn:aws:kafka:${var.aws_region}:${data.aws_caller_identity.current.account_id}:transactional-id/${aws_msk_serverless_cluster.this.cluster_name}/*/*"
        ]
      }
    ]
  })
}
