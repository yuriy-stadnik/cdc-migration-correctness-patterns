resource "aws_iam_user_policy" "ec2_instance_connect" {
  name = "${var.project}-ec2-instance-connect"
  user = var.ec2_instance_connect_user_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ec2-instance-connect:SendSSHPublicKey"
        ]
        Resource = aws_instance.kafka_mm2.arn
        Condition = {
          StringEquals = {
            "ec2:osuser" = "ec2-user"
          }
        }
      }
    ]
  })
}
