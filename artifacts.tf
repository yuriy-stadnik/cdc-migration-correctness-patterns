resource "aws_s3_bucket" "cdc_artifacts" {
  bucket = "${var.project}-${data.aws_caller_identity.current.account_id}-${var.aws_region}-artifacts"
}

resource "aws_s3_bucket_versioning" "cdc_artifacts" {
  bucket = aws_s3_bucket.cdc_artifacts.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_object" "transaction_projector" {
  bucket = aws_s3_bucket.cdc_artifacts.id
  key    = "transaction-projector/${filesha256("${path.module}/transaction-projector/build/libs/transaction-projector-all.jar")}/transaction-projector-all.jar"
  source = "${path.module}/transaction-projector/build/libs/transaction-projector-all.jar"
  etag   = filemd5("${path.module}/transaction-projector/build/libs/transaction-projector-all.jar")
}

resource "aws_iam_role_policy" "ec2_artifact_read" {
  name = "${var.project}-ec2-artifact-read"
  role = aws_iam_role.ec2.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject"]
      Resource = aws_s3_object.transaction_projector.arn
    }]
  })
}
