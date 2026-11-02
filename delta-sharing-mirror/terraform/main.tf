data "aws_partition" "current" {}
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  partition  = data.aws_partition.current.partition # "aws-us-gov" in GovCloud; never hardcoded
  account_id = data.aws_caller_identity.current.account_id
  name       = "${var.name_prefix}-${var.environment}"

  lake_bucket_arn = "arn:${local.partition}:s3:::${var.lake_bucket_name}"
  lake_prefix     = trim(var.lake_prefix, "/")
  lake_objects    = local.lake_prefix == "" ? "${local.lake_bucket_arn}/*" : "${local.lake_bucket_arn}/${local.lake_prefix}/*"

  # Key used for schedules, locks and alarms: mirrors the S3 path.
  tables = {
    for t in var.tables :
    "${t.share}/${t.schema}/${coalesce(t.target_name, t.table)}" => t
  }

  container_name = "mirror"
}

# ---------------------------------------------------------------------------
# ECR
# ---------------------------------------------------------------------------
resource "aws_ecr_repository" "this" {
  name                 = local.name
  image_tag_mutability = "IMMUTABLE"
  force_delete         = false

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = var.lake_kms_key_arn
  }
}

resource "aws_ecr_lifecycle_policy" "this" {
  repository = aws_ecr_repository.this.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 20 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 20 }
      action       = { type = "expire" }
    }]
  })
}

# ---------------------------------------------------------------------------
# Run lock (DynamoDB). delta-rs itself commits with S3 conditional writes; this table only
# stops two runs from working on the same target table at once.
# ---------------------------------------------------------------------------
resource "aws_dynamodb_table" "run_lock" {
  name         = "${local.name}-run-lock"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "table_key"

  attribute {
    name = "table_key"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.lake_kms_key_arn
  }

  point_in_time_recovery {
    enabled = false
  }
}

# ---------------------------------------------------------------------------
# Delta Sharing profile secret. Created with a placeholder; the real profile JSON is written
# out of band (see README "Rotating the token"). Terraform never sees the token.
# ---------------------------------------------------------------------------
resource "aws_secretsmanager_secret" "profile" {
  name                    = "${local.name}/delta-sharing-profile"
  description             = "Delta Sharing recipient profile (shareCredentialsVersion, endpoint, bearerToken, expirationTime)"
  kms_key_id              = var.secret_kms_key_arn
  recovery_window_in_days = var.secret_recovery_window_days
}

resource "aws_secretsmanager_secret_version" "placeholder" {
  secret_id = aws_secretsmanager_secret.profile.id
  secret_string = jsonencode({
    shareCredentialsVersion = 1
    endpoint                = "https://${var.provider_endpoint_host}/delta-sharing"
    bearerToken             = "REPLACE_ME_OUT_OF_BAND"
    expirationTime          = "1970-01-01T00:00:00Z"
  })

  lifecycle {
    ignore_changes = [secret_string] # the real value is managed outside Terraform
  }
}

# ---------------------------------------------------------------------------
# Optional lake bucket (platform team usually owns this)
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "lake" {
  count  = var.create_lake_bucket ? 1 : 0
  bucket = var.lake_bucket_name
}

resource "aws_s3_bucket_versioning" "lake" {
  count  = var.create_lake_bucket ? 1 : 0
  bucket = aws_s3_bucket.lake[0].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  count  = var.create_lake_bucket ? 1 : 0
  bucket = aws_s3_bucket.lake[0].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.lake_kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  count                   = var.create_lake_bucket ? 1 : 0
  bucket                  = aws_s3_bucket.lake[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "aws_iam_policy_document" "lake_bucket" {
  count = var.create_lake_bucket ? 1 : 0

  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    resources = [local.lake_bucket_arn, "${local.lake_bucket_arn}/*"]
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  statement {
    sid     = "DenyUnencryptedObjectUploads"
    effect  = "Deny"
    actions = ["s3:PutObject"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    resources = ["${local.lake_bucket_arn}/*"]
    condition {
      test     = "StringNotEquals"
      variable = "s3:x-amz-server-side-encryption"
      values   = ["aws:kms"]
    }
  }
}

resource "aws_s3_bucket_policy" "lake" {
  count  = var.create_lake_bucket ? 1 : 0
  bucket = aws_s3_bucket.lake[0].id
  policy = data.aws_iam_policy_document.lake_bucket[0].json
}

# ---------------------------------------------------------------------------
# Glue database (optional)
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_database" "this" {
  count        = var.register_glue && var.create_glue_database ? 1 : 0
  name         = var.glue_database_name
  description  = "Delta tables mirrored from Delta Sharing providers (plain Delta on S3)"
  location_uri = "s3://${var.lake_bucket_name}/${local.lake_prefix}/"
}
