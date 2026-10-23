variable "region" {
  description = "GovCloud region."
  type        = string
  default     = "us-gov-west-1"
}

variable "name_prefix" {
  description = "Prefix for every resource name."
  type        = string
  default     = "delta-sharing-mirror"
}

variable "environment" {
  description = "Environment label (dev, test, prod)."
  type        = string
}

variable "tags" {
  description = "Extra tags applied to every resource."
  type        = map(string)
  default     = {}
}

# ---------------------------------------------------------------------------
# Networking (existing VPC owned by the platform team)
# ---------------------------------------------------------------------------
variable "vpc_id" {
  description = "VPC the Fargate task runs in."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnets for the task (no public IP is ever assigned)."
  type        = list(string)
}

variable "create_vpc_endpoints" {
  description = "Create interface/gateway endpoints for the AWS services the task uses. Set false if the VPC already has them."
  type        = bool
  default     = true
}

variable "route_table_ids" {
  description = "Route tables to associate with the S3 and DynamoDB gateway endpoints (required when create_vpc_endpoints = true)."
  type        = list(string)
  default     = []
}

variable "provider_endpoint_host" {
  description = "Hostname of the Delta Sharing server (for the egress allowlist and documentation only; the token lives in Secrets Manager)."
  type        = string
}

variable "provider_storage_hosts" {
  description = "Hostnames the provider's pre-signed URLs point at (e.g. a specific S3/ADLS/GCS bucket host). Must be on the egress allowlist."
  type        = list(string)
  default     = []
}

# ---------------------------------------------------------------------------
# Lake
# ---------------------------------------------------------------------------
variable "lake_bucket_name" {
  description = "S3 bucket that holds the data lake (versioned, SSE-KMS)."
  type        = string
}

variable "create_lake_bucket" {
  description = "Create the lake bucket (versioning on, SSE-KMS, deny unencrypted puts). False when the platform team owns it."
  type        = bool
  default     = false
}

variable "lake_prefix" {
  description = "Prefix under the bucket where mirrored tables land: s3://<bucket>/<prefix>/<share>/<schema>/<table>/."
  type        = string
  default     = "bronze/shared"
}

variable "lake_kms_key_arn" {
  description = "Customer-managed KMS key used for the lake bucket objects."
  type        = string
}

# ---------------------------------------------------------------------------
# Image / task
# ---------------------------------------------------------------------------
variable "image_tag" {
  description = "Immutable image tag pushed to the ECR repository created here (e.g. the git SHA)."
  type        = string
}

variable "task_cpu" {
  description = "Fargate CPU units (1024 = 1 vCPU). Raise with task_memory for large initial loads."
  type        = number
  default     = 1024
}

variable "task_memory" {
  description = "Fargate memory in MiB. The mirror holds one table snapshot in memory, so size for the largest initial load."
  type        = number
  default     = 4096
}

variable "ephemeral_storage_gib" {
  description = "Fargate ephemeral storage for /tmp (21-200 GiB)."
  type        = number
  default     = 21
}

variable "log_level" {
  type    = string
  default = "INFO"
}

# ---------------------------------------------------------------------------
# Tables and schedules
# ---------------------------------------------------------------------------
variable "tables" {
  description = <<-EOT
    Tables to mirror. One EventBridge schedule per table. `schedule` is an EventBridge Scheduler
    expression (rate(...) or cron(...)). `mode` is auto | cdf | snapshot. `primary_keys` is required
    for CDF merges.
  EOT
  type = list(object({
    share                = string
    schema               = string
    table                = string
    mode                 = optional(string, "auto")
    primary_keys         = optional(list(string), [])
    partition_by         = optional(list(string), [])
    target_name          = optional(string)
    snapshot_schema_mode = optional(string, "overwrite")
    schedule             = optional(string, "rate(1 hour)")
  }))

  validation {
    condition     = alltrue([for t in var.tables : t.mode != "cdf" || length(t.primary_keys) > 0])
    error_message = "Tables with mode = \"cdf\" must declare primary_keys."
  }
  validation {
    condition     = alltrue([for t in var.tables : contains(["auto", "cdf", "snapshot"], t.mode)])
    error_message = "mode must be auto, cdf or snapshot."
  }
}

variable "schedule_timezone" {
  description = "Timezone for cron() schedules."
  type        = string
  default     = "UTC"
}

variable "run_lock_ttl_seconds" {
  description = "How long a run lock is honoured before it is considered stale (should exceed the longest run)."
  type        = number
  default     = 10800
}

# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------
variable "register_glue" {
  description = "Register/refresh each mirrored table in the Glue Data Catalog so Athena can query it."
  type        = bool
  default     = false
}

variable "glue_database_name" {
  type    = string
  default = "bronze_shared"
}

variable "create_glue_database" {
  description = "Create the Glue database (only used when register_glue = true)."
  type        = bool
  default     = true
}

# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------
variable "log_retention_days" {
  type    = number
  default = 90
}

variable "logs_kms_key_arn" {
  description = "Optional CMK for the CloudWatch log group."
  type        = string
  default     = null
}

variable "metrics_namespace" {
  type    = string
  default = "DeltaMirror"
}

variable "alarm_sns_topic_arn" {
  description = "SNS topic that receives alarm notifications (optional)."
  type        = string
  default     = null
}

variable "alarm_period_seconds" {
  description = "Evaluation period for the per-table alarms. Set to the schedule interval (missed-run alarm uses 2x)."
  type        = number
  default     = 3600
}

# ---------------------------------------------------------------------------
# Secret
# ---------------------------------------------------------------------------
variable "secret_kms_key_arn" {
  description = "Optional CMK for the Secrets Manager secret (default: aws/secretsmanager)."
  type        = string
  default     = null
}

variable "secret_recovery_window_days" {
  type    = number
  default = 7
}
