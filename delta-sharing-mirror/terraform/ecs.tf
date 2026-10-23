resource "aws_ecs_cluster" "this" {
  name = local.name

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_ecs_cluster_capacity_providers" "this" {
  cluster_name       = aws_ecs_cluster.this.name
  capacity_providers = ["FARGATE"]
}

locals {
  # Non-secret configuration only. The sharing profile is fetched from Secrets Manager at run time.
  container_environment = [
    { name = "AWS_REGION", value = var.region },
    { name = "MIRROR_PROFILE_SECRET_ARN", value = aws_secretsmanager_secret.profile.arn },
    { name = "MIRROR_LAKE_BUCKET", value = var.lake_bucket_name },
    { name = "MIRROR_LAKE_PREFIX", value = local.lake_prefix },
    { name = "MIRROR_KMS_KEY_ID", value = var.lake_kms_key_arn },
    { name = "MIRROR_LOCK_TABLE", value = aws_dynamodb_table.run_lock.name },
    { name = "MIRROR_RUN_LOCK_TTL_SECONDS", value = tostring(var.run_lock_ttl_seconds) },
    { name = "MIRROR_REGISTER_GLUE", value = tostring(var.register_glue) },
    { name = "MIRROR_GLUE_DATABASE", value = var.glue_database_name },
    { name = "MIRROR_METRICS_NAMESPACE", value = var.metrics_namespace },
    { name = "MIRROR_LOG_LEVEL", value = var.log_level },
    # Default table list (all tables). Each schedule overrides this with its single table.
    { name = "MIRROR_TABLES", value = jsonencode([for t in var.tables : {
      share                = t.share
      schema               = t.schema
      table                = t.table
      mode                 = t.mode
      primary_keys         = t.primary_keys
      partition_by         = t.partition_by
      target_name          = t.target_name
      snapshot_schema_mode = t.snapshot_schema_mode
    }]) },
  ]
}

resource "aws_ecs_task_definition" "this" {
  family                   = local.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  ephemeral_storage {
    size_in_gib = var.ephemeral_storage_gib
  }

  # Writable scratch space; the root filesystem is read-only.
  volume {
    name = "tmp"
  }

  container_definitions = jsonencode([
    {
      name                   = local.container_name
      image                  = "${aws_ecr_repository.this.repository_url}:${var.image_tag}"
      essential              = true
      user                   = "10001:10001"
      readonlyRootFilesystem = true
      environment            = local.container_environment
      mountPoints = [
        { sourceVolume = "tmp", containerPath = "/tmp", readOnly = false }
      ]
      linuxParameters = {
        initProcessEnabled = true
      }
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.this.name
          awslogs-region        = var.region
          awslogs-stream-prefix = "mirror"
        }
      }
    }
  ])
}
