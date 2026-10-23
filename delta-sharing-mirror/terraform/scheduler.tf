# One schedule per table. Flexible time window off; no scheduler retries (an overlapping retry
# would only contend for the run lock). The next scheduled run picks up where the watermark is.
resource "aws_scheduler_schedule_group" "this" {
  name = local.name
}

resource "aws_scheduler_schedule" "table" {
  for_each = local.tables

  name        = "${local.name}-${replace(each.key, "/", "-")}"
  group_name  = aws_scheduler_schedule_group.this.name
  description = "Mirror ${each.value.share}.${each.value.schema}.${each.value.table}"
  state       = "ENABLED"

  schedule_expression          = each.value.schedule
  schedule_expression_timezone = var.schedule_timezone

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_ecs_cluster.this.arn
    role_arn = aws_iam_role.scheduler.arn

    ecs_parameters {
      task_definition_arn     = aws_ecs_task_definition.this.arn
      task_count              = 1
      launch_type             = "FARGATE"
      platform_version        = "LATEST"
      enable_ecs_managed_tags = true
      propagate_tags          = "TASK_DEFINITION"

      network_configuration {
        subnets          = var.private_subnet_ids
        security_groups  = [aws_security_group.task.id]
        assign_public_ip = false
      }
    }

    retry_policy {
      maximum_retry_attempts       = 0
      maximum_event_age_in_seconds = 60
    }

    input = jsonencode({
      containerOverrides = [{
        name = local.container_name
        environment = [{
          name = "MIRROR_TABLES"
          value = jsonencode([{
            share                = each.value.share
            schema               = each.value.schema
            table                = each.value.table
            mode                 = each.value.mode
            primary_keys         = each.value.primary_keys
            partition_by         = each.value.partition_by
            target_name          = each.value.target_name
            snapshot_schema_mode = each.value.snapshot_schema_mode
          }])
        }]
      }]
    })
  }
}
