resource "aws_cloudwatch_log_group" "this" {
  name              = "/ecs/${local.name}"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.logs_kms_key_arn
}

locals {
  alarm_actions = var.alarm_sns_topic_arn == null ? [] : [var.alarm_sns_topic_arn]
  table_names   = { for k, t in local.tables : k => "${t.share}.${t.schema}.${t.table}" }
}

# A run reported Success=0 (any table failure exits non-zero and emits Success=0).
resource "aws_cloudwatch_metric_alarm" "run_failed" {
  for_each = local.table_names

  alarm_name          = "${local.name}-run-failed-${replace(each.key, "/", "-")}"
  alarm_description   = "tsa-delta-mirror run failed for ${each.value}"
  namespace           = var.metrics_namespace
  metric_name         = "Success"
  dimensions          = { Table = each.value }
  statistic           = "Minimum"
  period              = var.alarm_period_seconds
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions
}

# No run at all in two schedule intervals (task never started, scheduler disabled, etc.).
resource "aws_cloudwatch_metric_alarm" "missed_run" {
  for_each = local.table_names

  alarm_name          = "${local.name}-missed-run-${replace(each.key, "/", "-")}"
  alarm_description   = "tsa-delta-mirror has not run for ${each.value} in 2x the schedule interval"
  namespace           = var.metrics_namespace
  metric_name         = "Success"
  dimensions          = { Table = each.value }
  statistic           = "SampleCount"
  period              = var.alarm_period_seconds * 2
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions
}

# Recipient token expires within MIRROR_TOKEN_EXPIRY_WARN_DAYS (default 7).
resource "aws_cloudwatch_metric_alarm" "token_expiring" {
  alarm_name          = "${local.name}-token-expiring-soon"
  alarm_description   = "Delta Sharing recipient token expires soon; rotate the profile secret"
  namespace           = var.metrics_namespace
  metric_name         = "TokenExpiringSoon"
  statistic           = "Maximum"
  period              = var.alarm_period_seconds
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions
}
