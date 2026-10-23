output "region" {
  value = var.region
}

output "ecr_repository_url" {
  description = "Push the image here: docker push <url>:<image_tag>"
  value       = aws_ecr_repository.this.repository_url
}

output "ecs_cluster_arn" {
  value = aws_ecs_cluster.this.arn
}

output "task_definition_arn" {
  value = aws_ecs_task_definition.this.arn
}

output "task_security_group_id" {
  value = aws_security_group.task.id
}

output "subnet_ids_csv" {
  value = join(",", var.private_subnet_ids)
}

output "task_role_arn" {
  description = "Role that reads/writes the lake prefix. Grant the same S3 path to Unity Catalog later."
  value       = aws_iam_role.task.arn
}

output "profile_secret_arn" {
  description = "Write the real Delta Sharing profile JSON into this secret out of band."
  value       = aws_secretsmanager_secret.profile.arn
}

output "run_lock_table" {
  value = aws_dynamodb_table.run_lock.name
}

output "log_group" {
  value = aws_cloudwatch_log_group.this.name
}

output "lake_bucket" {
  value = var.lake_bucket_name
}

output "lake_prefix" {
  value = local.lake_prefix
}

output "schedules" {
  value = { for k, s in aws_scheduler_schedule.table : k => s.schedule_expression }
}

output "egress_allowlist" {
  description = "Hosts the platform team must allow on the task's egress path."
  value       = concat([var.provider_endpoint_host], var.provider_storage_hosts)
}
