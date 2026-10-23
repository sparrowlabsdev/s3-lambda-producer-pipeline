# Task security group: HTTPS egress only. Where that egress may go is enforced by the platform
# team's egress path (proxy/firewall): the provider's sharing endpoint, the provider's pre-signed
# storage host(s), and AWS service endpoints (served by the VPC endpoints below).
resource "aws_security_group" "task" {
  name        = "${local.name}-task"
  description = "tsa-delta-mirror Fargate task"
  vpc_id      = var.vpc_id

  egress {
    description = "HTTPS to AWS endpoints, the sharing server and provider storage"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "endpoints" {
  count       = var.create_vpc_endpoints ? 1 : 0
  name        = "${local.name}-vpce"
  description = "Interface endpoints used by tsa-delta-mirror"
  vpc_id      = var.vpc_id

  ingress {
    description     = "HTTPS from the mirror task"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.task.id]
  }
}

locals {
  interface_endpoints = var.create_vpc_endpoints ? toset(concat(
    ["ecr.api", "ecr.dkr", "secretsmanager", "logs", "sts", "monitoring"],
    var.register_glue ? ["glue"] : [],
  )) : toset([])
  gateway_endpoints = var.create_vpc_endpoints ? toset(["s3", "dynamodb"]) : toset([])
}

resource "aws_vpc_endpoint" "interface" {
  for_each = local.interface_endpoints

  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${var.region}.${each.key}"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.private_subnet_ids
  security_group_ids  = [aws_security_group.endpoints[0].id]
  private_dns_enabled = true
}

resource "aws_vpc_endpoint" "gateway" {
  for_each = local.gateway_endpoints

  vpc_id            = var.vpc_id
  service_name      = "com.amazonaws.${var.region}.${each.key}"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = var.route_table_ids
}
