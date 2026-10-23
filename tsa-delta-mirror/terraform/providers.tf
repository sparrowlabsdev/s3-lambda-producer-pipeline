provider "aws" {
  region = var.region

  default_tags {
    tags = merge({ Project = "tsa-delta-mirror", ManagedBy = "terraform" }, var.tags)
  }
}
