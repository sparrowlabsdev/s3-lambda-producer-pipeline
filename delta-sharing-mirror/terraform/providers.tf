provider "aws" {
  region = var.region

  default_tags {
    tags = merge({ Project = "delta-sharing-mirror", ManagedBy = "terraform" }, var.tags)
  }
}
