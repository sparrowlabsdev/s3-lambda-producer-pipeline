terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.100"
    }
  }

  # State backend is the platform team's choice. Example for GovCloud:
  # backend "s3" {
  #   bucket         = "tsa-terraform-state-<env>"
  #   key            = "tsa-delta-mirror/terraform.tfstate"
  #   region         = "us-gov-west-1"
  #   dynamodb_table = "tsa-terraform-locks"
  #   encrypt        = true
  # }
}
