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
  #   bucket         = "example-terraform-state-<env>"
  #   key            = "delta-sharing-mirror/terraform.tfstate"
  #   region         = "us-gov-west-1"
  #   dynamodb_table = "example-terraform-locks"
  #   encrypt        = true
  # }
}
