#!/usr/bin/env bash
# Smoke test for a real deployment: run ONE table once as a Fargate task, wait, print the
# resulting Delta history (read straight from S3 with deltalake, no Spark).
#
# Usage:
#   scripts/smoke.sh <terraform-dir-or-workspace> '<MIRROR_TABLES json for one table>'
# Example:
#   scripts/smoke.sh terraform '[{"share":"gov","schema":"ops","table":"people_cdf","mode":"auto","primary_keys":["id"]}]'
# Requires: aws cli (GovCloud profile/region exported), terraform outputs, python with deltalake.
set -euo pipefail
TF_DIR=${1:?terraform dir}
TABLES=${2:?MIRROR_TABLES json}

out() { terraform -chdir="$TF_DIR" output -raw "$1"; }
CLUSTER=$(out ecs_cluster_arn); TASKDEF=$(out task_definition_arn); SUBNETS=$(out subnet_ids_csv)
SG=$(out task_security_group_id); BUCKET=$(out lake_bucket); PREFIX=$(out lake_prefix); REGION=$(out region)
SECRET=$(out profile_secret_arn)

echo ">> running task once"
TASK_ARN=$(aws ecs run-task --region "$REGION" --cluster "$CLUSTER" --task-definition "$TASKDEF" \
  --launch-type FARGATE --count 1 \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SG],assignPublicIp=DISABLED}" \
  --overrides "{\"containerOverrides\":[{\"name\":\"mirror\",\"environment\":[{\"name\":\"MIRROR_TABLES\",\"value\":$(printf '%s' "$TABLES" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))')}]}]}" \
  --query 'tasks[0].taskArn' --output text)
echo ">> task $TASK_ARN"
aws ecs wait tasks-stopped --region "$REGION" --cluster "$CLUSTER" --tasks "$TASK_ARN"
EXIT=$(aws ecs describe-tasks --region "$REGION" --cluster "$CLUSTER" --tasks "$TASK_ARN" --query 'tasks[0].containers[0].exitCode' --output text)
echo ">> container exit code: $EXIT"

echo ">> Delta history from S3"
MIRROR_PROFILE_SECRET_ARN="$SECRET" MIRROR_TABLES="$TABLES" MIRROR_LAKE_BUCKET="$BUCKET" MIRROR_LAKE_PREFIX="$PREFIX" \
AWS_REGION="$REGION" python -m tsa_delta_mirror --history 5
test "$EXIT" = "0"
