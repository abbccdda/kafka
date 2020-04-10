#! /bin/bash

# exit when any command fails
set -e

WORKDIR=/mnt/tier_validator/workdir
S3_PREFIX=tier_validator_cron_logs

mkdir -p $WORKDIR

python /mnt/tier_validator/scripts/tier_validator_wrapper.py \
    --kubectl-namespace "$POD_NAMESPACE" \
    --confluent.tier.s3.bucket "$TIER_S3_BUCKET" \
    > $WORKDIR/python_stdout_stderr.log 2>&1

tail -n 20 $WORKDIR/python_stdout_stderr.log

NS=${POD_NAMESPACE:=unknown_namespace}
DATE_PREFIX=$(date +"%Y-%m-%d-%H-%M-%S")
POD_ID=${CAAS_POD_ID:=unknown_pod_id}
LOCAL_TAR_FILE_NAME=${DATE_PREFIX}_$POD_ID.tar
S3_OBJ_KEY=$S3_PREFIX/$NS/$LOCAL_TAR_FILE_NAME

find $WORKDIR -type f -name '*.log' -exec tar -cvf "$LOCAL_TAR_FILE_NAME" {} \;

echo "Completed run. Will upload to bucket: $TIER_S3_BUCKET at key: $S3_OBJ_KEY"

aws s3api put-object \
  --body "$LOCAL_TAR_FILE_NAME" \
  --bucket "$TIER_S3_BUCKET" \
  --key "$S3_OBJ_KEY" \
  --server-side-encryption AES256
echo "Run successully completed with logs uploaded to bucket: $TIER_S3_BUCKET at key: $S3_OBJ_KEY"

