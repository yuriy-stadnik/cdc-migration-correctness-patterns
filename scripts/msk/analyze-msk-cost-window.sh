#!/usr/bin/env bash
set -euo pipefail

START="${1:-}"
END="${2:-}"
REGION="${3:-${AWS_REGION:-us-east-1}}"

if [[ -z "$START" || -z "$END" ]]; then
  echo "Usage: $0 <start-date YYYY-MM-DD> <end-date YYYY-MM-DD> [region]" >&2
  exit 1
fi

hours_to_hms() {
  local hours="$1"
  awk -v h="$hours" 'BEGIN {
    total = int((h * 3600) + 0.5)
    hh = int(total / 3600)
    mm = int((total % 3600) / 60)
    ss = int(total % 60)
    printf "%02d:%02d:%02d\n", hh, mm, ss
  }'
}

echo "MSK cost usage-type breakdown (${START} -> ${END})"
aws ce get-cost-and-usage \
  --time-period "Start=${START},End=${END}" \
  --granularity DAILY \
  --metrics UnblendedCost UsageQuantity \
  --filter '{"Dimensions":{"Key":"SERVICE","Values":["Amazon Managed Streaming for Apache Kafka"]}}' \
  --group-by Type=DIMENSION,Key=USAGE_TYPE \
  --query 'ResultsByTime[].{start:TimePeriod.Start,end:TimePeriod.End,groups:Groups[].{usage:Keys[0],cost:Metrics.UnblendedCost.Amount,unit:Metrics.UnblendedCost.Unit,qty:Metrics.UsageQuantity.Amount,qty_unit:Metrics.UsageQuantity.Unit}}' \
  --output json

echo
echo "MSK operation breakdown (${START} -> ${END})"
aws ce get-cost-and-usage \
  --time-period "Start=${START},End=${END}" \
  --granularity DAILY \
  --metrics UnblendedCost UsageQuantity \
  --filter '{"Dimensions":{"Key":"SERVICE","Values":["Amazon Managed Streaming for Apache Kafka"]}}' \
  --group-by Type=DIMENSION,Key=OPERATION \
  --query 'ResultsByTime[].{start:TimePeriod.Start,end:TimePeriod.End,groups:Groups[].{op:Keys[0],cost:Metrics.UnblendedCost.Amount,qty:Metrics.UsageQuantity.Amount}}' \
  --output json

echo
echo "MSK cluster-hours billed day summary (${START} -> ${END})"
cluster_hour_rows="$(
  aws ce get-cost-and-usage \
    --time-period "Start=${START},End=${END}" \
    --granularity DAILY \
    --metrics UnblendedCost UsageQuantity \
    --filter '{"Dimensions":{"Key":"SERVICE","Values":["Amazon Managed Streaming for Apache Kafka"]}}' \
    --group-by Type=DIMENSION,Key=USAGE_TYPE \
    --query 'ResultsByTime[?length(Groups[?Keys[0]==`USE1-KafkaServerless-ClusterHours`]) > `0`].[TimePeriod.Start,Groups[?Keys[0]==`USE1-KafkaServerless-ClusterHours`]|[0].Metrics.UsageQuantity.Amount,Groups[?Keys[0]==`USE1-KafkaServerless-ClusterHours`]|[0].Metrics.UnblendedCost.Amount]' \
    --output text
)"
if [[ -z "${cluster_hour_rows}" ]]; then
  echo "No USE1-KafkaServerless-ClusterHours charges found in this date window."
else
  while IFS=$'\t' read -r day hours cost; do
    [[ -z "${day}" ]] && continue
    hms="$(hours_to_hms "${hours}")"
    echo "- ${day} UTC day: ${hours} hrs (${hms}), cost USD ${cost}"
  done <<< "${cluster_hour_rows}"
fi

echo
echo "CloudTrail Kafka cluster lifecycle events (${START}T00:00:00Z -> ${END}T23:59:59Z) in ${REGION}"
if ! aws cloudtrail lookup-events \
  --region "${REGION}" \
  --lookup-attributes AttributeKey=EventSource,AttributeValue=kafka.amazonaws.com \
  --start-time "${START}T00:00:00Z" \
  --end-time "${END}T23:59:59Z" \
  --query 'Events[?contains(EventName, `Cluster`) || contains(EventName, `Broker`) || contains(EventName, `VpcConnection`)].[EventTime,EventName,Username,Resources[0].ResourceName]' \
  --output table; then
  echo "CloudTrail lookup unavailable (missing cloudtrail:LookupEvents permission)."
  echo "Ask IAM admin to allow cloudtrail:LookupEvents for exact create/delete timestamps."
fi

echo
echo "Currently active MSK clusters in ${REGION}"
aws kafka list-clusters-v2 \
  --region "${REGION}" \
  --max-results 20 \
  --query 'ClusterInfoList[].{name:ClusterName,arn:ClusterArn,type:ClusterType,state:State,created:CreationTime}' \
  --output table
