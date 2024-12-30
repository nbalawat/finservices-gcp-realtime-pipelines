#!/bin/bash

# Set your project ID
PROJECT_ID="agentic-experiments-446019"

# Function to check job status
check_job_status() {
    local job_id=$1
    gcloud dataflow jobs describe "$job_id" \
        --project="$PROJECT_ID" \
        --format="table(
            id,
            name,
            type,
            createTime,
            state,
            currentWorkers,
            cpuTime,
            memoryInfo.currentUsage
        )"
}

# Function to get job metrics
get_job_metrics() {
    local job_id=$1
    gcloud dataflow jobs metrics list "$job_id" \
        --project="$PROJECT_ID" \
        --format="table(
            name,
            scalar,
            updateTime
        )"
}

# Get the latest job ID
JOB_ID=$(gcloud dataflow jobs list \
    --project="$PROJECT_ID" \
    --filter="name:payment-data-generator" \
    --format="get(id)" \
    --limit=1)

if [ -z "$JOB_ID" ]; then
    echo "No Dataflow job found"
    exit 1
fi

echo "Monitoring Dataflow job: $JOB_ID"
echo "View in Console: https://console.cloud.google.com/dataflow/jobs/$PROJECT_ID/$JOB_ID?project=$PROJECT_ID"
echo

while true; do
    clear
    echo "=== Job Status ==="
    check_job_status "$JOB_ID"
    echo
    echo "=== Job Metrics ==="
    get_job_metrics "$JOB_ID"
    echo
    echo "Press Ctrl+C to stop monitoring"
    sleep 30
done
