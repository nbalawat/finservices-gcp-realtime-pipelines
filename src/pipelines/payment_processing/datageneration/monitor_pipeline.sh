#!/bin/bash

# Set variables
PROJECT_ID="agentic-experiments-446019"
REGION="us-east4"

# Get the most recent job ID
JOB_ID=$(gcloud dataflow jobs list \
    --project=$PROJECT_ID \
    --region=$REGION \
    --status=active \
    --format="value(JOB_ID)" \
    --limit=1)

if [ -z "$JOB_ID" ]; then
    echo "No active Dataflow job found"
    exit 1
fi

echo "Monitoring Dataflow job: $JOB_ID"
echo "View in Console: https://console.cloud.google.com/dataflow/jobs/$PROJECT_ID/$JOB_ID?project=$PROJECT_ID"

# Monitor job progress
while true; do
    clear
    echo "=== Job Status ==="
    gcloud dataflow jobs show $JOB_ID \
        --project=$PROJECT_ID \
        --region=$REGION \
        --format="table(
            id,
            name,
            currentState,
            currentStateTime,
            createTime,
            requestedState,
            location,
            sdkVersion,
            environment.workerPools[].numWorkers
        )"

    echo -e "\n=== Job Progress ==="
    gcloud dataflow jobs show $JOB_ID \
        --project=$PROJECT_ID \
        --region=$REGION \
        --format="table(
            id,
            name,
            environment.workerPools[].defaultPackageSet,
            environment.workerPools[].machineType,
            environment.workerPools[].numWorkers,
            environment.workerPools[].taskrunnerSettings.taskSchedulingMode
        )"

    # Check if job is complete
    JOB_STATE=$(gcloud dataflow jobs show $JOB_ID \
        --project=$PROJECT_ID \
        --region=$REGION \
        --format="value(currentState)")

    if [[ "$JOB_STATE" == "JOB_STATE_DONE" || "$JOB_STATE" == "JOB_STATE_FAILED" || "$JOB_STATE" == "JOB_STATE_CANCELLED" ]]; then
        echo "Job $JOB_STATE"
        break
    fi

    echo -e "\nRefreshing in 30 seconds... (Ctrl+C to exit)"
    echo "For more detailed metrics, visit the Cloud Console URL above"
    sleep 30
done
