#!/bin/bash

# Configuration
PROJECT_ID="agentic-experiments-446019"
BIGTABLE_INSTANCE="payment-processing-dev"
REGION="us-east4"
ZONE="us-east4-a"
EXISTING_SERVICE_ACCOUNT="beam-local-pipeline@${PROJECT_ID}.iam.gserviceaccount.com"
BUCKET_NAME="${PROJECT_ID}-dataflow-temp"

enable_apis() {
    echo "=== Enabling Required Google Cloud APIs ==="
    echo "Enabling: Dataflow, BigTable, Storage, Compute, and Resource Manager APIs"
    gcloud services enable \
        dataflow.googleapis.com \
        bigtable.googleapis.com \
        storage-api.googleapis.com \
        storage-component.googleapis.com \
        compute.googleapis.com \
        cloudresourcemanager.googleapis.com
    echo "APIs enabled successfully"
}

setup_permissions() {
    echo "=== Setting up IAM Permissions ==="
    echo "Adding BigTable Admin role to service account: ${EXISTING_SERVICE_ACCOUNT}"
    gcloud projects add-iam-policy-binding ${PROJECT_ID} \
        --member="serviceAccount:${EXISTING_SERVICE_ACCOUNT}" \
        --role="roles/bigtable.admin"
    
    echo "Adding Dataflow Worker role to service account"    
    gcloud projects add-iam-policy-binding ${PROJECT_ID} \
        --member="serviceAccount:${EXISTING_SERVICE_ACCOUNT}" \
        --role="roles/dataflow.worker"
    
    echo "Adding Storage Object Viewer role to service account"    
    gcloud projects add-iam-policy-binding ${PROJECT_ID} \
        --member="serviceAccount:${EXISTING_SERVICE_ACCOUNT}" \
        --role="roles/storage.objectViewer"
    
    echo "IAM permissions setup completed"
}

setup_bigtable() {
    echo "=== Creating BigTable Instance ==="
    echo "Instance: ${BIGTABLE_INSTANCE}"
    echo "Zone: ${ZONE}"
    gcloud bigtable instances create ${BIGTABLE_INSTANCE} \
        --cluster=${BIGTABLE_INSTANCE}-cluster \
        --cluster-zone=${ZONE} \
        --display-name="Payment Processing Dev" \
        --instance-type=development
    echo "BigTable instance created successfully"
}

setup_storage() {
    echo "=== Setting up Cloud Storage ==="
    echo "Creating bucket: ${BUCKET_NAME} in region: ${REGION}"
    gsutil mb -p ${PROJECT_ID} -l ${REGION} gs://${BUCKET_NAME}
    
    echo "Setting up lifecycle policy (7 days retention)"
    cat > lifecycle.json <<EOF
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {
          "age": 7
        }
      }
    ]
  }
}
EOF
    
    gsutil lifecycle set lifecycle.json gs://${BUCKET_NAME}
    rm lifecycle.json
    echo "Storage bucket setup completed"
}

setup_python_env() {
    echo "=== Setting up Python Environment ==="
    echo "Creating virtual environment"
    python -m venv venv
    source venv/bin/activate
    echo "Upgrading pip"
    pip install --upgrade pip
    echo "Installing required packages"
    pip install apache-beam[gcp] google-cloud-bigtable
    echo "Python environment setup completed"
}

main() {
    echo "=== Starting Infrastructure Setup ==="
    echo "Project ID: ${PROJECT_ID}"
    echo "Region: ${REGION}"
    echo "Zone: ${ZONE}"
    
    if ! gcloud projects describe ${PROJECT_ID} >/dev/null 2>&1; then
        echo "Error: Project ${PROJECT_ID} not found"
        exit 1
    fi

    echo "Project verified successfully"
    echo "Enabling required APIs..."
    if ! enable_apis; then
        echo "Error enabling APIs"
        exit 1
    fi

    echo "Setting up permissions..."
    if ! setup_permissions; then
        echo "Error setting up permissions"
        exit 1
    fi

    echo "Setting up Bigtable..."
    if ! setup_bigtable; then
        echo "Error setting up Bigtable"
        exit 1
    fi

    echo "Setting up Storage..."
    if ! setup_storage; then
        echo "Error setting up Storage"
        exit 1
    fi

    # echo "Setting up Python environment..."
    # if ! setup_python_env; then
    #     echo "Error setting up Python environment"
    #     exit 1
    # fi

    echo "=== Infrastructure Setup Completed Successfully ==="
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main
fi