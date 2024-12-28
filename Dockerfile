FROM apache/beam_python3.9_sdk:2.50.0

WORKDIR /app

# Copy the pipeline code
COPY src/pipelines /app/pipelines
COPY requirements.txt /app/

# Install dependencies
RUN pip install -r requirements.txt

# Set the entrypoint
ENTRYPOINT ["python", "-m"]
