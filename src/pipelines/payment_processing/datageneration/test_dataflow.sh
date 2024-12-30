#!/bin/bash

export GOOGLE_APPLICATION_CREDENTIALS=/Users/nbalawat/development/apache-beam-examples/src/beam-bigquery-test.json

# Enable debug logging
export BEAM_VERBOSE_LOG=1

python test_dataflow.py
