#!/bin/bash
docker exec spark-local bash -c "export PYTHONPATH=/workspace && python -u /workspace/$1"