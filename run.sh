#!/bin/bash
docker exec -it spark-local bash -c "export PYTHONPATH=/workspace && python /workspace/$1"