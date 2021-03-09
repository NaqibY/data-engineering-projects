#!/bin/bash
docker run -it  \
-e GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/test-gcs.json \
-v $GOOGLE_APPLICATION_CREDENTIALS:/tmp/keys/test-gcs.json:ro \
-v $(pwd):/app \
--net=host \
project_four