#!/usr/bin/env bash
# Run inside a Spark 4 image: install the Connect client's packages, given as arguments, start a
# Spark Connect server, and run connect_probe.py against it once its port answers.
set -euo pipefail

pip install --quiet --disable-pip-version-check "$@"
/opt/spark/sbin/start-connect-server.sh \
  --conf spark.connect.grpc.binding.port=15002 --conf spark.ui.port=4040 >/dev/null
for _ in $(seq 1 60); do
  if (echo >/dev/tcp/localhost/15002) 2>/dev/null; then
    break
  fi
  sleep 1
done

py4j=(/opt/spark/python/lib/py4j-*-src.zip)
if ! PYTHONPATH="/opt/spark/python:${py4j[0]}" python3 /job/connect_probe.py /out; then
  tail -40 /opt/spark/logs/*.out >&2
  exit 1
fi
