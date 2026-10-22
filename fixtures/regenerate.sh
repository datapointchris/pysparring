#!/usr/bin/env bash
# Regenerate the Spark fixtures. Runs job/capture_plans.py under each Spark image and
# job/connect_probe.py against a Spark Connect server, then copies the files the tests read into
# spark-3.5.9/, spark-4.2.0/ and spark-connect-4.2.0/ beside this script.
#
#   fixtures/regenerate.sh              every capture
#   fixtures/regenerate.sh CAPTURE...   only these, e.g. spark-4.2.0
#
# The full capture and each run's log go to a temporary directory, removed on exit. Set
# CAPTURE_DIR to keep them there instead. Needs docker. Every capture reaches PyPI, and the classic
# captures reach Maven Central for sparkMeasure's jar.
set -euo pipefail

readonly CAPTURES=(spark-3.5.9 spark-4.2.0 spark-connect-4.2.0 static-conf-keys)
readonly SPARK_3=apache/spark:3.5.9-python3
readonly SPARK_4=apache/spark:4.2.0-python3

# sparkMeasure's Python half, and the release of the jar it drives.
readonly SPARKMEASURE_PYTHON=sparkmeasure==0.28.0
readonly SPARKMEASURE_JAR=0.28

# The Connect client's packages, at releases that meet pyspark 4.2.0's floors and install on the
# image's Python 3.10. Its generated protobuf code refuses a runtime older than 6.33.5.
readonly CONNECT_CLIENT_PACKAGES=(
  pandas==2.2.3 numpy==1.26.4 pyarrow==18.1.0
  grpcio==1.76.0 grpcio-status==1.76.0 protobuf==6.33.5 googleapis-common-protos==1.72.0
  zstandard==0.25.0
)

fixtures=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
readonly fixtures
if [ -n "${CAPTURE_DIR:-}" ]; then
  mkdir -p "$CAPTURE_DIR"
  work=$(cd "$CAPTURE_DIR" && pwd)
else
  work=$(mktemp -d)
  trap 'rm -rf "$work"' EXIT
fi
readonly work

# in_container IMAGE COMMAND...: run COMMAND in IMAGE as root, with job/ at /job and the capture
# directory at /out.
in_container() {
  local image=$1
  shift
  docker run --rm --user 0 -e HOME=/tmp -e "OWNER=$(id -u):$(id -g)" \
    -v "$fixtures/job:/job:ro" -v "$work:/out" --entrypoint sh "$image" \
    /job/run-and-hand-back-output.sh "$@"
}

run_capture() {
  case $1 in
    spark-3.5.9)
      capture_classic "$SPARK_3" 2.12
      ;;
    spark-4.2.0)
      capture_classic "$SPARK_4" 2.13
      ;;
    spark-connect-4.2.0)
      in_container "$SPARK_4" bash /job/run-connect-probe.sh "${CONNECT_CLIENT_PACKAGES[@]}"
      ;;
    static-conf-keys)
      mkdir -p "$work/static-conf-keys"
      in_container "$SPARK_3" /opt/spark/bin/spark-submit /job/list_static_confs.py /out/static-conf-keys/3.5.9.txt
      in_container "$SPARK_4" /opt/spark/bin/spark-submit /job/list_static_confs.py /out/static-conf-keys/4.2.0.txt
      ;;
  esac
}

# capture_classic IMAGE SCALA: run capture_plans.py under spark-submit, with the sparkMeasure jar
# built for SCALA, the image's Scala binary version.
capture_classic() {
  in_container "$1" sh -c \
    "pip install --quiet --disable-pip-version-check $SPARKMEASURE_PYTHON && exec /opt/spark/bin/spark-submit \"\$@\"" \
    spark-submit --packages "ch.cern.sparkmeasure:spark-measure_$2:$SPARKMEASURE_JAR" \
    --conf spark.jars.ivy=/tmp/ivy /job/capture_plans.py /out
}

main() {
  local selected=("$@") capture
  [ ${#selected[@]} -gt 0 ] || selected=("${CAPTURES[@]}")
  for capture in "${selected[@]}"; do
    case " ${CAPTURES[*]} " in
      *" $capture "*) ;;
      *)
        echo "regenerate.sh: no capture named $capture; the captures are: ${CAPTURES[*]}" >&2
        exit 2
        ;;
    esac
  done
  for capture in "${selected[@]}"; do
    echo "capturing $capture" >&2
    if ! run_capture "$capture" 2>"$work/$capture.log"; then
      tail -40 "$work/$capture.log" >&2
      echo "regenerate.sh: $capture failed, and the end of its log is above" >&2
      exit 1
    fi
  done
  python3 "$fixtures/copy_committed_subset.py" "$work" "$fixtures" "${selected[@]}"
}

main "$@"
