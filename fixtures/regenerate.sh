#!/usr/bin/env bash
# Regenerate the Spark fixtures. Runs job/capture_plans.py under spark-submit in each Spark image,
# which writes fixtures/spark-<version>/ in place.
#
#   fixtures/regenerate.sh              every release
#   fixtures/regenerate.sh VERSION...   only these, e.g. 4.2.0
#
# Needs docker.
set -euo pipefail

readonly SPARK_RELEASES=(3.5.9 4.2.0)

fixtures=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
readonly fixtures

# The Spark images run as a user whose uid owns nothing on the host, so the job runs as root and
# run-and-hand-back-output.sh gives the output back to the host user. The job imports the library
# from src/, for the schedule and plan capture compare_plans uses. The images run Python 3.10, below
# the library's floor, so the job runs on Ubuntu's python3.11 with rich installed beside it.
# Output lands in a staging directory and replaces the release's fixtures only when the job succeeds.
capture() {
  local staging
  staging=$(mktemp -d "$fixtures/.capture-XXXXXX")
  if ! docker run --rm --user 0 -e HOME=/tmp -e "OWNER=$(id -u):$(id -g)" \
    -v "$fixtures/job:/job:ro" -v "$fixtures/../src:/src:ro" -v "$staging:/out" --entrypoint sh "apache/spark:$1-python3" -c \
    'apt-get update -qq >/dev/null && apt-get install -y -qq python3.11 >/dev/null 2>&1 &&
     python3 -m pip install --quiet --disable-pip-version-check --target /tmp/python-deps rich &&
     PYSPARK_PYTHON=python3.11 PYSPARK_DRIVER_PYTHON=python3.11 PYTHONPATH=/src:/tmp/python-deps \
     /job/run-and-hand-back-output.sh /opt/spark/bin/spark-submit /job/capture_plans.py /out'; then
    rm -rf "$staging"
    return 1
  fi
  rm -rf "$fixtures/spark-$1"
  mv "$staging/spark-$1" "$fixtures/spark-$1"
  rmdir "$staging"
}

main() {
  local selected=("$@") version
  [ ${#selected[@]} -gt 0 ] || selected=("${SPARK_RELEASES[@]}")
  for version in "${selected[@]}"; do
    case " ${SPARK_RELEASES[*]} " in
      *" $version "*) ;;
      *)
        echo "regenerate.sh: no capture for Spark $version; the releases are: ${SPARK_RELEASES[*]}" >&2
        exit 2
        ;;
    esac
  done
  for version in "${selected[@]}"; do
    echo "capturing Spark $version" >&2
    capture "$version"
  done
}

main "$@"
