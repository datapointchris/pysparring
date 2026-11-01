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
# run-and-hand-back-output.sh gives the output back to the host user.
capture() {
  rm -rf "$fixtures/spark-$1"
  docker run --rm --user 0 -e HOME=/tmp -e "OWNER=$(id -u):$(id -g)" \
    -v "$fixtures/job:/job:ro" -v "$fixtures:/out" --entrypoint sh "apache/spark:$1-python3" \
    /job/run-and-hand-back-output.sh /opt/spark/bin/spark-submit /job/capture_plans.py /out
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
