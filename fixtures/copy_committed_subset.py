"""Copy the files the tests read out of a full capture, replacing each capture's fixture directory.

    python3 fixtures/copy_committed_subset.py CAPTURE_DIR FIXTURES_DIR CAPTURE...

regenerate.sh runs it after the captures. The rest of a capture stays in CAPTURE_DIR.
"""

import json
import re
import shutil
import sys
from pathlib import Path

CLASSIC_GLOBS = [
    'explain-*-formatted-initial.txt',
    'explain-*-simple-initial.txt',
    'explain-*-formatted-final.txt',
    'explain-*-simple-final.txt',
    'specimen-*-formatted-initial.txt',
    'timings-interleaved.json',
]
CONNECT_GLOBS = ['executionInfo-metrics.json', 'facts.json']

# Each form Spark's metric formatters print: bytesToString's units, msDurationToString's units,
# and a count past 999, which prints with a thousands separator.
METRIC_FORMS = {
    'B': re.compile(r'\d B\b'),
    'KiB': re.compile(r'\d KiB\b'),
    'MiB': re.compile(r'\d MiB\b'),
    'GiB': re.compile(r'\d GiB\b'),
    'ms': re.compile(r'\d ms\b'),
    's': re.compile(r'\d s\b'),
    'm': re.compile(r'\d m\b'),
    'h': re.compile(r'\d h\b'),
    'thousands': re.compile(r'\d,\d{3}\b'),
}


def coverage(execution: dict) -> set[str]:
    """The metric forms an execution's values show, and the operators its graph names."""
    nodes = execution.get('nodes', [])
    values = [metric['value'] for node in nodes for metric in node.get('metrics', [])]
    forms = {form for form, pattern in METRIC_FORMS.items() if any(pattern.search(value) for value in values)}
    return forms | {f'node:{node["nodeName"]}' for node in nodes}


def rest_sql_excerpt(executions: list[dict]) -> list[dict]:
    """The fewest executions, chosen greedily, that between them show every metric form and operator the whole list shows."""
    forms = {execution['id']: coverage(execution) for execution in executions}
    size = {execution['id']: len(json.dumps(execution)) for execution in executions}
    uncovered = set().union(*forms.values())
    chosen = []
    while uncovered:
        best = min(executions, key=lambda e: (-len(forms[e['id']] & uncovered), size[e['id']], e['id']))
        chosen.append(best)
        uncovered -= forms[best['id']]
    return sorted(chosen, key=lambda e: e['id'])


def copy_capture(source: Path, target: Path) -> None:
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    globs = CONNECT_GLOBS if source.name.startswith('spark-connect-') else CLASSIC_GLOBS
    for pattern in globs:
        matches = sorted(source.glob(pattern))
        if not matches:
            raise SystemExit(f'copy_committed_subset: {source} holds no {pattern}')
        for path in matches:
            shutil.copyfile(path, target / path.name)
    if source.name.startswith('spark-connect-'):
        return
    executions = json.loads((source / 'rest-sql-executions.json').read_text())
    excerpt = rest_sql_excerpt(executions)
    (target / 'rest-sql-executions-excerpt.json').write_text(json.dumps(excerpt, indent=2) + '\n')


def write_static_conf_keys(source: Path, target: Path) -> None:
    """The keys any captured Spark refuses at runtime, merged into the list the library ships."""
    lists = sorted(source.glob('*.txt'))
    if not lists or any(path.stat().st_size == 0 for path in lists):
        sys.exit(f'copy_committed_subset.py: {source} holds no static conf list, or an empty one')
    keys = {line for path in lists for line in path.read_text().splitlines() if line}
    target.write_text('\n'.join(sorted(keys)) + '\n')


def main() -> None:
    capture_dir, fixtures_dir = Path(sys.argv[1]), Path(sys.argv[2])
    for capture in sys.argv[3:]:
        if capture == 'static-conf-keys':
            write_static_conf_keys(capture_dir / capture, fixtures_dir.parent / 'library/src/pysparring/static_conf_keys.txt')
        else:
            copy_capture(capture_dir / capture, fixtures_dir / capture)


if __name__ == '__main__':
    main()
