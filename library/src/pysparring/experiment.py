"""An experiment: candidates × datasets × confs × clusters, expanded into cells and arms before any Spark call.

A cell is one (dataset, conf, cluster). An arm is one candidate inside a cell, and arms are compared
within a cell. Building an `Experiment` resolves everything a run could fail on, and raises `Refused`
with every problem it found.
"""

import dataclasses as dc
import datetime as dt
import hashlib
import importlib
import importlib.resources
import importlib.util
import inspect
import itertools
import json
import secrets
import sys
import tomllib
import types
import typing
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from functools import cache
from pathlib import Path
from typing import Any

from pysparring.refusals import Problem
from pysparring.refusals import Refused
from pysparring.speed_verdict import fewest_rounds_reaching
from pysparring.speed_verdict import smallest_reachable_p

SCHEMA_VERSION = 1
BACKENDS = ('local', 'glue-image', 'glue-session', 'glue-job', 'glue-livy', 'emr-serverless', 'athena', 'emr-ec2')
TIMING_ORDERS = ('interleaved', 'sequential')
FORMATS_BY_SUFFIX = {
    '.parquet': 'parquet',
    '.csv': 'csv',
    '.json': 'json',
    '.jsonl': 'json',
    '.ndjson': 'json',
    '.orc': 'orc',
    '.avro': 'avro',
    '.txt': 'text',
}
# Spark's own default source, `spark.sql.sources.default`, read for a path with no extension.
DEFAULT_FORMAT = 'parquet'
REMOTE_PREFIXES = ('s3://', 's3a://', 's3n://', 'gs://', 'abfss://', 'hdfs://', 'dbfs:/')
TABLE_PARAMETER_KINDS = (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
# A cell id joins names with `/` and `@`, and a problem's field joins them with `.`.
NAME_SEPARATORS = ('/', '@', '.')


@cache
def static_conf_keys() -> frozenset[str]:
    """The conf keys `spark.conf.set` refuses on any Spark the fixtures capture."""
    text = importlib.resources.files('pysparring').joinpath('static_conf_keys.txt').read_text()
    return frozenset(text.split())


def doppeldata_installed() -> bool:
    return importlib.util.find_spec('doppeldata') is not None


@dc.dataclass(frozen=True)
class Candidate:
    """A transform function. Its parameters without a default name the tables it receives."""

    name: str
    ref: str
    function: Callable[..., Any] = dc.field(repr=False, compare=False)
    kwargs: Mapping[str, Any]
    tables: tuple[str, ...]
    source_sha256: str | None

    def to_json(self) -> dict[str, Any]:
        return {'ref': self.ref, 'kwargs': dict(self.kwargs), 'tables': list(self.tables)}


@dc.dataclass(frozen=True)
class PathTable:
    """A table read from `path`, which is absolute for a local path and as written for a remote one."""

    path: str
    format: str

    def to_json(self, root: Path) -> dict[str, Any]:
        return {'kind': 'path', 'path': portable_path(self.path, root), 'format': self.format}


@dc.dataclass(frozen=True)
class CatalogTable:
    table: str

    def to_json(self, root: Path) -> dict[str, Any]:
        return {'kind': 'catalog', 'table': self.table}


@dc.dataclass(frozen=True)
class DoppeldataTable:
    """A table doppeldata generates from a profile, an optional scenario, a row count and a seed."""

    profile: str
    profile_sha256: str
    scenario: str | None
    scenario_sha256: str | None
    rows: int
    seed: int

    def to_json(self, root: Path) -> dict[str, Any]:
        scenario = portable_path(self.scenario, root) if self.scenario else None
        return {
            'kind': 'doppeldata',
            'profile': portable_path(self.profile, root),
            'scenario': scenario,
            'rows': self.rows,
            'seed': self.seed,
        }


@dc.dataclass(frozen=True)
class DataFrameTable:
    """A DataFrame handed to the SDK. Its plan is not part of the definition, so no Spark call is made to build one."""

    dataframe: Any = dc.field(repr=False, compare=False)

    def to_json(self, root: Path) -> dict[str, Any]:
        return {'kind': 'dataframe'}


TableSource = PathTable | CatalogTable | DoppeldataTable | DataFrameTable


@dc.dataclass(frozen=True)
class ClusterSpec:
    """Compute fixed when it is provisioned, including the confs a session takes only when it starts."""

    backend: str
    runtime_version: str | None = None
    worker_type: str | None = None
    workers: int | None = None
    execution_class: str = 'STANDARD'
    static_conf: Mapping[str, str] = dc.field(default_factory=dict)
    region: str | None = None


@dc.dataclass(frozen=True)
class Compare:
    """How outputs are compared. `reference` is the candidate the others are compared against, the first unless named."""

    reference: str | None = None
    keys: tuple[str, ...] = ()
    rel_tol: float = 1e-9
    abs_tol: float = 0.0
    exclude_columns: tuple[str, ...] = ()


@dc.dataclass(frozen=True)
class Timing:
    """How runs are timed. `underpowered` allows fewer rounds than the significance test needs to reach `alpha`."""

    warmup_rounds: int = 1
    rounds: int = 7
    order: str = 'interleaved'
    seed: int = 42
    alpha: float = 0.01
    min_effect: float = 1.1
    underpowered: bool = False


@dc.dataclass(frozen=True)
class Arm:
    """One candidate inside a cell, with each table parameter bound to the dataset's table of that name."""

    cell_id: str
    candidate: Candidate
    tables: Mapping[str, TableSource]


@dc.dataclass(frozen=True)
class Cell:
    """One (dataset, conf, cluster). Its id reads `dataset/conf@cluster`."""

    id: str
    dataset: str
    conf: str
    cluster: str
    arms: tuple[Arm, ...]


class Experiment:
    """Candidates × datasets × confs × clusters, resolved and checked when it is built.

    A candidate's module found under `root` is imported fresh at every build, with `root` on
    `sys.path` only while it imports. So a module beside the experiment file imports without being
    installed, and an edit since the last build is what binds. A relative local path resolves
    against `root` too. `root` is the experiment file's directory, or the working directory in the SDK.
    """

    def __init__(
        self,
        *,
        candidates: Sequence[Any] | Mapping[str, Any],
        datasets: Mapping[str, Mapping[str, Any]],
        confs: Mapping[str, Mapping[str, Any]] | None = None,
        clusters: Mapping[str, ClusterSpec | Mapping[str, Any]] | None = None,
        compare: Compare | Mapping[str, Any] | None = None,
        timing: Timing | Mapping[str, Any] | None = None,
        baseline: str | Mapping[str, Any] | None = None,
        expect: Mapping[str, Any] | None = None,
        root: str | Path | None = None,
    ) -> None:
        self.root = Path(Path.cwd() if root is None else root).resolve()
        problems: list[Problem] = []
        self.candidates = resolve_candidates(candidates, self.root, problems)
        self.datasets = resolve_datasets(datasets, self.root, problems)
        self.confs = resolve_confs({'default': {}} if confs is None else confs, problems)
        self.clusters = resolve_clusters({'local': {'backend': 'local'}} if clusters is None else clusters, problems)
        self.compare = parse_section(Compare(), compare, 'compare', problems)
        self.timing = parse_section(Timing(), timing, 'timing', problems)
        self.expect = resolve_expect(expect, problems)
        problems.extend(binding_problems(self.candidates, declared_table_names(datasets)))
        problems.extend(compare_problems(self.compare))
        problems.extend(timing_problems(self.timing))
        reference = self.compare.reference
        declared = declared_candidate_names(candidates)
        if reference is None and self.candidates:
            self.compare = dc.replace(self.compare, reference=next(iter(self.candidates)))
        elif reference is not None and reference not in declared:
            problems.append(unknown_name('compare.reference', reference, 'candidate', declared))
        self.cells = expand(self.candidates, self.datasets, self.confs, self.clusters)
        self.baseline = self.resolve_baseline(baseline, problems)
        if problems:
            raise Refused(problems)

    @classmethod
    def from_toml(cls, path: str | Path) -> 'Experiment':
        path = Path(path)
        try:
            with path.open('rb') as fh:
                document = tomllib.load(fh)
        except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
            raise Refused([Problem('E-EXPERIMENT-007', str(path), str(error))]) from error
        problems = [
            Problem('E-EXPERIMENT-001', key, 'is not a section of an experiment', valid=SECTIONS) for key in document if key not in SECTIONS
        ]
        sections = {'candidates': {}, 'datasets': {}} | {key: value for key, value in document.items() if key in SECTIONS}
        try:
            experiment = cls(**sections, root=path.parent)
        except Refused as refused:
            raise Refused(problems + refused.problems) from None
        if problems:
            raise Refused(problems)
        return experiment

    def resolve_baseline(self, declared: str | Mapping[str, Any] | None, problems: list[Problem]) -> str | None:
        if isinstance(declared, Mapping):
            unknown_keys(declared, ('cell',), 'baseline', 'baseline', problems)
            declared = declared.get('cell')
        cell_ids = [cell.id for cell in self.cells]
        if declared is None:
            return cell_ids[0] if cell_ids else None
        if declared not in cell_ids:
            problems.append(unknown_name('baseline.cell', declared, 'cell', cell_ids))
        return declared

    def definition(self) -> dict[str, Any]:
        """The definition as written and resolved, which the `experiment` JSON Schema describes and the identity hashes.

        A local path under `root` is relative to it, so one experiment has one identity in every
        checkout. Each candidate is named by its import path, and each section holds every field.
        """
        return {
            'schema_version': SCHEMA_VERSION,
            'candidates': {name: candidate.to_json() for name, candidate in self.candidates.items()},
            'datasets': {
                name: {table: source.to_json(self.root) for table, source in tables.items()} for name, tables in self.datasets.items()
            },
            'confs': {name: dict(overlay) for name, overlay in self.confs.items()},
            'clusters': {name: plain(dc.asdict(spec)) for name, spec in self.clusters.items()},
            'compare': plain(dc.asdict(self.compare)),
            'timing': dc.asdict(self.timing),
            'baseline': {'cell': self.baseline},
            'expect': self.expect,
        }

    @property
    def definition_sha256(self) -> str:
        """The SHA-256 of the definition's JSON with sorted keys and no whitespace."""
        text = json.dumps(self.definition(), sort_keys=True, separators=(',', ':'), ensure_ascii=False)
        return hashlib.sha256(text.encode()).hexdigest()

    def fingerprints(self) -> dict[str, Any]:
        """What the code and generated data were when this build read them, kept beside the identity rather than in it.

        A candidate's fingerprint hashes its own source text, so an edit to a helper it calls does
        not change it. It is null where Python cannot read the source, as for a function typed into
        a REPL.
        """
        doppeldata = {
            f'{dataset}.{table}': {'profile_sha256': source.profile_sha256, 'scenario_sha256': source.scenario_sha256}
            for dataset, tables in self.datasets.items()
            for table, source in tables.items()
            if isinstance(source, DoppeldataTable)
        }
        return {'candidates': {name: candidate.source_sha256 for name, candidate in self.candidates.items()}, 'doppeldata': doppeldata}

    def experiment_id(self, started: dt.datetime) -> str:
        """The definition's first 12 hex digits, the start time in UTC, and 6 random hex digits."""
        return f'{self.definition_sha256[:12]}-{started.astimezone(dt.UTC):%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}'


SECTIONS = tuple(name for name in inspect.signature(Experiment.__init__).parameters if name not in ('self', 'root'))


def portable_path(path: str, root: Path) -> str:
    """`path` relative to `root` where it lies under it, so the definition does not name the checkout."""
    if path.startswith(REMOTE_PREFIXES):
        return path
    resolved = Path(path)
    return resolved.relative_to(root).as_posix() if resolved.is_relative_to(root) else resolved.as_posix()


def plain(value: Any) -> Any:
    """JSON's shapes for what a dataclass holds: tuples become lists and mappings become dicts."""
    if isinstance(value, Mapping):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [plain(item) for item in value]
    return value


def unknown_name(path: str, name: str, kind: str, known: Sequence[str]) -> Problem:
    return Problem('E-EXPERIMENT-004', path, f'{name!r} names no {kind} in this experiment', valid=tuple(known))


def unknown_keys(declared: Mapping[str, Any], allowed: tuple[str, ...], path: str, what: str, problems: list[Problem]) -> None:
    for key in sorted(set(declared) - set(allowed)):
        problems.append(Problem('E-EXPERIMENT-001', f'{path}.{key}', f'is not a field of {what}', valid=allowed))


def name_problems(section: str, name: str, problems: list[Problem]) -> None:
    if any(separator in name for separator in NAME_SEPARATORS):
        message = "may not hold '/', '@' or '.', which cell ids and problem paths use as separators"
        problems.append(Problem('E-EXPERIMENT-001', f'{section}.{name}', message))


def json_value_problems(value: Any, path: str, problems: list[Problem]) -> None:
    """A problem where `value` does not come back unchanged from JSON, which the identity is hashed over."""
    try:
        round_trips = json.loads(json.dumps(value, allow_nan=False)) == value
    except (TypeError, ValueError):
        round_trips = False
    if not round_trips:
        problems.append(Problem('E-EXPERIMENT-001', path, 'must be a JSON value: a string, number, boolean, list or table'))


def resolve_candidates(declared: Any, root: Path, problems: list[Problem]) -> dict[str, Candidate]:
    if isinstance(declared, Mapping):
        entries = list(declared.items())
    elif isinstance(declared, Sequence) and not isinstance(declared, str):
        entries = [(candidate_name(entry), entry) for entry in declared]
        names = [name for name, _ in entries]
        for name in sorted({name for name in names if names.count(name) > 1}):
            problems.append(
                Problem('E-EXPERIMENT-001', 'candidates', f'two candidates are named {name!r}', 'pass a mapping to name each one')
            )
    else:
        problems.append(Problem('E-EXPERIMENT-001', 'candidates', 'must be a table of name = "module:function"'))
        return {}
    if not entries:
        problems.append(Problem('E-EXPERIMENT-001', 'candidates', 'declares no candidate'))
    candidates = {}
    for name, entry in entries:
        name_problems('candidates', str(name), problems)
        candidate = resolve_candidate(str(name), entry, root, problems)
        if candidate is not None:
            candidates[candidate.name] = candidate
    return candidates


def declared_candidate_names(declared: Any) -> list[str]:
    """Each candidate's name as written, so a candidate that failed to import can still be named."""
    if isinstance(declared, Mapping):
        return [str(name) for name in declared]
    if isinstance(declared, Sequence) and not isinstance(declared, str):
        return [candidate_name(entry) for entry in declared]
    return []


def candidate_name(entry: Any) -> str:
    if isinstance(entry, str):
        return entry.rpartition(':')[2].rpartition('.')[2]
    return str(getattr(entry, '__name__', entry))


def resolve_candidate(name: str, entry: Any, root: Path, problems: list[Problem]) -> Candidate | None:
    path = f'candidates.{name}'
    target, kwargs = entry, {}
    if isinstance(entry, Mapping):
        unknown_keys(entry, ('ref', 'kwargs'), path, 'a candidate', problems)
        target, kwargs = entry.get('ref'), entry.get('kwargs', {})
        if not isinstance(kwargs, Mapping):
            problems.append(Problem('E-EXPERIMENT-001', f'{path}.kwargs', 'must be a table of keyword arguments'))
            kwargs = {}
    if isinstance(target, str):
        ref, function = target, import_ref(target, path, root, problems)
        if function is None:
            return None
    elif callable(target):
        module, qualname = getattr(target, '__module__', None), getattr(target, '__qualname__', None)
        if not isinstance(module, str) or not isinstance(qualname, str):
            hint = 'pass a function, and bind its keyword arguments with kwargs'
            problems.append(Problem('E-BINDING-003', path, f'a {type(target).__name__} is not a function', hint))
            return None
        ref, function = f'{module}:{qualname}', target
    else:
        problems.append(Problem('E-EXPERIMENT-001', path, 'must be "module:function", a callable, or a table with ref'))
        return None
    if not callable(function):
        problems.append(Problem('E-BINDING-003', path, f'{ref} is a {type(function).__name__}, not a function'))
        return None
    try:
        signature = inspect.signature(function)
    except (TypeError, ValueError):
        problems.append(Problem('E-BINDING-003', path, f'{ref} has no signature Python can read, so its tables cannot be bound'))
        return None
    parameters = [p for p in signature.parameters.values() if p.kind in TABLE_PARAMETER_KINDS]
    tables = tuple(p.name for p in parameters if p.default is p.empty)
    keywords = tuple(p.name for p in parameters if p.default is not p.empty)
    for key, value in kwargs.items():
        if key not in keywords:
            message = f'{ref} has no keyword parameter {key!r} with a default'
            problems.append(Problem('E-BINDING-004', f'{path}.kwargs.{key}', message, valid=keywords))
        else:
            json_value_problems(value, f'{path}.kwargs.{key}', problems)
    return Candidate(name, ref, function, dict(kwargs), tables, source_sha256(function))


def import_ref(ref: str, path: str, root: Path, problems: list[Problem]) -> Any:
    module_name, _, qualname = ref.partition(':')
    if not module_name or not qualname:
        problems.append(Problem('E-BINDING-002', path, f'{ref!r} is not an import path', 'write it as module:function'))
        return None
    try:
        target: Any = import_fresh(module_name, root)
    except (Exception, SystemExit) as error:
        problems.append(Problem('E-BINDING-002', path, f'importing {module_name} raised {type(error).__name__}: {error}'))
        return None
    for part in qualname.split('.'):
        if not hasattr(target, part):
            problems.append(Problem('E-BINDING-002', path, f'{module_name} has no {qualname}'))
            return None
        target = getattr(target, part)
    return target


def import_fresh(module_name: str, root: Path) -> types.ModuleType:
    """The module, read again from `root` where its top-level package lives there, and imported as usual elsewhere."""
    top = module_name.partition('.')[0]
    if not ((root / f'{top}.py').is_file() or (root / top).is_dir()):
        return importlib.import_module(module_name)
    for loaded in [loaded for loaded in sys.modules if loaded == top or loaded.startswith(f'{top}.')]:
        del sys.modules[loaded]
    importlib.invalidate_caches()
    sys.path.insert(0, str(root))
    try:
        return importlib.import_module(module_name)
    finally:
        sys.path.remove(str(root))


def source_sha256(function: Callable[..., Any]) -> str | None:
    try:
        return hashlib.sha256(inspect.getsource(function).encode()).hexdigest()
    except (OSError, TypeError):
        return None


def resolve_datasets(declared: Any, root: Path, problems: list[Problem]) -> dict[str, dict[str, TableSource]]:
    if not isinstance(declared, Mapping) or not declared:
        problems.append(Problem('E-EXPERIMENT-001', 'datasets', 'must declare at least one dataset'))
        return {}
    datasets = {}
    for name, tables in declared.items():
        name_problems('datasets', str(name), problems)
        if not isinstance(tables, Mapping) or not tables:
            problems.append(Problem('E-EXPERIMENT-001', f'datasets.{name}', 'must map at least one table name to its source'))
            continue
        resolved = {}
        for table, source in tables.items():
            table_source = resolve_table(source, f'datasets.{name}.{table}', root, problems)
            if table_source is not None:
                resolved[str(table)] = table_source
        datasets[str(name)] = resolved
    return datasets


def declared_table_names(declared: Any) -> dict[str, set[str]]:
    """Each dataset's table names as written, so a table whose source failed still binds."""
    if not isinstance(declared, Mapping):
        return {}
    return {str(name): {str(table) for table in tables} for name, tables in declared.items() if isinstance(tables, Mapping) and tables}


def resolve_table(source: Any, path: str, root: Path, problems: list[Problem]) -> TableSource | None:
    if isinstance(source, str | Path):
        resolved = resolve_path(str(source), path, root, problems)
        return PathTable(resolved, infer_format(str(source))) if resolved else None
    if hasattr(source, 'semanticHash'):
        return DataFrameTable(source)
    if not isinstance(source, Mapping):
        problems.append(Problem('E-EXPERIMENT-001', path, 'must be a path, a table with path, table or profile, or a DataFrame'))
        return None
    if 'path' in source:
        unknown_keys(source, ('path', 'format'), path, 'a path table', problems)
        resolved = resolve_path(str(source['path']), f'{path}.path', root, problems)
        return PathTable(resolved, str(source.get('format', infer_format(str(source['path']))))) if resolved else None
    if 'table' in source:
        unknown_keys(source, ('table',), path, 'a catalog table', problems)
        return CatalogTable(str(source['table']))
    if 'profile' in source:
        return resolve_doppeldata(source, path, root, problems)
    problems.append(Problem('E-EXPERIMENT-001', path, 'names no path, table or profile'))
    return None


def resolve_path(raw: str, path: str, root: Path, problems: list[Problem]) -> str | None:
    if raw.startswith(REMOTE_PREFIXES):
        return raw
    resolved = (root / Path(raw).expanduser()).resolve()
    if not resolved.exists():
        problems.append(Problem('E-EXPERIMENT-005', path, f'{resolved} does not exist'))
        return None
    return str(resolved)


def infer_format(raw: str) -> str:
    return FORMATS_BY_SUFFIX.get(Path(raw.rstrip('/')).suffix.lower(), DEFAULT_FORMAT)


def resolve_doppeldata(source: Mapping[str, Any], path: str, root: Path, problems: list[Problem]) -> DoppeldataTable | None:
    if not doppeldata_installed():
        problems.append(Problem('E-EXPERIMENT-006', path, 'is a doppeldata spec, and doppeldata is not installed', 'install doppeldata'))
        return None
    before = len(problems)
    unknown_keys(source, ('profile', 'scenario', 'rows', 'seed'), path, 'a doppeldata spec', problems)
    for key, floor in (('rows', 1), ('seed', 0)):
        value = source.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < floor:
            problems.append(Problem('E-EXPERIMENT-001', f'{path}.{key}', f'must be a whole number of at least {floor}'))
    profile = resolve_path(str(source['profile']), f'{path}.profile', root, problems)
    scenario = resolve_path(str(source['scenario']), f'{path}.scenario', root, problems) if 'scenario' in source else None
    profile_sha256 = file_sha256(profile, f'{path}.profile', problems) if profile else None
    scenario_sha256 = file_sha256(scenario, f'{path}.scenario', problems) if scenario else None
    if len(problems) > before or profile is None or profile_sha256 is None:
        return None
    return DoppeldataTable(profile, profile_sha256, scenario, scenario_sha256, source['rows'], source['seed'])


def file_sha256(file: str, path: str, problems: list[Problem]) -> str | None:
    try:
        return hashlib.sha256(Path(file).read_bytes()).hexdigest()
    except OSError as error:
        problems.append(Problem('E-EXPERIMENT-005', path, f'{file} cannot be read: {error.strerror}'))
        return None


def conf_text(value: Any) -> str | None:
    """A conf value as Spark reads it, or None for a value no conf holds."""
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, str | int | float):
        return str(value)
    return None


def resolve_confs(declared: Any, problems: list[Problem]) -> dict[str, dict[str, str]]:
    if not isinstance(declared, Mapping) or not declared:
        problems.append(Problem('E-EXPERIMENT-001', 'confs', 'must declare at least one conf overlay, which may be empty'))
        return {}
    confs: dict[str, dict[str, str]] = {}
    for name, overlay in declared.items():
        name_problems('confs', str(name), problems)
        if not isinstance(overlay, Mapping):
            problems.append(Problem('E-EXPERIMENT-001', f'confs.{name}', 'must be a table of Spark conf keys and values'))
            continue
        confs[str(name)] = {}
        for key, value in overlay.items():
            at = f'confs.{name}."{key}"'
            text = conf_text(value)
            if key in static_conf_keys():
                hint = 'set it in static_conf on a cluster, which applies it when the session starts'
                problems.append(
                    Problem('E-EXPERIMENT-003', at, f'{key} is fixed when the session starts, so spark.conf.set refuses it', hint)
                )
            elif text is None:
                problems.append(Problem('E-EXPERIMENT-001', at, 'must be a string, number or boolean'))
            else:
                confs[str(name)][key] = text
    return confs


def resolve_clusters(declared: Any, problems: list[Problem]) -> dict[str, ClusterSpec]:
    if not isinstance(declared, Mapping) or not declared:
        problems.append(Problem('E-EXPERIMENT-001', 'clusters', 'must declare at least one cluster'))
        return {}
    clusters = {}
    for name, spec in declared.items():
        path = f'clusters.{name}'
        name_problems('clusters', str(name), problems)
        if isinstance(spec, Mapping) and 'backend' not in spec:
            problems.append(Problem('E-EXPERIMENT-001', f'{path}.backend', 'is required', valid=BACKENDS))
            continue
        cluster = parse_section(ClusterSpec(backend='local'), spec, path, problems)
        if cluster.backend not in BACKENDS:
            problems.append(Problem('E-EXPERIMENT-001', f'{path}.backend', f'{cluster.backend!r} is not a backend', valid=BACKENDS))
        if cluster.workers is not None and cluster.workers < 1:
            problems.append(Problem('E-EXPERIMENT-001', f'{path}.workers', 'must be at least 1'))
        clusters[str(name)] = cluster
    return clusters


def resolve_expect(declared: Any, problems: list[Problem]) -> dict[str, Any]:
    if declared is None:
        return {}
    if not isinstance(declared, Mapping):
        problems.append(Problem('E-EXPERIMENT-001', 'expect', 'must be a table of declared promises'))
        return {}
    for key, value in declared.items():
        json_value_problems(value, f'expect.{key}', problems)
    return dict(declared)


SectionType = typing.TypeVar('SectionType', ClusterSpec, Compare, Timing)
NOT_CONFORMING: Any = object()
SCALAR_DESCRIPTIONS = {str: 'a string', int: 'a whole number', float: 'a number', bool: 'true or false'}


def parse_section(default: SectionType, declared: Any, path: str, problems: list[Problem]) -> SectionType:
    """`default` with each field the table sets, type-checked. A field that fails keeps its default."""
    if declared is None:
        return default
    if isinstance(declared, type(default)):
        declared = dc.asdict(declared)
    if not isinstance(declared, Mapping):
        problems.append(Problem('E-EXPERIMENT-001', path, 'must be a table'))
        return default
    hints = typing.get_type_hints(type(default))
    values = {}
    for key, value in declared.items():
        if key not in hints:
            problems.append(Problem('E-EXPERIMENT-001', f'{path}.{key}', f'is not a field of {path}', valid=tuple(hints)))
            continue
        converted = conform(value, hints[key])
        if converted is NOT_CONFORMING:
            problems.append(Problem('E-EXPERIMENT-001', f'{path}.{key}', f'must be {describe(hints[key])}'))
            continue
        values[key] = converted
    return dc.replace(default, **values)


def conform(value: Any, hint: Any) -> Any:
    """`value` in the shape a section field of type `hint` holds, or `NOT_CONFORMING`."""
    origin = typing.get_origin(hint)
    if origin in (typing.Union, types.UnionType):
        if value is None:
            return None if type(None) in typing.get_args(hint) else NOT_CONFORMING
        arms = (conform(value, arm) for arm in typing.get_args(hint) if arm is not type(None))
        return next((converted for converted in arms if converted is not NOT_CONFORMING), NOT_CONFORMING)
    if origin is tuple:
        if isinstance(value, list | tuple) and all(isinstance(item, str) for item in value):
            return tuple(value)
        return NOT_CONFORMING
    if origin is Mapping:
        if not isinstance(value, Mapping):
            return NOT_CONFORMING
        texts = {str(key): conf_text(item) for key, item in value.items()}
        return NOT_CONFORMING if None in texts.values() else texts
    if hint is bool:
        return value if isinstance(value, bool) else NOT_CONFORMING
    if isinstance(value, bool):
        return NOT_CONFORMING
    if hint is int:
        return value if isinstance(value, int) else NOT_CONFORMING
    if hint is float:
        return float(value) if isinstance(value, int | float) else NOT_CONFORMING
    return value if isinstance(value, hint) else NOT_CONFORMING


def describe(hint: Any) -> str:
    origin = typing.get_origin(hint)
    if origin in (typing.Union, types.UnionType):
        return ' or '.join(describe(arm) for arm in typing.get_args(hint) if arm is not type(None))
    if origin is tuple:
        return 'a list of strings'
    if origin is Mapping:
        return 'a table of conf keys and values'
    return SCALAR_DESCRIPTIONS[hint]


def binding_problems(candidates: Mapping[str, Candidate], tables_by_dataset: Mapping[str, set[str]]) -> list[Problem]:
    problems = []
    for candidate in candidates.values():
        for dataset, tables in tables_by_dataset.items():
            for parameter in candidate.tables:
                if parameter not in tables:
                    problems.append(
                        Problem(
                            'E-BINDING-001',
                            f'candidates.{candidate.name}.{parameter}',
                            f'parameter {parameter!r} names no table in dataset {dataset!r}',
                            f'add {parameter} to [datasets.{dataset}] or rename the parameter',
                            valid=tuple(sorted(tables)),
                        )
                    )
    return problems


def compare_problems(compare: Compare) -> list[Problem]:
    return [
        Problem('E-EXPERIMENT-001', f'compare.{field}', 'must be at least 0')
        for field, value in (('rel_tol', compare.rel_tol), ('abs_tol', compare.abs_tol))
        if value < 0
    ]


def timing_problems(timing: Timing) -> list[Problem]:
    problems = []
    if timing.order not in TIMING_ORDERS:
        problems.append(Problem('E-EXPERIMENT-001', 'timing.order', f'{timing.order!r} is not an order', valid=TIMING_ORDERS))
    if timing.warmup_rounds < 0:
        problems.append(Problem('E-EXPERIMENT-001', 'timing.warmup_rounds', 'must be at least 0'))
    if not 0 < timing.alpha < 1:
        problems.append(Problem('E-EXPERIMENT-001', 'timing.alpha', 'must be above 0 and below 1'))
    if timing.min_effect < 1:
        problems.append(Problem('E-EXPERIMENT-001', 'timing.min_effect', 'must be at least 1, as a ratio of the slower arm to the faster'))
    if timing.rounds < 1:
        problems.append(Problem('E-EXPERIMENT-001', 'timing.rounds', 'must be at least 1'))
    elif 0 < timing.alpha < 1 and not timing.underpowered and smallest_reachable_p(timing.rounds) >= timing.alpha:
        fewest = fewest_rounds_reaching(timing.alpha)
        problems.append(
            Problem(
                'E-EXPERIMENT-002',
                'timing.rounds',
                f'{timing.rounds} rounds cannot give a p-value below alpha {timing.alpha}, and {fewest} can',
                'raise timing.rounds, or set timing.underpowered = true to accept fewer',
            )
        )
    return problems


def expand(
    candidates: Mapping[str, Candidate],
    datasets: Mapping[str, Mapping[str, TableSource]],
    confs: Mapping[str, Mapping[str, str]],
    clusters: Mapping[str, ClusterSpec],
) -> tuple[Cell, ...]:
    """Every (dataset, conf, cluster) in declaration order, each holding one arm per candidate."""
    cells = []
    for dataset, conf, cluster in itertools.product(datasets, confs, clusters):
        cell_id = f'{dataset}/{conf}@{cluster}'
        tables = datasets[dataset]
        arms = tuple(
            Arm(cell_id, candidate, {parameter: tables[parameter] for parameter in candidate.tables if parameter in tables})
            for candidate in candidates.values()
        )
        cells.append(Cell(cell_id, dataset, conf, cluster, arms))
    return tuple(cells)
