import datetime as dt
import json
import re
import sys
from pathlib import Path

import jsonschema
import pytest
from pysparring import Experiment
from pysparring import Refused
from pysparring import experiment
from pysparring.schemas import schema_text

PIPELINES = """
def join_then_aggregate(orders, customers):
    return orders


def aggregate_then_join(orders, customers, min_amount=0):
    return orders
"""


@pytest.fixture
def root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A directory holding a candidate module and two local tables, imported fresh in each test."""
    monkeypatch.setattr(sys, 'path', list(sys.path))
    monkeypatch.delitem(sys.modules, 'pipelines', raising=False)
    (tmp_path / 'pipelines.py').write_text(PIPELINES)
    (tmp_path / 'data' / 'orders').mkdir(parents=True)
    (tmp_path / 'data' / 'customers.csv').write_text('id\n1\n')
    return tmp_path


def test_toml_and_sdk_forms_expand_to_one_definition(root: Path) -> None:
    (root / 'experiment.toml').write_text(
        """
[candidates]
join_then_aggregate = "pipelines:join_then_aggregate"

[candidates.aggregate_then_join_min10]
ref = "pipelines:aggregate_then_join"
kwargs = { min_amount = 10 }

[datasets.local]
orders = "data/orders/"
customers = "data/customers.csv"

[datasets.catalog]
orders = { table = "sales.orders" }
customers = { path = "data/customers.csv", format = "parquet" }

[confs.default]

[confs.broadcast_off]
"spark.sql.autoBroadcastJoinThreshold" = -1

[compare]
keys = ["segment"]
"""
    )
    from_toml = Experiment.from_toml(root / 'experiment.toml')

    assert [cell.id for cell in from_toml.cells] == [
        'local/default@local',
        'local/broadcast_off@local',
        'catalog/default@local',
        'catalog/broadcast_off@local',
    ]
    arm = from_toml.cells[0].arms[1]
    assert (arm.candidate.name, dict(arm.candidate.kwargs)) == ('aggregate_then_join_min10', {'min_amount': 10})
    assert {table: source.to_json() for table, source in arm.tables.items()} == {
        'orders': {'kind': 'path', 'path': str(root / 'data' / 'orders'), 'format': 'parquet'},
        'customers': {'kind': 'path', 'path': str(root / 'data' / 'customers.csv'), 'format': 'csv'},
    }
    assert from_toml.confs['broadcast_off'] == {'spark.sql.autoBroadcastJoinThreshold': '-1'}
    assert (from_toml.compare.reference, from_toml.baseline) == ('join_then_aggregate', 'local/default@local')
    validator = jsonschema.Draft202012Validator(json.loads(schema_text('experiment')))
    validator.validate(from_toml.definition())

    import pipelines

    from_sdk = Experiment(
        candidates={
            'join_then_aggregate': pipelines.join_then_aggregate,
            'aggregate_then_join_min10': {'ref': pipelines.aggregate_then_join, 'kwargs': {'min_amount': 10}},
        },
        datasets={
            'local': {'orders': root / 'data' / 'orders', 'customers': 'data/customers.csv'},
            'catalog': {'orders': {'table': 'sales.orders'}, 'customers': {'path': 'data/customers.csv', 'format': 'parquet'}},
        },
        confs={'default': {}, 'broadcast_off': {'spark.sql.autoBroadcastJoinThreshold': '-1'}},
        compare=experiment.Compare(keys=('segment',)),
        root=root,
    )
    assert from_sdk.definition_sha256 == from_toml.definition_sha256
    started = dt.datetime(2026, 10, 21, 22, 5, 9, tzinfo=dt.timezone(dt.timedelta(hours=-4)))
    assert re.fullmatch(r'[0-9a-f]{12}-20261022T020509Z', from_sdk.experiment_id(started))


def test_every_problem_is_refused_together(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(experiment, 'doppeldata_installed', lambda: False)
    (root / 'experiment.toml').write_text(
        """
[candidates]
good = "pipelines:join_then_aggregate"
missing = "pipelines:no_such_function"
bad_kwarg = { ref = "pipelines:aggregate_then_join", kwargs = { min_ammount = 10 } }

[datasets.orders_only]
orders = "data/absent/"

[datasets.synthetic]
orders = { profile = "profiles/orders.yaml", rows = 1000, seed = 7 }
customers = { table = "sales.customers" }

[confs.static]
"spark.sql.extensions" = "io.delta.sql.DeltaSparkSessionExtension"

[compare]
reference = "nobody"

[timing]
rounds = 3
order = "random"

[timings]
rounds = 9

[baseline]
cell = "nowhere/default@local"
"""
    )
    with pytest.raises(Refused) as refused:
        Experiment.from_toml(root / 'experiment.toml')

    assert sorted((problem.code, problem.path) for problem in refused.value.problems) == [
        ('E-BINDING-001', 'candidates.bad_kwarg'),
        ('E-BINDING-001', 'candidates.good'),
        ('E-BINDING-002', 'candidates.missing'),
        ('E-BINDING-004', 'candidates.bad_kwarg.kwargs.min_ammount'),
        ('E-EXPERIMENT-001', 'timing.order'),
        ('E-EXPERIMENT-001', 'timings'),
        ('E-EXPERIMENT-002', 'timing.rounds'),
        ('E-EXPERIMENT-003', 'confs.static."spark.sql.extensions"'),
        ('E-EXPERIMENT-004', 'baseline.cell'),
        ('E-EXPERIMENT-004', 'compare.reference'),
        ('E-EXPERIMENT-005', 'datasets.orders_only.orders'),
        ('E-EXPERIMENT-006', 'datasets.synthetic.orders'),
    ]
    binding = next(problem for problem in refused.value.problems if problem.path == 'candidates.good')
    assert binding.message == "parameter 'customers' names no table in dataset 'orders_only'"
