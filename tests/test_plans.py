"""compare_plans' plan reading and speed verdict, over real Spark output in fixtures/."""

import json
from pathlib import Path

import pytest

from pysparring.plans import read_plan
from pysparring.significance import mann_whitney_p
from pysparring.significance import speed_verdict

FIXTURES = Path(__file__).parent.parent / 'fixtures'
RELEASES = sorted(path.name for path in FIXTURES.glob('spark-*'))


def plan(release: str, name: str) -> str:
    return (FIXTURES / release / f'explain-{name}.txt').read_text()


@pytest.mark.parametrize('release', RELEASES)
def test_a_rebuilt_plan_differs_as_printed_and_matches_once_ids_are_stripped(release):
    first, rebuilt = plan(release, 'join_then_aggregate-broadcast_off'), plan(release, 'join_then_aggregate-broadcast_off-rebuilt')
    assert first != rebuilt
    assert read_plan(first) == read_plan(rebuilt)


@pytest.mark.parametrize('release', RELEASES)
def test_turning_broadcast_off_trades_the_broadcast_join_for_a_sort_merge_join(release):
    _, on = read_plan(plan(release, 'join_then_aggregate-broadcast_on'))
    _, off = read_plan(plan(release, 'join_then_aggregate-broadcast_off'))
    delta = {op: off.get(op, 0) - on.get(op, 0) for op in on.keys() | off.keys() if off.get(op, 0) != on.get(op, 0)}
    assert delta == {'BroadcastExchange': -1, 'BroadcastHashJoin': -1, 'Exchange': 2, 'Sort': 2, 'SortMergeJoin': 1}


@pytest.mark.parametrize('release', RELEASES)
def test_the_speed_verdict_needs_significance_and_a_large_enough_ratio(release):
    timings = json.loads((FIXTURES / release / 'timings-interleaved.json').read_text())
    off = speed_verdict(
        'aggregate_then_join',
        timings['aggregate_then_join:broadcast_off'],
        'join_then_aggregate',
        timings['join_then_aggregate:broadcast_off'],
    )
    assert off.faster == 'aggregate_then_join'
    assert mann_whitney_p([1, 2, 3, 4, 5], [6, 7, 8, 9, 10]) == 2 / 252
    assert speed_verdict('b', [10, 12, 11, 13, 9], 'a', [11, 10, 12, 9, 13]).faster is None
