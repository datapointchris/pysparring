"""Diff the physical plans of two or more DataFrames, and time them when asked."""

from __future__ import annotations

import collections
import contextlib
import dataclasses as dc
import difflib
import io
import random
import re
import statistics
import time
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Sequence
from typing import TYPE_CHECKING
from typing import Any

from rich.console import Console
from rich.console import ConsoleOptions
from rich.console import Group
from rich.table import Table
from rich.text import Text

from pysparring.rendering import plain_text
from pysparring.rendering import show
from pysparring.significance import SpeedVerdict
from pysparring.significance import fewest_runs
from pysparring.significance import speed_verdict

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SCHEDULE_SEED = 0
# With fewer warm-up rounds, the first measured round still runs slow. docs/how-each-comparison-decides.md has the numbers.
WARMUP_ROUNDS = 3

# Each id Spark renumbers on every build. A rewrite is anchored to the syntax around its id,
# so a number inside an expression, as in count(1), is left alone.
ID_REWRITES = [
    (re.compile(r'#\d+L?'), ''),  # expression ids: amount#10, count#103L, scalar-subquery#45
    (re.compile(r',? ?\[plan_id=\d+\]'), ''),  # exchange ids, Spark 3.2 on
    (re.compile(r',? ?\[id=#\d+\]'), ''),  # exchange ids before Spark 3.2
    (re.compile(r' ?\[codegen id : \d+\]'), ''),
    (re.compile(r'((?:Shuffle|Broadcast|Result|TableCache)QueryStage) \d+'), r'\1'),
    (re.compile(r'(Hosting operator id = )\d+'), r'\1N'),
    (re.compile(r'(Reuses operator id: )\d+'), r'\1N'),
]
# Formatted mode numbers every node, so one added node renumbers all those printed after it.
TREE_NODE_NUMBER = re.compile(r'\s*\(\d+\)$')
DETAIL_HEADER = re.compile(r'^\(\d+\) (.+?)\s*$')
# A header's trailing bracket, as in `ReusedExchange [Reuses operator id: N]`, is not part of the operator.
HEADER_ANNOTATION = re.compile(r' \[[^\]]*\]$')
PHYSICAL_PLAN = '== Physical Plan =='
CACHED_SCAN = 'InMemoryTableScan'


def explain_formatted(df: DataFrame, name: str = 'DataFrame') -> str:
    """The plan as `explain("formatted")` prints it. Classic and Spark Connect sessions both print it to stdout."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain(mode='formatted')
    text = buffer.getvalue()
    if PHYSICAL_PLAN not in text:
        raise RuntimeError(f'explain(mode="formatted") on {name} printed no physical plan to stdout, and printed {text[:200]!r}')
    return text


def read_plan(text: str) -> tuple[str, dict[str, int]]:
    """A formatted plan with its ids and node numbers stripped, and how many of each operator it holds."""
    lines: list[str] = []
    operators: collections.Counter[str] = collections.Counter()
    in_tree = False
    for line in text.splitlines():
        for pattern, replacement in ID_REWRITES:
            line = pattern.sub(replacement, line)
        header = DETAIL_HEADER.match(line)
        if header:
            line = header.group(1)
            operators[HEADER_ANNOTATION.sub('', line)] += 1
        elif in_tree:
            line = TREE_NODE_NUMBER.sub('', line)
        if line.startswith((PHYSICAL_PLAN, 'Subquery:')):
            in_tree = True
        elif not line.strip():
            in_tree = False
        lines.append(line.rstrip())
    return '\n'.join(lines).strip() + '\n', dict(operators)


def plan_diff(reference: str, reference_plan: str, name: str, plan: str) -> list[str]:
    return list(difflib.unified_diff(reference_plan.splitlines(), plan.splitlines(), fromfile=reference, tofile=name, lineterm=''))


def timing_rounds(names: Iterable[str], runs: int) -> Iterator[tuple[int, list[str]]]:
    """Each round's number and the order it runs every name in. Rounds numbered 0 or below are warm-ups."""
    order = list(names)
    schedule = random.Random(SCHEDULE_SEED)
    for round_number in range(1 - WARMUP_ROUNDS, runs + 1):
        schedule.shuffle(order)
        yield round_number, list(order)


def time_runs(frames: dict[str, DataFrame], runs: int) -> dict[str, list[float]]:
    """Milliseconds per run of each DataFrame, written to Spark's no-op sink."""
    samples: dict[str, list[float]] = {name: [] for name in frames}
    with Console(stderr=True).status('warming up') as status:
        for round_number, order in timing_rounds(frames, runs):
            if round_number > 0:
                status.update(f'round {round_number} of {runs}')
            for name in order:
                started = time.perf_counter()
                frames[name].write.format('noop').mode('overwrite').save()
                if round_number > 0:
                    samples[name].append((time.perf_counter() - started) * 1000)
    return samples


@dc.dataclass(frozen=True, repr=False)
class PlanComparison:
    """Every DataFrame's plan against the first's, and when timed, its run times and speed verdict."""

    names: tuple[str, ...]
    plans: dict[str, str]
    normalized_plans: dict[str, str]
    diffs: dict[str, list[str]]
    operator_counts: dict[str, dict[str, int]]
    timings_ms: dict[str, list[float]]
    verdicts: dict[str, SpeedVerdict]

    @property
    def reference(self) -> str:
        return self.names[0]

    def operator_deltas(self) -> dict[str, dict[str, int]]:
        """Each operator whose count is not the same in every plan, with its count in each."""
        operators = sorted({op for counts in self.operator_counts.values() for op in counts})
        table = {op: {name: self.operator_counts[name].get(op, 0) for name in self.names} for op in operators}
        return {op: counts for op, counts in table.items() if len(set(counts.values())) > 1}

    def plan_section(self) -> Iterator[Any]:
        for name in self.names[1:]:
            yield Text(f'Plan diff, {self.reference} → {name}', style='bold')
            if not self.diffs[name]:
                yield Text('  identical once ids are stripped\n', style='dim')
                continue
            for line in self.diffs[name]:
                yield Text(line, style=diff_style(line))
            yield Text('')
        deltas = self.operator_deltas()
        if deltas:
            yield Text('Operators whose counts differ', style='bold')
            table = Table()
            table.add_column('operator')
            for name in self.names:
                table.add_column(name, justify='right')
            for op, counts in deltas.items():
                table.add_row(op, *[str(counts[name]) for name in self.names])
            yield table
        else:
            yield Text('Every operator count matches.')
        if any('isFinalPlan=false' in plan for plan in self.plans.values()):
            yield Text('Plans as explained before running. Adaptive execution can still change joins and partitions.', style='dim')

    def timing_section(self) -> Iterator[Any]:
        if not self.timings_ms:
            return
        yield Text('\nRun times, ms', style='bold')
        table = Table()
        for column in ('', 'median', 'min', 'max', 'runs'):
            table.add_column(column, justify='left' if not column else 'right')
        for name in self.names:
            samples = self.timings_ms[name]
            table.add_row(name, f'{statistics.median(samples):,.0f}', f'{min(samples):,.0f}', f'{max(samples):,.0f}', str(len(samples)))
        yield table
        cached = [name for name in self.names if CACHED_SCAN in self.plans[name]]
        if cached:
            yield Text(f'{", ".join(cached)} read cached data, so those runs time the cache.', style='dim')
        for verdict in self.verdicts.values():
            yield Text(verdict.sentence, style='green' if verdict.faster else 'yellow')

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> Iterator[Any]:
        yield from self.plan_section()
        yield from self.timing_section()

    def __str__(self) -> str:
        return plain_text(self)

    def __repr__(self) -> str:
        changed = len(self.operator_deltas())
        faster = [v.faster for v in self.verdicts.values() if v.faster]
        timing = f', faster: {", ".join(faster)}' if faster else ', no clear difference' if self.verdicts else ''
        return f'<PlanComparison {" → ".join(self.names)}: {changed} operator counts differ{timing}>'


def diff_style(line: str) -> str:
    if line.startswith(('---', '+++')):
        return 'bold'
    if line.startswith('@@'):
        return 'cyan'
    if line.startswith('-'):
        return 'red'
    if line.startswith('+'):
        return 'green'
    return ''


def compare_plans(*dataframes: DataFrame, names: Sequence[str] | None = None, runs: int = 0) -> PlanComparison:
    """Print each DataFrame's plan diffed against the first's, and the operators whose counts differ.

    `runs=N` also times each DataFrame N times with a no-op write, and says which is faster when the
    difference is significant. The plans print before the timing starts.
    """
    labels = tuple(names) if names is not None else tuple(f'df{i}' for i in range(1, len(dataframes) + 1))
    if len(dataframes) < 2:
        raise ValueError('compare_plans needs at least two DataFrames')
    if len(labels) != len(dataframes) or len(set(labels)) != len(labels):
        raise ValueError(f'names must give each of the {len(dataframes)} DataFrames its own name, and got {list(labels)}')
    if runs < 0 or 0 < runs < fewest_runs():
        raise ValueError(f'runs={runs} is too few to show a difference. Use runs={fewest_runs()} or more, or runs=0 to skip timing.')
    frames = dict(zip(labels, dataframes, strict=True))
    plans = {name: explain_formatted(df, name) for name, df in frames.items()}
    read = {name: read_plan(text) for name, text in plans.items()}
    normalized = {name: plan for name, (plan, _) in read.items()}
    reference = labels[0]
    comparison = PlanComparison(
        names=labels,
        plans=plans,
        normalized_plans=normalized,
        diffs={name: plan_diff(reference, normalized[reference], name, normalized[name]) for name in labels[1:]},
        operator_counts={name: counts for name, (_, counts) in read.items()},
        timings_ms={},
        verdicts={},
    )
    show(Group(*comparison.plan_section()))
    if not runs:
        return comparison
    timings = time_runs(frames, runs)
    verdicts = {name: speed_verdict(name, timings[name], reference, timings[reference]) for name in labels[1:]}
    comparison = dc.replace(comparison, timings_ms=timings, verdicts=verdicts)
    show(Group(*comparison.timing_section()))
    return comparison
