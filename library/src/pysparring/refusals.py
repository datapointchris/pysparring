"""What pysparring refuses before the first Spark call, each with a stable code and the field that failed.

A code never changes meaning, and a retired code is never reused. `CODES` is the registry, so a code
is declared in one place.
"""

import dataclasses as dc
from typing import Any

CODES = {
    'E-EXPERIMENT-001': 'a field of the experiment has the wrong type or value, or is not a field',
    'E-EXPERIMENT-002': 'timing.rounds is too few for the significance test to reach timing.alpha',
    'E-EXPERIMENT-003': 'a conf overlay sets a conf Spark fixes when the session starts',
    'E-EXPERIMENT-004': 'compare.reference or baseline.cell names nothing the experiment declares',
    'E-EXPERIMENT-005': 'a local input path does not exist',
    'E-EXPERIMENT-006': 'a table is a doppeldata spec, and doppeldata is not installed',
    'E-EXPERIMENT-007': 'the experiment file cannot be read as TOML',
    'E-BINDING-001': "a candidate's table parameter names no table in a dataset",
    'E-BINDING-002': "a candidate's import path does not resolve",
    'E-BINDING-003': "a candidate's import path resolves to something that is not callable",
    'E-BINDING-004': "a candidate's kwargs name no keyword parameter with a default",
}


@dc.dataclass(frozen=True)
class Problem:
    """One reason to refuse, located by a dotted path into the experiment as it is written.

    `valid` holds the values the field accepts, where they are a closed set.
    """

    code: str
    path: str
    message: str
    hint: str | None = None
    valid: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.code not in CODES:
            raise ValueError(f'{self.code} is not a registered refusal code')

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


class Refused(ValueError):
    """Every problem found before the first Spark call, raised together so one pass fixes them all."""

    def __init__(self, problems: list[Problem]) -> None:
        self.problems = problems
        lines = [f'{problem.code} {problem.path}: {problem.message}' for problem in problems]
        super().__init__('\n'.join(lines))
