"""Whether one set of run times is faster than another, by an exact two-sided Mann-Whitney U test."""

import dataclasses as dc
import math
import statistics

ALPHA = 0.01
MIN_RATIO = 1.1


def u_counts(n: int, m: int) -> list[int]:
    """How many orderings of n and m samples give each U from 0 to n*m: the Gaussian binomial [n+m, m]."""
    top = n * m
    counts = [1] + [0] * top
    for i in range(1, m + 1):
        for k in range(top, n + i - 1, -1):
            counts[k] -= counts[k - n - i]
        for k in range(i, top + 1):
            counts[k] += counts[k - i]
    return counts


def mann_whitney_p(a: list[float], b: list[float]) -> float:
    """The exact two-sided p-value for a and b coming from one distribution. Ties count half."""
    u = sum(1.0 if x > y else 0.5 if x == y else 0.0 for x in a for y in b)
    counts = u_counts(len(a), len(b))
    total = math.comb(len(a) + len(b), len(b))
    lower = sum(counts[: math.floor(u) + 1]) / total
    upper = sum(counts[math.ceil(u) :]) / total
    return min(1.0, 2 * min(lower, upper))


def smallest_reachable_p(runs: int) -> float:
    return 2 / math.comb(2 * runs, runs)


@dc.dataclass(frozen=True)
class SpeedVerdict:
    """One DataFrame's run times against the reference's. `faster` is None when the difference is not clear."""

    name: str
    reference: str
    median_ms: float
    reference_median_ms: float
    ratio: float
    p_value: float
    faster: str | None
    runs: int

    @property
    def sentence(self) -> str:
        runs = f'{self.runs} runs each'
        if self.faster is None:
            return f'no clear difference between {self.reference} and {self.name} (ratio {self.ratio:.2f}x, p={self.p_value:.3f}, {runs})'
        slower = self.reference if self.faster == self.name else self.name
        return f'{self.faster} ran {self.ratio:.2f}x faster than {slower} (p={self.p_value:.4f}, {runs})'


def speed_verdict(name: str, samples_ms: list[float], reference: str, reference_ms: list[float]) -> SpeedVerdict:
    """Faster needs p below ALPHA and the slower median at least MIN_RATIO times the faster one."""
    median, reference_median = statistics.median(samples_ms), statistics.median(reference_ms)
    ratio = max(median, reference_median) / min(median, reference_median)
    p_value = mann_whitney_p(samples_ms, reference_ms)
    faster = None
    if p_value < ALPHA and ratio >= MIN_RATIO:
        faster = name if median < reference_median else reference
    return SpeedVerdict(name, reference, median, reference_median, ratio, p_value, faster, len(samples_ms))
