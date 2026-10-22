"""What the speed verdict's significance test can reach, read before a run so too few rounds are refused."""

import math


def smallest_reachable_p(rounds: int) -> float:
    """The smallest p-value a two-sided exact Mann-Whitney U test gives with `rounds` samples per arm."""
    return 2 / math.comb(2 * rounds, rounds)


def fewest_rounds_reaching(alpha: float) -> int:
    rounds = 1
    while smallest_reachable_p(rounds) >= alpha:
        rounds += 1
    return rounds
