# pysparring docs

How to use pysparring, and why it is shaped the way it is. The design page states its design as
requirements the code must meet, and says what was weighed and rejected.

| Page | Holds |
| --- | --- |
| [user-guide.md](user-guide.md) | installing on each platform, a first comparison, reading every part of both reports, choosing runs, keys and tolerances, testing with `assert_matches`, a refactor step by step, sampling real tables so they still join, color in notebooks and logs, troubleshooting, and what pysparring does not do |
| [how-each-comparison-decides.md](how-each-comparison-decides.md) | why a plan never names a winner, how ids are stripped, why timing uses a no-op write, a warm-up and shuffled rounds and leaves the cache alone, the significance test and its floor of 5 runs, how outputs compare with a tolerance with and without keys, and what the two functions leave out |

Each page's headings state its findings, so a page's `##` lines are its contents.
