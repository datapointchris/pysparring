"""Compare PySpark DataFrames: their physical plans, how fast they run, and whether their outputs match.

pysparring is two functions. Each prints its report and returns it as an object.

- `compare_plans` diffs the physical plans of two or more DataFrames, lists the operators whose
  counts differ, and with `runs` times them and says which is faster.
- `compare_outputs` says whether two DataFrames hold the same rows, and when they do not, which
  columns differ and how.

Example:
    ```python
    from pysparring import compare_outputs
    from pysparring import compare_plans

    compare_plans(old, new, names=['old', 'new'], runs=5)
    compare_outputs(old, new, keys='customer_id', names=('old', 'new')).assert_matches()
    ```

The user guide is https://github.com/datapointchris/pysparring/blob/main/docs/user-guide.md.
"""

from pysparring.outputs import OutputComparison
from pysparring.outputs import compare_outputs
from pysparring.plans import PlanComparison
from pysparring.plans import compare_plans
from pysparring.significance import SpeedVerdict
