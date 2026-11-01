"""Compare PySpark DataFrames: their physical plans, how fast they run, and whether their outputs match."""

from pysparring.outputs import OutputComparison
from pysparring.outputs import compare_outputs
from pysparring.plans import PlanComparison
from pysparring.plans import compare_plans
from pysparring.significance import SpeedVerdict
