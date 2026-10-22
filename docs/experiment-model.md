# The experiment model

## An experiment is four declared dimensions, expanded before any Spark call

An experiment declares candidates, datasets, confs and clusters. Nothing is discovered.

- **A candidate is a transform function.** Its parameters without a default name the tables it
  receives, the way pytest injects fixtures by name. A keyword parameter with a default is not a
  table, and `kwargs` binds it. So one function with two `kwargs` values is two candidates, which is
  asv's `params`.
- **A dataset maps table names to sources.** A source is a local or remote path, a catalog table, a
  doppeldata spec, or in the SDK a DataFrame.
- **A conf is a named overlay of runtime SQL confs.** Overlays are applied with `spark.conf.set`
  between runs on one session.
- **A cluster is compute fixed when it is provisioned.** That includes `static_conf`, the confs a
  session takes only when it starts.

A cell is one (dataset, conf, cluster), and its id reads `dataset/conf@cluster`. An arm is one
candidate inside a cell. Timing compares arms within a cell. The rank-stability table compares
cells against `baseline.cell`, which defaults to the first.

## The TOML file and the SDK object take the same sections

`Experiment.from_toml` passes each TOML section to `Experiment` as the keyword of the same name.
`compare` and `timing` are tables in TOML and `Compare` and `Timing` in the SDK, or plain mappings
in either. So a field is named and placed the same way in both forms.

*Rejected:* flat SDK keywords such as `keys=` and `rounds=`. They give the two forms different
shapes, and every new section field would need a second name.

A candidate's module is imported with the experiment file's directory on `sys.path`. So a
`pipelines.py` beside the TOML file imports without being installed. A relative local path resolves
against the same directory, or the working directory in the SDK.

## Everything a run could fail on is refused together, before the first Spark call

Building an `Experiment` imports every candidate, resolves every path, binds every table parameter
and checks every overlay and timing field. It raises `Refused` holding every problem found, so one
pass fixes them all. Each problem carries a code, a dotted path into the experiment as written, a
message and an optional hint.

`refusals.CODES` registers each code. A code never changes meaning, and a retired code is never
reused.

A table whose source failed still counts as declared when parameters are bound. So a missing path is
reported once, as a missing path, and not again as an unbound parameter.

## Static confs are read from Spark's registry, because `isModifiable` misreports

`spark.conf.set` refuses a key in one of two cases, which is the test
`RuntimeConfig.requireNonStaticConf` makes:

- `SQLConf.isStaticConfigKey(key)` is true;
- the key is registered in `ConfigEntry.knownConfigs`, and `SQLConf.containsConfigKey(key)` is false.
  These are core confs such as `spark.executor.memory`.

Every other key is accepted. That includes `spark.hadoop.*`, `spark.sql.catalog.<name>`, Delta's
keys and any custom key. `spark.conf.isModifiable` returns false for all of those on Spark 3.5.9 and
4.2.0. A check built on it would refuse overlays Spark runs.

A Spark Connect client has no JVM handle, so no session-start probe reaches the registry there. The
library ships `static_conf_keys.txt` instead. It is the union of the refused keys over every Spark
the fixtures capture, written by `fixtures/regenerate.sh static-conf-keys`. A key that only a newer
Spark registers is not in the list. Spark's own error refuses it when the overlay is applied.

## Too few timing rounds are refused, because the test cannot reach alpha

The speed verdict uses a two-sided exact Mann-Whitney U test. With `n` rounds per arm, its smallest
reachable p-value is `2 / C(2n, n)`. At the default `alpha` of 0.01 that takes 5 rounds. An
experiment whose `timing.rounds` cannot reach `timing.alpha` is refused with `E-EXPERIMENT-002`.
`timing.underpowered = true` lets it run, and the report labels every verdict underpowered.

## An experiment's identity hashes its resolved definition

`Experiment.definition()` is JSON. Local paths in it are absolute, and every section field holds its
value with defaults filled in. Each candidate carries its import path and the SHA-256 of its source
text. `schemas/experiment.json` describes it, and `pysparring schema show experiment` prints that
schema.

`definition_sha256` hashes the definition's JSON with sorted keys and no whitespace. Two runs of one
definition share it, so the store can line up results across experiments, such as before and after
a refactor. An experiment id is its first 12 hex digits and the start time in UTC, as in
`3f9c2a71b0de-20261022T020509Z`.

- Editing a candidate's body changes the identity. Editing a helper it calls does not, because only
  the candidate's own source is hashed.
- A doppeldata table is identified by the SHA-256 of its profile and scenario files, its row count
  and its seed. So reformatting a profile file changes the identity.
- A DataFrame given to the SDK is identified by Spark's `semanticHash()` of its logical plan.
