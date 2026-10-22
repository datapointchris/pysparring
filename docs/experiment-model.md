# The experiment model

## An experiment is four declared dimensions, expanded before any Spark call

An experiment declares candidates, datasets, confs and clusters. Nothing is discovered.

- **A candidate is a transform function.** Its parameters without a default name the tables it
  receives, the way pytest injects fixtures by name. A keyword parameter with a default is not a
  table, and `kwargs` binds it. So one function with two `kwargs` values is two candidates, which is
  asv's `params`.
- **A dataset maps table names to sources.** A source is a local or remote path, a catalog table, a
  doppeldata spec, or in the SDK a DataFrame.
- **A conf is a named overlay of runtime SQL confs.** Overlays are set with `spark.conf.set` on a
  running session.
- **A cluster is compute fixed when it is provisioned.** That includes `static_conf`, the confs a
  session takes only when it starts.

A cell is one (dataset, conf, cluster), and its id reads `dataset/conf@cluster`. An arm is one
candidate inside a cell, and arms are compared within a cell. `baseline.cell` names the cell the
others are ranked against, and defaults to the first. A name may not hold `/`, `@` or `.`, so a cell
id and a problem's path each read one way.

## The TOML file and the SDK object take the same sections

`Experiment.from_toml` passes each TOML section to `Experiment` as the keyword of the same name.
`compare` and `timing` are tables in TOML and `Compare` and `Timing` in the SDK, or plain mappings
in either. So a field is named and placed the same way in both forms.

*Rejected:* flat SDK keywords such as `keys=` and `rounds=`. They give the two forms different
shapes, and every new section field would need a second name.

## A candidate's module is read fresh from the experiment's directory at every build

A candidate written `module:function` whose top-level module lives under the experiment's root is
imported again at every build. Any copy already in `sys.modules` is dropped first, and the root is on
`sys.path` only while the import runs. The root is the experiment file's directory, or the working
directory in the SDK. A module that is not under the root imports as usual.

So a module beside the experiment file imports without being installed. An edit made since the last
build in the same process is what binds. Two experiments whose directories each hold a
`pipelines.py` each bind their own. A `json.py` beside one shadows nothing once the build returns.

## Everything a run could fail on is refused together, before the first Spark call

Building an `Experiment` imports every candidate, resolves every path, binds every table parameter
and checks every overlay and timing field. It raises `Refused` holding every problem found, so one
pass fixes them all. A candidate module that raises at import, even `SystemExit`, is a problem, as
is a file that cannot be read.

Each problem carries a code, a dotted path into the experiment as written, a message, an optional
hint, and `valid`, the values the field accepts where they are a closed set. `refusals.CODES`
registers each code. A code never changes meaning, and a retired code is never reused.

Names that failed still count as declared. A table whose source failed still binds, and a candidate
that failed to import can still be the compare reference. So one fault is reported once.

## Static confs are read from Spark's registry, because `isModifiable` misreports

`spark.conf.set` refuses a key in one of two cases, which is the test
`RuntimeConfig.requireNonStaticConf` makes:

- `SQLConf.isStaticConfigKey(key)` is true;
- the key is registered in `ConfigEntry.knownConfigs`, and `SQLConf.containsConfigKey(key)` is false.
  These are core confs such as `spark.executor.memory`.

Every other key is accepted. That includes `spark.hadoop.*`, `spark.sql.catalog.<name>`, Delta's
keys and any custom key. `spark.conf.isModifiable` returns true only for a registered, non-static
SQL conf. So it returns false for every key Spark never registered, and a check built on it would
refuse overlays Spark runs.

A Spark Connect client has no JVM handle, so no session-start probe reaches the registry there. The
library ships `static_conf_keys.txt` instead. It is the union of the refused keys over every Spark
the fixtures capture, written by `fixtures/regenerate.sh static-conf-keys`. Spark registers a conf
only when the object declaring it loads, so the capture loads Hive's, YARN's and Kubernetes' config
objects before reading. A key that only a newer Spark registers is not in the list. Spark's own
error refuses it when the overlay is applied.

## Too few timing rounds are refused, because the test cannot reach alpha

The speed verdict uses a two-sided exact Mann-Whitney U test. With `n` rounds per arm, its smallest
reachable p-value is `2 / C(2n, n)`, which `speed_verdict.smallest_reachable_p` computes. An
experiment whose `timing.rounds` cannot reach `timing.alpha` is refused with `E-EXPERIMENT-002`,
and the refusal names the fewest rounds that can. `timing.underpowered = true` lets it run.

## An experiment's identity hashes the definition as written, and fingerprints sit beside it

`Experiment.definition()` is JSON. A local path under the experiment's root is relative to it,
each candidate is named by its import path, and every section field holds its value with defaults
filled in. A kwarg or `expect` value must come back unchanged from JSON, so the definition never
holds a value's `repr`. `schemas/experiment.json` describes it, and `pysparring schema show
experiment` prints that schema.

`definition_sha256` hashes the definition's JSON with sorted keys and no whitespace. So one
experiment has one identity in every checkout and on every machine, before and after a refactor of
its candidates. That is what lines up runs of one experiment across commits.

`fingerprints()` records what the code and generated data were, beside the identity rather than in
it. Comparing two runs' fingerprints says whether either changed between them.

- A candidate's fingerprint hashes its own source text. Editing a helper it calls does not change
  it. It is null where Python cannot read the source, as for a function typed into a REPL.
- A doppeldata table's fingerprint is the SHA-256 of its profile and scenario files as written.
- A DataFrame given to the SDK has no fingerprint. Spark's `semanticHash()` differs between
  sessions reading one file, and on Connect it is a call to the server.

An experiment id is the identity's first 12 hex digits, the start time in UTC and 6 random hex
digits, as in `3f9c2a71b0de-20261022T020509Z-a41c07`. Two runs of one experiment started in the
same second get two ids.
