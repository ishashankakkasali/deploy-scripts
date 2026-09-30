# Test runbook: upgrade 1.x → 2.0 by database migration

**Purpose.** Test the other way of upgrading, migrating the 1.x tables in place, on a copy of a 1.x
deployment (for example a laptop with a restored 1.x database), and compare it with the replay. It is
a test procedure, not one for UAT or prod: the recommended upgrade is the replay (`../README.md`).

Everything here uses `migrate-1x-to-2x.sh`, one command per step. It is separate from the replay
(`../replay/`) and does not use it. It works with Docker Compose.

**How the two ways differ:**

| | Replay | Migration (this runbook) |
|---|---|---|
| The 1.x data | tables set aside; every event in `inbound_event_log` processed again by 2.0 | tables kept and **reshaped** by the 2.0 services' own Flyway migrations (Protocol V2, Matcher V2–V7) |
| Flyway baseline of Protocol and Matcher | 0 (build from V1) | **1** (V1 skipped: the tables exist) |
| Enrolments, steps, deviations | recomputed from the events | the 1.x rows, converted (`state` → `step_status` + `sla_status`, …) |
| Events 1.x never processed | processed | not processed |
| History dates | the replay day | kept |

## Files

| File | What |
|---|---|
| `migrate-1x-to-2x.sh` | the steps below |
| `migration-local.env` | example settings; copy and adjust it |
| `docker-compose.baseline-1.yml` | sets `CCE_FLYWAY_BASELINE_VERSION=1` on Protocol and Matcher; the last file in `COMPOSE_FILES` |
| `../replay/protocol-conversion/` | (optional) a protocol converted to the 2.0 `relatedAction` direction, same version: `PROTOCOL_UPDATES` |

State, the backup and the report go to `~/cce-migration/<NAME>/`.

## Before you start

- CCE **1.x running** with Docker Compose (collector, compliance, scheduler, intelligence, Postgres,
  Kafka, Kafka Connect with the Debezium connector, ClickHouse, Insights).
- A compose file that defines the **2.0 services** (Protocol, Matcher, Step SLA, 2.0 Insights) with the
  usual container names, not started yet.
- A checkout of **cce-matcher-service** (for `migration/verify.sql`).
- `docker`, `curl` and `python3` on the machine.
- Your settings file, copied from `migration-local.env`. Check every value, especially `COMPOSE_DIR`,
  `COMPOSE_FILES`, the container names and `PROTOCOL_UPDATES`.

Run everything from the deploy-scripts folder:
```bash
M="bash upgrade-1x-to-2x/data-migration/migrate-1x-to-2x.sh --config <your settings file>"
```

The numbers in the **Checks** below come from the test on a copy of Rwanda UAT (3,733 events, 780
steps). On your data they differ, but the lines to look for are the same.

## M1. Check (changes nothing)
```bash
$M check
```
It checks Postgres, Kafka, Kafka Connect, ClickHouse, the data-pipeline and `verify.sql`, lists the
services, and runs Matcher V2's three refusal checks plus the protocol direction V2 assumes.

**Check:**
- `OK 1.x tables present`;
- checks 1 and 2 `OK`;
- check 3: `OK`, or a `WARN … steps point at an event not in compliance_event_log (V2 stops)`, which M4 handles
  (on the UAT copy: 184 steps);
- check 4 shows each protocol's first step and the step its `relatedAction` names. `visit-encounter ->
  vitals-recording` means the first step names the **next** step: that's the 1.x direction, which 2.0
  reads reversed and V2 doesn't convert. Set `PROTOCOL_UPDATES` to a converted file (M3).

## M2. Save the 1.x numbers, back up
```bash
$M snapshot
$M backup
```
**Check:** `OK before-numbers saved …`, then `OK backup written: <size> …`.

## M3. Stop the 1.x services
```bash
$M stop-1x --yes
```
Compliance and scheduler must be down: two versions writing `step_instance` across the reshape corrupt
it. The collector keeps running; events it accepts wait in the inbound topic for Matcher.

**Check:** `OK removed: cce-compliance-service cce-scheduler-service`.

Then, **optional** (only when `PROTOCOL_UPDATES` is set):
```bash
$M update-protocol --yes
```
It replaces the stored definition with the file's, keeping url and version, so every enrolment keeps
pointing at it. It must run after M3 and before M7: only 2.0 may read the 2.0 `relatedAction`
direction. **Check:** `OK <url> <version>: definition replaced from <file>`. With no
`PROTOCOL_UPDATES` it prints `nothing to do`.

## M4. Resolve Matcher V2's check 3
Only if M1's check 3 warned:
```bash
$M fix-orphans --yes
```
V2's own instruction is to "null those references or restore the events". This nulls the links of
steps whose event is in neither `compliance_event_log` nor `inbound_event_log`. The steps stay completed.

**Check:** the steps listed (count, source, completion dates), then `OK <n> step links … set to NULL`.
`$M check` now shows check 3 `OK`.

On a real deployment this is a data decision for the team, not something to do silently.

## M5. Create 2.0, stopped, with Flyway baseline 1
```bash
$M deploy-2x
```
Stops the 1.x Insights, creates Protocol, Matcher, Step SLA and the 2.0 Insights without starting them,
and stops if Protocol or Matcher don't have baseline 1.

**Check:** all `created`, then `OK created, not started; Protocol and Matcher baseline at 1`.

## M6. Matcher starts where compliance stopped
```bash
$M copy-offsets --yes
```
Matcher's consumer group is new, and a new group reads from the earliest record: it would re-read every
event the topic still holds, which compliance already processed. This gives Matcher compliance's
committed offsets before Matcher first starts.

**Check:** compliance's offsets listed, then `OK cce-matcher-service now starts where
cce-compliance-service stopped`. On a copy restored from a database dump the inbound topic is empty,
so it prints `WARN cce-compliance-service has no committed offsets …` and sets every partition to the
end: right for a copy. On a real deployment, compliance's offsets must be listed.

## M7. Migrate
```bash
$M migrate --yes
```
1. Protocol starts: Flyway baselines at 1 and applies V2. It may then fail its own start-up check (it
   needs a sequence Matcher's V6 sets up): expected, it prints a `WARN` and goes on.
2. Matcher starts: baseline 1, then V2 (the reshape), V3–V7, and runs.
3. Protocol starts again, now cleanly.

**Check:** `OK Protocol migrated (ledger 1, 2)`, `OK Matcher migrated (ledger 1, 2, 3, 4, 5, 6, 7) and
running`, `OK Protocol running`. It is safe to run again: each service is restarted and Flyway has
nothing left to apply.

Then, **optional** (after `update-protocol`):
```bash
$M rebuild-index --yes
```
Protocol rebuilds the trigger index of the replaced definition through its API (`PROTOCOL_API_URL`).
**Check:** `OK <url> <version>: trigger index rebuilt (<before> -> <after> rows)`.

**If it goes wrong:** Matcher V2 names the problem (`Cannot …`) and the step stops. Restore the M2
backup and start again.

## M8. Check the migrated schema
```bash
$M verify-schema
```
Runs `cce-matcher-service/migration/verify.sql`. **Check:** every section's "no rows = OK" holds,
except checks 2 (`step_instance.due_date`, which V2 keeps) and 11 (`idx_step_instance_matched_event`,
which V2 drops): both are out of date with the current migrations. Section 8 counts the deadlines
already past that Step SLA judges in its first sweep: note it.

## M9. Start Step SLA, rebuild ClickHouse
```bash
$M fix-history-ids --yes
$M start --yes
$M rebuild-clickhouse --yes
```
- **`fix-history-ids`:** needed when the 1.x schema was built by Hibernate (`ddl-auto=update`, as on
  UAT). Its two history tables then have **identity** ids, which Matcher V6 keeps, and Step SLA's
  Hibernate check doesn't see identity sequences, so Step SLA won't start (`missing sequence
  [protocol_instance_history_id_seq]`). This gives them ordinary sequence-backed ids, stepping by 50;
  where the ids are already sequence-backed it does nothing.
- **`start`:** starts Step SLA. Its first sweep then raises deviations for the deadlines M8 counted.
- **`rebuild-clickhouse`:** rebuilds ClickHouse from the migrated Postgres with the 2.0 data-pipeline,
  refills the current-state rollups, backfills past days' KPIs (`schema/09`), and starts the 2.0 Insights.

**Check:**
- `fix-history-ids`: `OK history ids are sequence-backed …` or `nothing to do`;
- `start`: `OK Step SLA running`;
- `rebuild-clickhouse`: `publication tables: <n>`, the schema lines without `ERROR`, `connector 200/201`,
  `OK copy settled`, `4 statement(s) sent, 0 failed (rollup refill + past days, schema/09)`, Insights
  `running`.

## M10. Verify and report
```bash
$M verify
$M report                                   # or: $M report --replay-report <a replay report of the same data>
```
**Check `verify`:**
- lag 0 and DLQ 0;
- `steps (CDC check) … — match`;
- ledgers `1, 2` and `1, 2, 3, 4, 5, 6, 7`;
- services `running`;
- `accepted events never processed`: the events 1.x never processed stay unprocessed (the migration
  doesn't process them).

**`report`** writes `~/cce-migration/<NAME>/migration-report-<time>.md`: **1.x (before) | Migration**
for events, tables, enrolments, steps, verdicts and deviations. With `--replay-report` (the `report` of
a replay upgrade of the same data) it adds **Replay | Migration − replay**.

Then open Insights and note tracked patients, compliance rate, and the daily compliance history.

## What to expect against a replay

From the Rwanda UAT copy, both with the converted protocol:

| | Replay | Migration | Why |
|---|---:|---:|---|
| events processed | 3,733 | 3,115 | the migration doesn't process the 618 events 1.x never did |
| enrolments | 109 | 71 | the replay adds those patients and drops 1.x's duplicate enrolments; the migration keeps 1.x's rows |
| steps | 1,011 | 780 | recomputed from events vs 1.x's rows reshaped |
| deviations | 400 | 221 | every deadline judged again by 2.0 vs 1.x's deviations kept plus Step SLA's first sweep |
| optional steps | no verdict | a verdict (V2 derives one from timestamps) | |
| daily compliance history | real past days, recomputed | real past days, 1.x's dates | both with the new `schema/09` |

## Start again

To start again, restore the M2 backup (the whole `ccedb`), then bring the 1.x services back, or
rebuild your copy the way you first made it.
