# UAT protocol conversion: `emr-service-protocol` 1.0.0, 1.x direction → 2.0 direction (same version)

An example of a protocol that must change for 2.0, from the Rwanda UAT upgrade: CCE 2.0 reads the
1.x protocol's step order reversed, and judges same-visit steps late. Use it as it is for a Rwanda UAT
database, or as a model (and `convert-to-2x-direction.py` as the tool) for another 1.x protocol.

| File | What |
|---|---|
| `emr-service-protocol-1.0.0-uat.json` | the protocol as UAT has it (from the UAT dump of 2026-09-29) |
| `emr-service-protocol-1.0.0-2x.json` | the converted protocol (same url, version 1.0.0) |
| `convert-to-2x-direction.py` | the conversion (`convert-to-2x-direction.py <1.x.json> <2.x.json> <version>`) |


## What changes

1. **Direction.** In 1.x each step's `relatedAction` names the **next** step; CCE 2.0 reads
   it as the step a step **waits on**. Every link is moved to the other end and pointed back, keeping
   its relationship and offset. The order of the protocol is the same.
2. **Deadlines as in 1.x.** `tolerance-days` means something else in 2.0:

   | | 1.x (offset `o`, tolerance `t`) | 2.0 |
   |---|---|---|
   | on time until | due + `t` (`o + t` after the predecessor) | the due date (`o`) |
   | OVERDUE from | `o + t` | `o` |
   | MISSED from | `o + 2t` | `o + t` |

   So each **mandatory** step's offset becomes **`o + t`**, and its tolerance stays `t`: 2.0 then gives
   the same on-time, OVERDUE and MISSED dates as 1.x. For vitals-recording and consultation that is
   0 d + 1 d = **1 d**: a step recorded later in the same visit stays on time (with 0 d it would be
   OVERDUE seconds after its predecessor; upstream Matcher's `9f7ae7e` covers equal timestamps only).
   For diagnosis it is 1 d + 1 d = **2 d**. Optional steps get no deadline in 2.0, so their offsets
   decide nothing; their 0 d offsets become 1 d.
3. **Optional steps.** `tolerance-days` is removed from the `could` steps: 2.0 gives optional steps no
   deadline (Matcher V4), and the 2.0 Protocol API rejects `tolerance-days` on them.
4. The **version stays 1.0.0**: the stored row is updated in place, so every enrolment keeps pointing at
   it. Triggers, codes and everything else are unchanged.

| action | required | tolerance (1.x → 2.0 file) | relatedAction in the 1.x file (names the next step) | relatedAction in the 2.0 file (names the step it waits on) |
|---|---|---|---|---|
| `visit-encounter` | could | — → — | vitals-recording after-start +0d | — |
| `vitals-recording` | must | 1 → 1 | consultation after-end +0d | visit-encounter after-start +1d (0 d + tolerance 1 d) |
| `consultation` | must | 1 → 1 | chief-complaints after-end +0d | vitals-recording after-end +1d (0 d + tolerance 1 d) |
| `chief-complaints` | could | 1 → — | history-assessment after-end +0d | consultation after-end +1d |
| `history-assessment` | could | 1 → — | lab-order after-end +0d | chief-complaints after-end +1d |
| `lab-order` | could | 1 → — | lab-results after-end +3d | history-assessment after-end +1d |
| `lab-results` | could | 5 → — | diagnosis after-end +1d | lab-order after-end +3d |
| `diagnosis` | must | 1 → 1 | treatment after-end +0d | lab-results after-end +2d (1 d + tolerance 1 d) |
| `treatment` | could | 1 → — | referral after-end +0d | diagnosis after-end +1d |
| `referral` | could | 1 → — | — | treatment after-end +1d |

## For review

- **`diagnosis` (must) waits on `lab-results` (could)**, as in the 1.x chain. In 2.0 a mandatory step is
  created ahead of time (so it can be judged OVERDUE / MISSED) only when its predecessor completes; in a
  visit without a lab, `diagnosis` appears only when its own event arrives, with its due date set to
  that moment (`createInitialStep`, not fixed upstream). If `diagnosis` should wait on `consultation`
  (must), as the Kenya protocol's diagnosis waits on vitals, change that one link.

## How it is applied

Optional, and only by a 2.0 service's view of the data: 1.x compliance reads `relatedAction` the 1.x way
and would get the order reversed. Both upgrade scripts apply it at the right moment when the settings
list it:

```bash
PROTOCOL_UPDATES="$SCRIPT_DIR/protocol-conversion/emr-service-protocol-1.0.0-2x.json"   # in replay/*.env
PROTOCOL_API_URL=http://<protocol service>:<port>     # Protocol's API, as seen from where the script runs
```

- **replay upgrade** (`upgrade-1x-to-2x/replay/replay-inbound-events.sh`, runbook part U): `prepare` (A4)
  replaces the stored definition once the protocol rows are back, before any event is sent, starts
  Protocol for it if needed, and has Protocol rebuild the trigger index
  (`POST /v1/protocol/protocol-definitions/{id}/rebuild-index`);
- **database migration** (`upgrade-1x-to-2x/data-migration/migrate-1x-to-2x.sh`): `update-protocol` after the 1.x
  services are stopped and before `migrate`, then `rebuild-index` once Protocol runs.

Both refuse a file whose url and version are not stored. With `PROTOCOL_UPDATES` empty they do nothing.
