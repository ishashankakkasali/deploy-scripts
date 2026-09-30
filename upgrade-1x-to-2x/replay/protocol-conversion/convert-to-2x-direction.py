#!/usr/bin/env python3
"""Convert a 1.x PlanDefinition to the relatedAction direction CCE 2.0 reads.

1.x: each action's relatedAction names the NEXT action ("vitals-recording comes after me").
2.0: each action's relatedAction names the action it WAITS ON ("I come after visit-encounter").
Every link is moved to the other end and pointed back, keeping its relationship and offset.
Also, for 2.0, keeping 1.x's deadlines:
  * a 'must' step's offset becomes offset + its tolerance-days. 1.x counted a step on time until
    due + tolerance (OVERDUE there, MISSED at due + 2 x tolerance); 2.0 is OVERDUE at the due date
    and MISSED at due + tolerance. With the tolerance moved into the offset, both give the same
    dates (e.g. 0 d + 1 d tolerance -> 1 d: a step done later in the same visit stays on time);
  * other steps: offsetDuration 0 d -> 1 d (they get no deadline in 2.0, so this changes nothing
    they are judged on);
  * tolerance-days is removed from 'could' steps: 2.0 gives optional steps no deadline, and the
    2.0 Protocol API rejects tolerance-days on them.
Usage: convert-to-2x-direction.py <1.x.json> <2.x.json> <new version>
"""
import json, sys, copy

src, dst, version = sys.argv[1], sys.argv[2], sys.argv[3]
d = json.load(open(src))
new = copy.deepcopy(d)
TOL = "http://openphc.org/fhir/StructureDefinition/tolerance-days"

def actions(a):
    for x in a:
        yield x
        yield from actions(x.get("action", []))

by_id = {x["id"]: x for x in actions(new["action"])}
original_by_id = {x["id"]: x for x in actions(d["action"])}

def tolerance_days(action):
    return next((e.get("valueInteger") for e in action.get("extension", []) if e.get("url") == TOL), None)
moved = []
for x in list(actions(new["action"])):          # take every 1.x link off its action ...
    for r in x.pop("relatedAction", []):
        moved.append((x["id"], r))
for pred, r in moved:                            # ... and put it on the action it names, pointing back
    succ = r.get("actionId") or r.get("targetId")
    if succ not in by_id:
        sys.exit(f"relatedAction of {pred} names unknown action {succ}")
    nr = dict(r)
    nr.pop("targetId", None); nr["actionId"] = pred
    off = nr.get("offsetDuration")
    tolerance = tolerance_days(original_by_id[succ])
    if off and off.get("code", off.get("unit")) != "d":
        sys.exit(f"relatedAction {pred} -> {succ}: offsetDuration in '{off.get('code', off.get('unit'))}', only days are converted")
    if by_id[succ].get("requiredBehavior") == "must" and tolerance:
        base = dict(off) if off else {"unit": "d", "system": "http://unitsofmeasure.org", "code": "d", "value": 0}
        nr["offsetDuration"] = dict(base, value=base.get("value", 0) + tolerance)
    elif off and off.get("value") == 0:
        nr["offsetDuration"] = dict(off, value=1)
    by_id[succ].setdefault("relatedAction", []).append(nr)
for x in actions(new["action"]):
    if x.get("requiredBehavior") == "could" and x.get("extension"):
        x["extension"] = [e for e in x["extension"] if e.get("url") != TOL]
        if not x["extension"]:
            del x["extension"]
new["version"] = version
json.dump(new, open(dst, "w"), indent=2)
print(f"{len(moved)} links reversed; written {dst}")
