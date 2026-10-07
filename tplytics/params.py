"""Query parameter generators.

Each query declares its parameters in its TOML file; every execution draws a
new set of values from the current DatabaseState so the benchmark adapts to
the number of warehouses and to the time range of the stored transactions,
and never runs exactly the same statement twice in a row.

Parameter types (``name = { type = "...", ... }``) and the placeholders
they provide in the SQL text:

  warehouse        {name}                         one warehouse id
  warehouse_range  {name_lo} {name_hi} {name_count}
                   fraction = [lo, hi] of all warehouses, min = 1, max = N
  district         {name}                         1 .. districts per warehouse
  item_range       {name_lo} {name_hi}            fraction = [lo, hi] of the item ids
  time_window      {name_start} {name_mid} {name_end} {name_seconds} {name_bucket}
                   fraction = [lo, hi] of the stored time range,
                   anchor = "recent" | "any" | "mixed", min_seconds = 60,
                   buckets = 24 (target number of buckets for {name_bucket})
  int              {name}   range = [lo, hi]
  choice           {name}   values = [...], raw = true inserts the value unquoted
                            (identifiers such as an ORDER BY column)
"""
import datetime as dt

NICE_BUCKETS = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 10800,
                21600, 43200, 86400, 172800, 604800]


class Raw(str):
    """A value inserted into SQL text as is (validated against the query file's list)."""


def _range_in(rng, spec, total, first):
    lo_f, hi_f = spec.get("fraction", [0.1, 0.1])
    count = int(round(total * rng.uniform(lo_f, hi_f)))
    count = max(int(spec.get("min", 1)), count)
    if "max" in spec:
        count = min(int(spec["max"]), count)
    count = max(1, min(total, count))
    lo = first + rng.randint(0, total - count)
    return lo, lo + count - 1, count


def bucket_for(seconds, target):
    want = seconds / max(1, target)
    for b in NICE_BUCKETS:
        if b >= want:
            return b
    return NICE_BUCKETS[-1]


def generate(specs, state, rng):
    """Return {placeholder: value} for one execution."""
    out = {}
    for name, spec in specs.items():
        t = spec["type"]
        if t == "warehouse":
            out[name] = rng.randint(state.w_min, state.w_max)
        elif t == "warehouse_range":
            lo, hi, n = _range_in(rng, spec, state.warehouses, state.w_min)
            out.update({name + "_lo": lo, name + "_hi": hi, name + "_count": n})
        elif t == "district":
            out[name] = rng.randint(1, state.districts)
        elif t == "item_range":
            lo, hi, n = _range_in(rng, spec, state.items, 1)
            out.update({name + "_lo": lo, name + "_hi": hi, name + "_count": n})
        elif t == "time_window":
            out.update(_time_window(name, spec, state, rng))
        elif t == "int":
            a, b = spec["range"]
            out[name] = rng.randint(int(a), int(b))
        elif t == "choice":
            v = rng.choice(spec["values"])
            out[name] = Raw(v) if spec.get("raw") else v
        else:
            raise ValueError("unknown parameter type %r" % t)
    return out


def _time_window(name, spec, state, rng):
    t_min, t_max = state.t_min, state.t_max
    span = state.span_seconds
    lo_f, hi_f = spec.get("fraction", [0.05, 0.25])
    width = span * rng.uniform(lo_f, hi_f)
    width = min(span, max(float(spec.get("min_seconds", 60)), width))
    anchor = spec.get("anchor", "mixed")
    if anchor == "mixed":
        anchor = "recent" if rng.random() < 0.5 else "any"
    if anchor == "recent":
        end = t_max + dt.timedelta(seconds=1)
        start = end - dt.timedelta(seconds=width)
    else:
        offset = rng.uniform(0, max(0.0, span - width))
        start = t_min + dt.timedelta(seconds=offset)
        end = start + dt.timedelta(seconds=width)
    start = start.replace(microsecond=0)
    end = (end + dt.timedelta(seconds=1)).replace(microsecond=0) if end.microsecond else end
    seconds = max(1, int((end - start).total_seconds()))
    mid = start + dt.timedelta(seconds=seconds // 2)
    return {name + "_start": start, name + "_end": end, name + "_mid": mid,
            name + "_seconds": seconds,
            name + "_bucket": bucket_for(seconds, int(spec.get("buckets", 24)))}


def describe(values):
    """Short human readable rendering of a parameter set."""
    parts = []
    for k, v in values.items():
        if isinstance(v, dt.datetime):
            v = v.strftime("%Y-%m-%d %H:%M:%S")
        parts.append("%s=%s" % (k, v))
    return ", ".join(parts)
