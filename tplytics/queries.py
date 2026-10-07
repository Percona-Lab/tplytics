"""Loading query definitions (queries/*.toml) and rendering them for an engine."""
import glob
import os
import re

try:
    import tomllib
except ImportError:  # Python < 3.11
    import tomli as tomllib

from . import params as P

PLACEHOLDER = re.compile(r"\{([a-z][a-z0-9_]*)\}")
DEFAULT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queries")


class Query:
    def __init__(self, path, data):
        self.path = path
        self.id = data["id"]
        self.name = data["name"]
        self.description = data["description"].strip()
        self.complexity = int(data.get("complexity", 1))
        self.tables = data.get("tables", [])
        w = data.get("weight", {})
        self.weight = {"serial": float(w.get("serial", 1)), "concurrent": float(w.get("concurrent", 1))}
        self.params = data.get("params", {})
        self.sql = data.get("sql", {})
        self.unsupported = [u.lower() for u in data.get("unsupported", [])]
        self.notes = data.get("notes", {})
        if not self.sql:
            raise ValueError("%s: no [sql] section" % path)

    def sql_for(self, engine):
        """SQL text for this engine, or None when the engine does not support the query."""
        if engine.name in self.unsupported:
            return None
        for d in engine.dialects:
            if d in self.sql:
                text = self.sql[d]
                if text is False or (isinstance(text, str) and text.strip().upper() == "UNSUPPORTED"):
                    return None
                return text.strip()
        return None

    def supported(self, engine):
        return self.sql_for(engine) is not None

    def render(self, engine, values):
        text = self.sql_for(engine)

        def sub(m):
            k = m.group(1)
            if k not in values:
                raise KeyError("%s: no value for placeholder {%s}" % (self.id, k))
            v = values[k]
            return str(v) if isinstance(v, P.Raw) else engine.literal(v)
        return PLACEHOLDER.sub(sub, text)

    def new_params(self, state, rng):
        return P.generate(self.params, state, rng)


def load(directory=None, only=None, exclude=None):
    directory = directory or DEFAULT_DIR
    files = sorted(glob.glob(os.path.join(directory, "*.toml")))
    if not files:
        raise SystemExit("no query files in %s" % directory)
    out = []
    for f in files:
        with open(f, "rb") as fh:
            q = Query(f, tomllib.load(fh))
        out.append(q)
    ids = [q.id for q in out]
    dup = {i for i in ids if ids.count(i) > 1}
    if dup:
        raise SystemExit("duplicate query ids: %s" % ", ".join(sorted(dup)))
    if only:
        want = [normalize_id(x) for x in only]
        unknown = set(want) - set(ids)
        if unknown:
            raise SystemExit("unknown query id(s): %s" % ", ".join(sorted(unknown)))
        out = [q for q in out if q.id in want]
    if exclude:
        skip = {normalize_id(x) for x in exclude}
        out = [q for q in out if q.id not in skip]
    return out


def normalize_id(x):
    x = x.strip().upper()
    if x.isdigit():
        x = "Q%02d" % int(x)
    elif re.fullmatch(r"Q\d", x):
        x = "Q0" + x[1:]
    return x
