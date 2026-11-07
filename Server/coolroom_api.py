#!/usr/bin/env python3
"""
coolroom_api.py — read-only HTTP API over the Node-RED coolroom CSV files.

Reads  : <DATA_DIR>/coolroom_YYYY-MM-DD.csv
Writes : nothing, ever. No MQTT, no third-party packages.

Endpoints
  GET /api/days                                  -> ["2026-11-05", ...]
  GET /api/latest                                -> newest reading
  GET /api/readings?from=<ms>&to=<ms>&max=<n>    -> readings in a time range
  GET /                                          -> index.html from this folder

Config via environment variables:
  COOLROOM_DATA  folder holding the CSVs   (default /var/snap/node-red/common/coolroom)
  COOLROOM_PORT  port to listen on         (default 8080)
  COOLROOM_TZ    hours ahead of UTC in the CSV timestamps (default 7 = Bangkok)
"""

import csv, json, os, re, sys, threading
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

DATA_DIR = os.environ.get("COOLROOM_DATA", "/var/snap/node-red/common/coolroom")
PORT     = int(os.environ.get("COOLROOM_PORT", "8080"))
TZ       = timezone(timedelta(hours=float(os.environ.get("COOLROOM_TZ", "7"))))
HERE     = os.path.dirname(os.path.abspath(__file__))

FIELDS   = ["T_in", "H_in", "V_in", "T_out", "H_out", "V_out"]
FILE_RE  = re.compile(r"^coolroom_(\d{4}-\d{2}-\d{2})\.csv$")

_cache: dict[str, tuple] = {}        # day -> (mtime, size, rows)
_lock  = threading.Lock()


def list_days():
    """Dates we have a file for, oldest first."""
    try:
        names = os.listdir(DATA_DIR)
    except OSError:
        return []
    days = [m.group(1) for m in (FILE_RE.match(n) for n in names) if m]
    return sorted(days)


def _parse_file(path, day):
    """Parse one day's CSV into [{t: epoch_ms, T_in: float|None, ...}]."""
    rows = []
    with open(path, newline="", encoding="utf-8-sig", errors="replace") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return rows
        # first two columns are date and time whatever they are named (they are Thai);
        # map the rest by name so a column reorder in Node-RED cannot break us.
        idx = {}
        for i, name in enumerate(header[2:], start=2):
            key = name.strip()
            if key in FIELDS:
                idx[key] = i
        for raw in reader:
            if len(raw) < 2:
                continue
            d = (raw[0] or day).strip()
            t = (raw[1] or "").strip()
            if not t:
                continue
            try:
                dt = datetime.strptime(f"{d} {t}", "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    dt = datetime.strptime(f"{d} {t}", "%Y-%m-%d %H:%M")
                except ValueError:
                    continue
            rec = {"t": int(dt.replace(tzinfo=TZ).timestamp() * 1000)}
            has_value = False
            for key in FIELDS:
                i = idx.get(key)
                val = None
                if i is not None and i < len(raw):
                    s = raw[i].strip()
                    if s != "":
                        try:
                            val = float(s)
                            has_value = True
                        except ValueError:
                            val = None
                rec[key] = val
            # rows with a timestamp but no readings at all are noise; skip them
            if has_value:
                rows.append(rec)
    rows.sort(key=lambda r: r["t"])
    return rows


def load_day(day):
    """Parsed rows for one day, re-read only when the file changes on disk."""
    path = os.path.join(DATA_DIR, f"coolroom_{day}.csv")
    try:
        st = os.stat(path)
    except OSError:
        return []
    key = (st.st_mtime, st.st_size)
    with _lock:
        hit = _cache.get(day)
        if hit and hit[0] == key:
            return hit[1]
    try:
        rows = _parse_file(path, day)
    except OSError:
        return []
    with _lock:
        _cache[day] = (key, rows)
    return rows


def days_between(from_ms, to_ms):
    """Day strings whose file might hold rows in this range (inclusive, padded)."""
    start = datetime.fromtimestamp(from_ms / 1000, TZ).date()
    end   = datetime.fromtimestamp(to_ms   / 1000, TZ).date()
    out, cur = [], start - timedelta(days=1)
    end = end + timedelta(days=1)
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    have = set(list_days())
    return [d for d in out if d in have]


def thin(rows, n):
    if n <= 0 or len(rows) <= n:
        return rows
    step = (len(rows) - 1) / (n - 1)
    return [rows[round(i * step)] for i in range(n)]


def readings(from_ms, to_ms, max_points):
    out = []
    for day in days_between(from_ms, to_ms):
        for r in load_day(day):
            if from_ms <= r["t"] <= to_ms:
                out.append(r)
    out.sort(key=lambda r: r["t"])
    total = len(out)
    return thin(out, max_points), total


def latest():
    for day in reversed(list_days()):
        rows = load_day(day)
        if rows:
            return rows[-1]
    return None


class Handler(BaseHTTPRequestHandler):
    server_version = "coolroom/1.0"

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, obj, code=200):
        self._send(code, json.dumps(obj, separators=(",", ":")))

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        path = u.path

        try:
            if path == "/api/days":
                return self._json({"days": list_days(), "tz_offset_hours": TZ.utcoffset(None).total_seconds() / 3600})

            if path == "/api/latest":
                r = latest()
                return self._json({"reading": r, "server_time": int(datetime.now(timezone.utc).timestamp() * 1000)})

            if path == "/api/readings":
                now = int(datetime.now(timezone.utc).timestamp() * 1000)
                try:
                    to_ms = int(float(q.get("to", [now])[0]))
                    frm   = int(float(q.get("from", [to_ms - 3600_000])[0]))
                    mx    = int(float(q.get("max", [2000])[0]))
                except ValueError:
                    return self._json({"error": "from, to and max must be numbers"}, 400)
                if frm > to_ms:
                    frm, to_ms = to_ms, frm
                mx = max(10, min(mx, 20000))
                rows, total = readings(frm, to_ms, mx)
                return self._json({
                    "from": frm, "to": to_ms,
                    "count": len(rows), "total_in_range": total,
                    "fields": FIELDS, "readings": rows,
                    "server_time": now,
                })

            if path in ("/", "/index.html"):
                f = os.path.join(HERE, "index.html")
                if not os.path.exists(f):
                    return self._send(404, "index.html not found next to coolroom_api.py", "text/plain; charset=utf-8")
                with open(f, "rb") as fh:
                    return self._send(200, fh.read(), "text/html; charset=utf-8")

            return self._send(404, "not found", "text/plain; charset=utf-8")

        except Exception as e:                       # never take the server down
            sys.stderr.write(f"error handling {path}: {e!r}\n")
            return self._json({"error": "internal error"}, 500)

    do_HEAD = do_GET

    def log_message(self, fmt, *args):               # one tidy line per request
        sys.stderr.write("%s %s\n" % (self.log_date_time_string(), fmt % args))


def main():
    if not os.path.isdir(DATA_DIR):
        sys.stderr.write(f"data folder not found: {DATA_DIR}\n")
        sys.exit(1)
    days = list_days()
    print(f"coolroom api  data={DATA_DIR}  days={len(days)}"
          f"{' (' + days[0] + ' to ' + days[-1] + ')' if days else ''}  port={PORT}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
