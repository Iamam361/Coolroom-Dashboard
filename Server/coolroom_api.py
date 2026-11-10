#!/usr/bin/env python3
"""
coolroom_api.py — read-only HTTP API over the Node-RED CSV files.

Serves every room listed in ROOMS below. Reads files, never writes them.
No MQTT, no third-party packages: Python standard library only.

Endpoints
  GET /api/rooms                                          -> room list + channel definitions
  GET /api/days?room=<id>                                 -> ["2026-11-05", ...]
  GET /api/latest?room=<id>                               -> newest reading
  GET /api/readings?room=<id>&from=<ms>&to=<ms>&max=<n>   -> readings in a time range
  GET /                                                   -> index.html from this folder

`room` defaults to the first room, so older clients keep working.

Config via environment variables:
  COOLROOM_DATA  folder for the cool room  (default /var/snap/node-red/common/coolroom)
  GAS_DATA       folder for the gas sensor (default /var/snap/node-red/common/Mitr_Phol_Gas_Sensor)
  COOLROOM_PORT  port to listen on         (default 8080)
  COOLROOM_TZ    hours ahead of UTC in the CSV timestamps (default 7 = Bangkok)
"""

import csv, json, os, re, sys, threading
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

PORT = int(os.environ.get("COOLROOM_PORT", "8080"))
TZ   = timezone(timedelta(hours=float(os.environ.get("COOLROOM_TZ", "7"))))
HERE = os.path.dirname(os.path.abspath(__file__))

COOLROOM_DIR = os.environ.get("COOLROOM_DATA", "/var/snap/node-red/common/coolroom")
GAS_DIR      = os.environ.get("GAS_DATA",      "/var/snap/node-red/common/Mitr_Phol_Gas_Sensor")


def ch(key, col, label, unit, dec=2, **kw):
    """One channel: key is what the web page uses, col is the CSV header."""
    d = {"key": key, "col": col, "label": label, "unit": unit, "dec": dec}
    d.update(kw)
    return d


# --------------------------------------------------------------------------
# Rooms. Adding a third sensor means adding one entry here and nothing else.
# --------------------------------------------------------------------------
ROOMS = [
    {
        "id": "coolroom",
        "name": "Cool room",
        "short": "Cool room",
        "dir": COOLROOM_DIR,
        # coolroom_2026-11-09.csv
        # anything after the date is allowed: Node-RED adds suffixes of its own
        # (" (20.30)", "_15") whenever a flow is redeployed
        "re": re.compile(r"^coolroom_(\d{4}-\d{2}-\d{2}).*\.csv$"),
        "channels": [
            ch("T_in",  "T_in",  "Temperature — inlet",  "°C",  1),
            ch("H_in",  "H_in",  "Humidity — inlet",     "%RH",      1),
            ch("V_in",  "V_in",  "Air velocity — inlet", "m/s",      2),
            ch("T_out", "T_out", "Temperature — outlet", "°C",  1),
            ch("H_out", "H_out", "Humidity — outlet",    "%RH",      1),
            ch("V_out", "V_out", "Air velocity — outlet","m/s",      2),
        ],
        "groups": [
            {"id": "T", "title": "Temperature",  "short": "Temp", "unit": "°C", "dec": 1,
             "icon": "i-temp", "cls": "temp", "paired": True,
             "series": [{"k": "T_in", "label": "Inlet"}, {"k": "T_out", "label": "Outlet"}]},
            {"id": "H", "title": "Humidity",     "short": "Hum",  "unit": "%RH", "dec": 1,
             "icon": "i-drop", "cls": "hum", "paired": True,
             "series": [{"k": "H_in", "label": "Inlet"}, {"k": "H_out", "label": "Outlet"}]},
            {"id": "V", "title": "Air velocity", "short": "Vel",  "unit": "m/s", "dec": 2,
             "icon": "i-wind", "cls": "vel", "paired": True,
             "series": [{"k": "V_in", "label": "Inlet"}, {"k": "V_out", "label": "Outlet"}]},
        ],
        # temperature paired with the humidity measured at the same place
        "psychro": [
            {"id": "in",  "name": "Inlet",  "t": "T_in",  "rh": "H_in"},
            {"id": "out", "name": "Outlet", "t": "T_out", "rh": "H_out"},
        ],
        "combo": {
            "axes": [
                {"id": "y1", "title": "Temperature (\u00b0C)", "unit": "\u00b0C", "side": "left"},
                {"id": "y2", "title": "Humidity (%RH)",         "unit": "%RH",     "side": "right"},
                {"id": "y3", "title": "Air velocity (m/s)",     "unit": "m/s",     "side": "right"},
            ],
            "series": [
                {"k": "T_in",  "label": "Temperature \u2014 inlet",  "short": "Temp in",  "axis": "y1"},
                {"k": "T_out", "label": "Temperature \u2014 outlet", "short": "Temp out", "axis": "y1"},
                {"k": "H_in",  "label": "Humidity \u2014 inlet",     "short": "Hum in",   "axis": "y2"},
                {"k": "H_out", "label": "Humidity \u2014 outlet",    "short": "Hum out",  "axis": "y2"},
                {"k": "V_in",  "label": "Air velocity \u2014 inlet", "short": "Vel in",   "axis": "y3"},
                {"k": "V_out", "label": "Air velocity \u2014 outlet","short": "Vel out",  "axis": "y3"},
            ],
        },
    },
    {
        "id": "gas",
        "name": "Mitr Phol gas sensor",
        "short": "Gas sensor",
        "dir": GAS_DIR,
        # Gas_Sensor-2026-11-10.csv, "... (20.30).csv", "..._15.csv" — all of them
        "re": re.compile(r"^Gas_Sensor-(\d{4}-\d{2}-\d{2}).*\.csv$"),
        "channels": [
            ch("valve",   "Valve",               "Valve",                "",      0, meta=True),
            ch("round",   "Round",               "Round",                "",      0, meta=True),
            ch("co",      "CO(ppm)",             "Carbon monoxide",      "ppm",   2),
            ch("ch4l",    "CH4_Laser(%LEL)",     "Methane (laser)",      "%LEL",  2),
            ch("ch4m",    "CH4_MiCS(ppm)",       "Methane (MiCS)",       "ppm",   2),
            ch("o2",      "O2(%Vol)",            "Oxygen",               "%Vol",  2),
            ch("co2",     "CO2(ppm)",            "Carbon dioxide",       "ppm",   2),
            ch("etoh",    "EtOH(ppm)",           "Ethanol",              "ppm",   2),
            ch("voc",     "VOC(ppm)",            "VOC",                  "ppm",   2),
            ch("temp",    "Temp_SHT31(C)",       "Temperature",          "°C", 2),
            ch("rh",      "RH(%)",               "Humidity",             "%",     2),
            ch("flow",    "Flow(m/s)",           "Flow",                 "m/s",   2),
            ch("p1",      "Probe_V1(C)",         "Probe 1",              "°C", 2, diag=True),
            ch("p2",      "Probe_V2(C)",         "Probe 2",              "°C", 2, diag=True),
            ch("p3",      "Probe_V3(C)",         "Probe 3",              "°C", 2, diag=True),
            ch("co_t",    "CO_Temp(C)",          "CO sensor temp",       "°C", 2, diag=True),
            ch("ch4l_t",  "CH4_Laser_Temp(C)",   "CH₄ laser temp",  "°C", 2, diag=True),
            ch("o2_t",    "O2_Temp(C)",          "O₂ sensor temp",  "°C", 2, diag=True),
            ch("etoh_v",  "EtOH_V(V)",           "Ethanol raw",          "V",     2, diag=True),
            ch("flow_v",  "Flow_V(V)",           "Flow raw",             "V",     2, diag=True),
            ch("ch4_err", "CH4_Laser_Err(code)", "CH₄ laser error", "code",  0, diag=True),
        ],
        "groups": [
            {"id": "CO",   "title": "Carbon monoxide", "short": "CO",    "unit": "ppm",  "dec": 2,
             "icon": "i-gas",  "cls": "gas1", "series": [{"k": "co",   "label": "CO"}]},
            {"id": "CH4",  "title": "Methane",         "short": "CH₄", "unit": "ppm", "dec": 2,
             "icon": "i-flame","cls": "gas2", "series": [{"k": "ch4m", "label": "MiCS (ppm)"}]},
            {"id": "CH4L", "title": "Methane, laser",  "short": "CH₄ laser", "unit": "%LEL", "dec": 2,
             "icon": "i-flame","cls": "gas2", "series": [{"k": "ch4l", "label": "Laser"}]},
            {"id": "O2",   "title": "Oxygen",          "short": "O₂", "unit": "%Vol", "dec": 2,
             "icon": "i-gas",  "cls": "gas3", "series": [{"k": "o2",  "label": "O₂"}]},
            {"id": "CO2",  "title": "Carbon dioxide",  "short": "CO₂", "unit": "ppm", "dec": 2,
             "icon": "i-gas",  "cls": "gas4", "series": [{"k": "co2", "label": "CO₂"}]},
            {"id": "ETOH", "title": "Ethanol",         "short": "EtOH",  "unit": "ppm",  "dec": 2,
             "icon": "i-gas",  "cls": "gas5", "series": [{"k": "etoh","label": "Ethanol"}]},
            {"id": "VOC",  "title": "VOC",             "short": "VOC",   "unit": "ppm",  "dec": 2,
             "icon": "i-gas",  "cls": "gas6", "series": [{"k": "voc", "label": "VOC"}]},
            {"id": "TEMP", "title": "Temperature",     "short": "Temp",  "unit": "°C", "dec": 2,
             "icon": "i-temp", "cls": "temp", "series": [{"k": "temp", "label": "SHT31"}]},
            {"id": "RH",   "title": "Humidity",        "short": "Hum",   "unit": "%",    "dec": 2,
             "icon": "i-drop", "cls": "hum",  "series": [{"k": "rh",  "label": "RH"}]},
            {"id": "FLOW", "title": "Flow",            "short": "Flow",  "unit": "m/s",  "dec": 2,
             "icon": "i-wind", "cls": "vel",  "series": [{"k": "flow","label": "Flow"}]},
            {"id": "PROBE", "title": "Probe temperatures", "short": "Probes", "unit": "°C", "dec": 2,
             "icon": "i-temp", "cls": "temp", "diag": True,
             "series": [{"k": "p1", "label": "Probe 1"}, {"k": "p2", "label": "Probe 2"},
                        {"k": "p3", "label": "Probe 3"}]},
            {"id": "STEMP", "title": "Sensor temperatures", "short": "Sensor temp", "unit": "°C", "dec": 2,
             "icon": "i-temp", "cls": "temp", "diag": True,
             "series": [{"k": "co_t", "label": "CO"}, {"k": "ch4l_t", "label": "CH₄ laser"},
                        {"k": "o2_t", "label": "O₂"}]},
            {"id": "RAW", "title": "Raw voltages", "short": "Voltages", "unit": "V", "dec": 2,
             "icon": "i-gas", "cls": "gas6", "diag": True,
             "series": [{"k": "etoh_v", "label": "Ethanol"}, {"k": "flow_v", "label": "Flow"}]},
        ],
        # each round steps the valve through the sampling points, so a round
        # number lets two positions be compared at the same moment in the cycle
        "position": {"key": "valve", "label": "Position", "cycle": "round"},
        "psychro": [
            {"id": "s", "name": "Sample", "t": "temp", "rh": "rh"},
        ],
        "combo": {
            "axes": [
                {"id": "y1", "title": "Gases (ppm)",      "unit": "ppm",   "side": "left"},
                {"id": "y2", "title": "O\u2082 (%Vol)",   "unit": "%Vol",  "side": "right"},
                {"id": "y3", "title": "Temperature (\u00b0C)", "unit": "\u00b0C", "side": "right"},
            ],
            "series": [
                {"k": "co",   "label": "Carbon monoxide", "short": "CO",    "axis": "y1"},
                {"k": "ch4m", "label": "Methane (MiCS)",  "short": "CH\u2084", "axis": "y1"},
                {"k": "co2",  "label": "Carbon dioxide",  "short": "CO\u2082", "axis": "y1"},
                {"k": "etoh", "label": "Ethanol",         "short": "EtOH",  "axis": "y1"},
                {"k": "voc",  "label": "VOC",             "short": "VOC",   "axis": "y1"},
                {"k": "o2",   "label": "Oxygen",          "short": "O\u2082", "axis": "y2"},
                {"k": "temp", "label": "Temperature",     "short": "Temp",  "axis": "y3"},
            ],
        },
    },
]

BY_ID = {r["id"]: r for r in ROOMS}
_cache: dict = {}                     # (room_id, day) -> (stamp, rows)
_lock = threading.Lock()


def pick_room(q):
    rid = (q.get("room", [None])[0] or "").strip()
    return BY_ID.get(rid) or ROOMS[0]


def files_for(room):
    """{day: [paths]} — a day can have several files when Node-RED is redeployed."""
    out = {}
    try:
        names = os.listdir(room["dir"])
    except OSError:
        return out
    for n in names:
        m = room["re"].match(n)
        if m:
            out.setdefault(m.group(1), []).append(os.path.join(room["dir"], n))
    for day in out:
        out[day].sort()
    return out


def list_days(room):
    return sorted(files_for(room).keys())


def _parse_file(path, day, room):
    """One CSV -> [{t: epoch_ms, <key>: float|None, ...}]."""
    rows = []
    chans = room["channels"]
    with open(path, newline="", encoding="utf-8-sig", errors="replace") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return rows
        # Columns are matched by header name, so a reorder in Node-RED cannot
        # silently shift every value into the wrong field. The first two
        # columns are date and time, whatever they are called (they are Thai).
        pos = {}
        for i, name in enumerate(header):
            pos[name.strip()] = i
        idx = {c["key"]: pos.get(c["col"]) for c in chans}

        for raw in reader:
            if len(raw) < 2:
                continue
            d = (raw[0] or day).strip()
            t = (raw[1] or "").strip()
            if not t:
                continue
            dt = None
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                try:
                    dt = datetime.strptime(f"{d} {t}", fmt)
                    break
                except ValueError:
                    pass
            if dt is None:
                continue
            rec = {"t": int(dt.replace(tzinfo=TZ).timestamp() * 1000)}
            has_value = False
            for c in chans:
                i = idx.get(c["key"])
                val = None
                if i is not None and i < len(raw):
                    s = raw[i].strip()
                    if s != "":
                        try:
                            val = float(s)
                            if not c.get("meta"):
                                has_value = True
                        except ValueError:
                            # "No device" and friends: the channel is simply absent
                            val = None
                rec[c["key"]] = val
            if has_value:
                rows.append(rec)
    return rows


def load_day(room, day):
    """Merged rows for one day, re-read only when a file changes on disk."""
    paths = files_for(room).get(day, [])
    if not paths:
        return []
    stamp = []
    for p in paths:
        try:
            st = os.stat(p)
            stamp.append((p, st.st_mtime, st.st_size))
        except OSError:
            pass
    stamp = tuple(stamp)
    ck = (room["id"], day)
    with _lock:
        hit = _cache.get(ck)
        if hit and hit[0] == stamp:
            return hit[1]

    merged = {}
    for p in paths:
        try:
            for r in _parse_file(p, day, room):
                # later files win for the same instant: they are the newer run
                merged[r["t"]] = r
        except OSError:
            continue
    rows = [merged[k] for k in sorted(merged)]
    with _lock:
        _cache[ck] = (stamp, rows)
    return rows


def days_between(room, from_ms, to_ms):
    start = datetime.fromtimestamp(from_ms / 1000, TZ).date() - timedelta(days=1)
    end   = datetime.fromtimestamp(to_ms   / 1000, TZ).date() + timedelta(days=1)
    have, out, cur = set(list_days(room)), [], start
    while cur <= end:
        s = cur.isoformat()
        if s in have:
            out.append(s)
        cur += timedelta(days=1)
    return out


def thin(rows, n):
    if n <= 0 or len(rows) <= n:
        return rows
    if n == 1:
        return rows[:1]
    step = (len(rows) - 1) / (n - 1)
    return [rows[round(i * step)] for i in range(n)]


def thin_by_position(rows, n, pos_key):
    """Thin without destroying the valve mix.

    The valves take turns, so the rows arrive as 1,2,3,1,2,3,... A plain
    uniform stride lands on the same valve over and over: at a stride of 3
    it keeps one valve and throws the other two away completely. So each
    valve is thinned on its own and the results merged back in time order.
    """
    if n <= 0 or len(rows) <= n or not pos_key:
        return thin(rows, n)

    groups = {}
    for r in rows:
        groups.setdefault(r.get(pos_key), []).append(r)
    if len(groups) < 2:
        return thin(rows, n)

    share = max(1, n // len(groups))
    out = []
    for g in groups.values():
        out.extend(thin(g, share))
    out.sort(key=lambda r: r["t"])
    return out


def readings(room, from_ms, to_ms, max_points):
    out = []
    for day in days_between(room, from_ms, to_ms):
        for r in load_day(room, day):
            if from_ms <= r["t"] <= to_ms:
                out.append(r)
    out.sort(key=lambda r: r["t"])
    pos = (room.get("position") or {}).get("key")
    return thin_by_position(out, max_points, pos), len(out)


def latest(room):
    for day in reversed(list_days(room)):
        rows = load_day(room, day)
        if rows:
            return rows[-1]
    return None


def room_public(room):
    """What the web page needs to build itself. 'col' and 'dir' stay private."""
    return {
        "id": room["id"],
        "name": room["name"],
        "short": room["short"],
        "available": os.path.isdir(room["dir"]),
        "channels": [{k: v for k, v in c.items() if k != "col"} for c in room["channels"]],
        "groups": room["groups"],
        "combo": room.get("combo"),
        "position": room.get("position"),
        "psychro": room.get("psychro"),
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "rooms/2.0"

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
            if path == "/api/rooms":
                return self._json({
                    "rooms": [room_public(r) for r in ROOMS],
                    "tz_offset_hours": TZ.utcoffset(None).total_seconds() / 3600,
                })

            if path == "/api/days":
                room = pick_room(q)
                return self._json({
                    "room": room["id"], "days": list_days(room),
                    "tz_offset_hours": TZ.utcoffset(None).total_seconds() / 3600,
                })

            if path == "/api/latest":
                room = pick_room(q)
                return self._json({
                    "room": room["id"], "reading": latest(room),
                    "server_time": int(datetime.now(timezone.utc).timestamp() * 1000),
                })

            if path == "/api/readings":
                room = pick_room(q)
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
                rows, total = readings(room, frm, to_ms, mx)
                return self._json({
                    "room": room["id"], "from": frm, "to": to_ms,
                    "count": len(rows), "total_in_range": total,
                    "fields": [c["key"] for c in room["channels"]],
                    "readings": rows, "server_time": now,
                })

            if path in ("/", "/index.html"):
                f = os.path.join(HERE, "index.html")
                if not os.path.exists(f):
                    return self._send(404, "index.html not found next to coolroom_api.py",
                                      "text/plain; charset=utf-8")
                with open(f, "rb") as fh:
                    return self._send(200, fh.read(), "text/html; charset=utf-8")

            return self._send(404, "not found", "text/plain; charset=utf-8")

        except Exception as e:                       # never take the server down
            sys.stderr.write(f"error handling {path}: {e!r}\n")
            return self._json({"error": "internal error"}, 500)

    do_HEAD = do_GET

    def log_message(self, fmt, *args):
        sys.stderr.write("%s %s\n" % (self.log_date_time_string(), fmt % args))


def main():
    live = [r for r in ROOMS if os.path.isdir(r["dir"])]
    if not live:
        sys.stderr.write("none of the data folders exist:\n" +
                         "".join(f"  {r['dir']}\n" for r in ROOMS))
        sys.exit(1)
    for r in ROOMS:
        if os.path.isdir(r["dir"]):
            d = list_days(r)
            print(f"  {r['id']:9s} {r['dir']}  days={len(d)}"
                  f"{' (' + d[0] + ' to ' + d[-1] + ')' if d else ''}", flush=True)
        else:
            print(f"  {r['id']:9s} {r['dir']}  MISSING - room hidden", flush=True)
    print(f"rooms api listening on port {PORT}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
