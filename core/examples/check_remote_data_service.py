# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Check a remote data service against doc/remote-data-integration-contract.md,
over plain HTTP. Driven by scripts/check-remote-data-service.sh.

    python3 core/examples/check_remote_data_service.py <base-url>

The service is a black box: this speaks HTTP/1.1 to it with the standard
library only -- no httpx, no p-SWAMP imports, no knowledge of how the service
is built -- so the same check runs against the stub in this repo and against a
deployment's own service, whatever it is written in. It reads each query's
response line by line off the socket, exactly as the Remote Data Client does.

What it checks is the contract's acceptance list, as far as a client can see
it: coverage and its exclusive end, a bounded query's records, framing and
terminal line, the half-open window, the ``mrid`` filter, refusals by status
code before any body, a streamed (not pre-sized) body, two queries answered
independently on two connections, and a service that still answers after a
client hung up mid-query. What it cannot see from outside is whether the
service stopped reading its store when the client went away; that is the
service owner's to verify on their side.

It only ever asks for small windows (a couple of seconds at the start of the
coverage), except the hang-up case, which asks for everything and reads three
lines. Every step runs; the exit status is non-zero if any failed.
"""

from __future__ import annotations

import http.client
import json
import sys
import urllib.parse
from datetime import datetime, timedelta, timezone

MODEL = "pmu.frame"
TIMEOUT_S = 15.0
KINDS = {"record", "end", "error"}

failures: list[str] = []


def ok(label: str) -> None:
    print(f"    \033[32m✓\033[0m {label}")


def bad(label: str) -> None:
    print(f"    \033[31m✗\033[0m {label}")
    failures.append(label)


def check(condition: bool, label: str) -> bool:
    (ok if condition else bad)(label)
    return condition


def section(title: str) -> None:
    print(f"\n  {title}")


# --- HTTP, by hand ---------------------------------------------------------------


class Service:
    def __init__(self, base_url: str) -> None:
        parts = urllib.parse.urlsplit(base_url.rstrip("/"))
        if parts.scheme not in ("http", "https"):
            raise SystemExit(f"not an http(s) URL: {base_url}")
        self.scheme = parts.scheme
        self.host = parts.hostname or "localhost"
        self.port = parts.port
        self.prefix = parts.path

    def connection(self) -> http.client.HTTPConnection:
        cls = http.client.HTTPSConnection if self.scheme == "https" else http.client.HTTPConnection
        return cls(self.host, self.port, timeout=TIMEOUT_S)

    def get_json(self, path: str) -> tuple[int, object]:
        conn = self.connection()
        try:
            conn.request("GET", self.prefix + path, headers={"Accept": "application/json"})
            response = conn.getresponse()
            body = response.read()
            try:
                return response.status, json.loads(body) if body else None
            except ValueError:
                return response.status, None
        finally:
            conn.close()

    def open_query(self, body: dict | bytes) -> tuple[http.client.HTTPConnection, http.client.HTTPResponse]:
        """POST a query and return the response unread, for line-by-line reading."""
        payload = body if isinstance(body, bytes) else json.dumps(body).encode()
        conn = self.connection()
        conn.request(
            "POST",
            self.prefix + "/v1/queries",
            body=payload,
            headers={"Content-Type": "application/json", "Accept": "application/x-ndjson"},
        )
        return conn, conn.getresponse()


def next_line(response: http.client.HTTPResponse) -> dict | None:
    """The next non-blank line as JSON; ``None`` at the end of the body."""
    while True:
        raw = response.readline()
        if not raw:
            return None
        text = raw.decode("utf-8").strip()
        if text:
            return json.loads(text)


class Answer:
    """One query's response, read to its end and checked for framing."""

    def __init__(self, status: int, headers: dict[str, str], lines: list[dict], problems: list[str]):
        self.status = status
        self.headers = headers
        self.lines = lines
        self.problems = problems

    @property
    def records(self) -> list[dict]:
        return [line["record"] for line in self.lines if line.get("kind") == "record"]

    @property
    def terminal(self) -> dict | None:
        last = self.lines[-1] if self.lines else None
        return last if last is not None and last.get("kind") in ("end", "error") else None


def read_answer(response: http.client.HTTPResponse) -> Answer:
    headers = {k.lower(): v for k, v in response.getheaders()}
    lines: list[dict] = []
    problems: list[str] = []
    try:
        while (line := next_line(response)) is not None:
            if lines and lines[-1].get("kind") in ("end", "error"):
                problems.append("a line follows the terminal line")
            lines.append(line)
    except ValueError as error:
        problems.append(f"a line is not JSON: {error}")
    return Answer(response.status, headers, lines, problems)


def query(service: Service, body: dict) -> Answer:
    conn, response = service.open_query(body)
    try:
        return read_answer(response)
    finally:
        conn.close()


def instant(value: object) -> datetime | None:
    """An ISO 8601 instant *with* a UTC offset, or ``None``."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


# --- the checks --------------------------------------------------------------------


def check_answer_framing(answer: Answer, what: str) -> None:
    """The rules every successful answer keeps, whatever it holds."""
    check(answer.status == 200, f"{what}: 200 (got {answer.status})")
    content_type = answer.headers.get("content-type", "")
    check(
        content_type.startswith("application/x-ndjson"),
        f"{what}: Content-Type application/x-ndjson (got {content_type or 'none'})",
    )
    check(
        "content-length" not in answer.headers,
        f"{what}: streamed, not pre-sized (Content-Length "
        f"{answer.headers.get('content-length', 'absent')}, "
        f"Transfer-Encoding {answer.headers.get('transfer-encoding', 'absent')})",
    )
    check(not answer.problems, f"{what}: every line is JSON and nothing follows the terminal line"
          + (f" ({'; '.join(answer.problems)})" if answer.problems else ""))
    kinds = [line.get("kind") for line in answer.lines]
    check(all(kind in KINDS for kind in kinds), f"{what}: every line has a known kind")
    terminal = answer.terminal
    check(terminal is not None and kinds.count("end") + kinds.count("error") == 1,
          f"{what}: exactly one terminal line, and it is the last")
    if terminal is not None and terminal.get("kind") == "end":
        check(terminal.get("count") == len(answer.records),
              f"{what}: end count {terminal.get('count')} matches {len(answer.records)} record line(s)")
    elif terminal is not None:
        bad(f"{what}: ended in an error line: {terminal.get('error')!r}")
    check(all(line.get("model") == MODEL for line in answer.lines if line.get("kind") == "record"),
          f"{what}: every record line names model {MODEL!r}")


def check_records(records: list[dict], start: datetime, end: datetime, what: str) -> None:
    stamps = [instant(r.get("timestamp")) for r in records]
    check(all(s is not None for s in stamps), f"{what}: every timestamp is ISO 8601 with a UTC offset")
    good = [s for s in stamps if s is not None]
    check(good == sorted(good), f"{what}: records in ascending timestamp order")
    check(all(start <= s < end for s in good), f"{what}: every record inside [start, end)")
    widths_ok = True
    for record in records:
        header = record.get("header") or {}
        columns = [header.get(k) for k in ("station", "channel", "measurement", "units")]
        values = record.get("values")
        if not all(isinstance(c, list) for c in columns) or not isinstance(values, list):
            widths_ok = False
            break
        if len({len(c) for c in columns}) != 1 or len(values) != len(columns[0]):
            widths_ok = False
            break
    check(widths_ok, f"{what}: every frame's values match its own header's width")
    check(all(isinstance(r.get("mRID"), str) and r.get("mRID") for r in records),
          f"{what}: every record names its stream (mRID)")


def run(base_url: str) -> int:
    service = Service(base_url)
    print(f"  checking {base_url} against the remote data contract")

    section("Health and coverage")
    status, _ = service.get_json("/healthz")
    check(status == 200, f"GET /healthz -> 200 (got {status})")
    status, coverage = service.get_json(f"/v1/coverage?model={MODEL}")
    if not check(status == 200 and isinstance(coverage, dict), f"GET /v1/coverage?model={MODEL} -> 200 JSON"):
        return 1
    start, end = instant(coverage.get("start")), instant(coverage.get("end"))
    if not check(start is not None and end is not None and end > start,
                 f"coverage [{coverage.get('start')}, {coverage.get('end')}) is two instants with offsets, end after start"):
        return 1
    status, _ = service.get_json("/v1/coverage?model=no.such.model")
    check(status == 404, f"coverage of an unknown model -> 404 (got {status})")

    section("A bounded query")
    window_end = min(start + timedelta(seconds=1), end)
    answer = query(service, {"version": "v1", "query_id": "check-bounded", "model": MODEL,
                             "start": iso(start), "end": iso(window_end)})
    check_answer_framing(answer, "first second")
    records = answer.records
    if not check(bool(records), f"first second: at least one record (got {len(records)})"):
        return 1
    check_records(records, start, window_end, "first second")
    first = instant(records[0]["timestamp"])
    check(first == start, f"coverage start {iso(start)} is the first record's timestamp")

    section("Coverage end is exclusive")
    tail = query(service, {"version": "v1", "query_id": "check-tail", "model": MODEL,
                           "start": iso(max(start, end - timedelta(seconds=1))), "end": None})
    check_answer_framing(tail, "last second")
    last = instant(tail.records[-1]["timestamp"]) if tail.records else None
    check(last is not None and last < end, f"the last record ({last and iso(last)}) lies before the coverage end")

    section("Half-open window")
    if len(records) >= 2:
        second = instant(records[1]["timestamp"])
        one = query(service, {"version": "v1", "query_id": "check-half-open", "model": MODEL,
                              "start": iso(first), "end": iso(second)})
        check_answer_framing(one, "[first, second)")
        check([instant(r["timestamp"]) for r in one.records] == [first],
              f"[first, second) returns exactly the first record (got {len(one.records)})")
    else:
        bad("half-open window: the first second holds fewer than two records to test with")

    section("mrid filter")
    stream_id = records[0].get("mRID")
    mine = query(service, {"version": "v1", "query_id": "check-mrid", "model": MODEL,
                           "start": iso(start), "end": iso(window_end), "mrid": [stream_id]})
    check_answer_framing(mine, f"mrid [{stream_id}]")
    check(bool(mine.records) and all(r.get("mRID") == stream_id for r in mine.records),
          f"mrid [{stream_id}] returns only that stream ({len(mine.records)} record(s))")
    nobody = query(service, {"version": "v1", "query_id": "check-mrid-none", "model": MODEL,
                             "start": iso(start), "end": iso(window_end), "mrid": ["no-such-stream"]})
    check_answer_framing(nobody, "mrid [no-such-stream]")
    check(nobody.records == [], "an mrid nobody has returns no records and a zero count")

    section("Refusals come as a status code, before any body")
    conn, response = service.open_query({"version": "v1", "query_id": "check-unknown", "model": "no.such.model"})
    try:
        body = response.read()
        check(response.status == 404, f"unknown model -> 404 (got {response.status})")
        check(b'"kind"' not in body, "unknown model: no NDJSON lines in the refusal")
    finally:
        conn.close()
    conn, response = service.open_query(b'{"query_id": "check-malformed"')
    try:
        response.read()
        check(response.status in (400, 422), f"malformed body -> 400 or 422 (got {response.status})")
    finally:
        conn.close()

    section("Two queries on two connections, read in turn")
    two_end = min(start + timedelta(seconds=2), end)
    opened = [service.open_query({"version": "v1", "query_id": f"check-concurrent-{i}", "model": MODEL,
                                  "start": iso(start), "end": iso(two_end)}) for i in range(2)]
    try:
        got: list[list[dict]] = [[], []]
        done = [False, False]
        while not all(done):
            for i, (_conn, response) in enumerate(opened):
                if done[i]:
                    continue
                line = next_line(response)
                if line is None or line.get("kind") in ("end", "error"):
                    done[i] = True
                    if line is not None:
                        got[i].append(line)
                else:
                    got[i].append(line)
        counts = [sum(1 for line in lines if line.get("kind") == "record") for lines in got]
        check(counts[0] == counts[1] and counts[0] > 0 and all(
            lines and lines[-1].get("kind") == "end" and lines[-1].get("count") == n
            for lines, n in zip(got, counts)),
            f"both answered in full and independently ({counts[0]} and {counts[1]} record(s))")
        check([line.get("record") for line in got[0]] == [line.get("record") for line in got[1]],
              "both carry the same records")
    finally:
        for conn, _response in opened:
            conn.close()

    section("A client that hangs up mid-query")
    conn, response = service.open_query({"version": "v1", "query_id": "check-hang-up", "model": MODEL,
                                         "start": iso(start), "end": None})
    try:
        taken = [next_line(response) for _ in range(3)]
        check(all(line is not None and line.get("kind") == "record" for line in taken),
              "the first lines of an open-ended query arrive before the client has read the rest")
    finally:
        conn.close()  # the cancel: the connection goes away
    after = query(service, {"version": "v1", "query_id": "check-after-hang-up", "model": MODEL,
                            "start": iso(start), "end": iso(window_end)})
    check(after.status == 200 and after.terminal is not None and len(after.records) == len(records),
          "the service answers the next query in full after the hang-up")
    print("    (not visible from here: whether the service stopped reading its store -- check its own logs)")

    print()
    if failures:
        print(f"  \033[31m{len(failures)} check(s) failed.\033[0m")
        return 1
    print("  \033[32mThe service keeps the contract, as far as a client can see.\033[0m")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    try:
        raise SystemExit(run(sys.argv[1]))
    except (OSError, http.client.HTTPException) as error:
        print(f"    \033[31m✗\033[0m cannot talk to {sys.argv[1]}: {type(error).__name__}: {error}")
        raise SystemExit(1) from None
