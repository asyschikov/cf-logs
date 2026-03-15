"""CLI to query Cloudflare Workers Observability logs."""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Literal

import click
import httpx
from dotenv import load_dotenv
from pydantic import BaseModel, Field

load_dotenv()

API_BASE = "https://api.cloudflare.com/client/v4/accounts"


# ── Pydantic models ─────────────────────────────────────────────────────────────


class FilterOperation(str, Enum):
    eq = "eq"
    neq = "neq"
    gt = "gt"
    gte = "gte"
    lt = "lt"
    lte = "lte"
    includes = "includes"
    not_includes = "not_includes"
    starts_with = "starts_with"
    regex = "regex"
    exists = "exists"
    is_null = "is_null"
    in_ = "in"
    not_in = "not_in"


class CalculationOperator(str, Enum):
    count = "count"
    uniq = "uniq"
    min = "min"
    max = "max"
    sum = "sum"
    avg = "avg"
    median = "median"
    p001 = "p001"
    p01 = "p01"
    p05 = "p05"
    p10 = "p10"
    p25 = "p25"
    p75 = "p75"
    p90 = "p90"
    p95 = "p95"
    p99 = "p99"
    p999 = "p999"
    stddev = "stddev"
    variance = "variance"


class FieldType(str, Enum):
    string = "string"
    number = "number"
    boolean = "boolean"


class ViewType(str, Enum):
    events = "events"
    calculations = "calculations"
    invocations = "invocations"


class ApiModel(BaseModel):
    """Base for request bodies — always serializes with by_alias and exclude_none."""
    def model_dump(self, **kwargs):
        kwargs.setdefault("by_alias", True)
        kwargs.setdefault("exclude_none", True)
        return super().model_dump(**kwargs)


class Filter(BaseModel):
    key: str
    operation: FilterOperation
    type: FieldType = FieldType.string
    value: str | float | bool | None = None


class Needle(BaseModel):
    value: str | float | bool
    isRegex: bool = False
    matchCase: bool = False


class Calculation(BaseModel):
    operator: CalculationOperator
    key: str | None = None
    alias: str | None = None


class GroupBy(BaseModel):
    type: FieldType = FieldType.string
    value: str


class Having(BaseModel):
    key: str
    operation: Literal["eq", "gt", "gte", "lt", "lte"]
    value: float


class OrderBy(BaseModel):
    value: str
    order: Literal["asc", "desc"] = "desc"


class QueryParameters(BaseModel):
    filterCombination: Literal["and", "or"] = "and"
    filters: list[Filter] | None = None
    needle: Needle | None = None
    calculations: list[Calculation] | None = None
    groupBys: list[GroupBy] | None = None
    havings: list[Having] | None = None
    orderBy: OrderBy | None = None
    limit: int | None = None


class Timeframe(BaseModel):
    model_config = {"populate_by_name": True}
    from_: int = Field(alias="from", serialization_alias="from")
    to: int


class QueryBody(ApiModel):
    queryId: str
    view: ViewType = ViewType.events
    timeframe: Timeframe
    parameters: QueryParameters
    limit: int | None = None
    granularity: int | None = None
    offset: str | None = None
    offsetBy: int | None = None
    offsetDirection: Literal["next", "prev"] | None = None
    dry: bool | None = None


class KeysRequestBody(ApiModel):
    timeframe: Timeframe
    filters: list[Filter] | None = None
    needle: Needle | None = None
    keyNeedle: Needle | None = None
    limit: int | None = None


class ValuesRequestBody(ApiModel):
    timeframe: Timeframe
    key: str
    type: FieldType = FieldType.string
    filters: list[Filter] | None = None
    needle: Needle | None = None
    limit: int | None = None


class KeyInfo(BaseModel):
    key: str
    type: str
    lastSeenAt: int | None = None


class ValueInfo(BaseModel):
    key: str | None = None
    type: str | None = None
    value: str | float | bool
    dataset: str | None = None


class AggregateGroup(BaseModel):
    key: str
    value: str


class Aggregate(BaseModel):
    groups: list[AggregateGroup] | None = None
    value: float | None = None
    count: int | None = None


class CalculationResult(BaseModel):
    calculation: str | None = None
    alias: str | None = None
    aggregates: list[Aggregate] | None = None


class EventMetadata(BaseModel):
    id: str | None = None
    requestId: str | None = None
    traceId: str | None = None
    spanId: str | None = None
    service: str | None = None
    level: str | None = None
    message: str | None = None
    error: str | None = None

    model_config = {"extra": "allow"}


class EventWorkers(BaseModel):
    scriptName: str | None = None
    outcome: str | None = None
    eventType: str | None = None
    cpuTimeMs: float | None = None
    wallTimeMs: float | None = None
    executionModel: str | None = None
    event: dict | None = None

    model_config = {"extra": "allow"}


class TelemetryEvent(BaseModel):
    dataset: str | None = None
    timestamp: int | float = 0

    model_config = {"extra": "allow"}

    @property
    def metadata(self) -> EventMetadata:
        raw = self.model_extra.get("$metadata", {}) if self.model_extra else {}
        return EventMetadata.model_validate(raw)

    @property
    def workers(self) -> EventWorkers:
        raw = self.model_extra.get("$workers", {}) if self.model_extra else {}
        return EventWorkers.model_validate(raw)


class EventsResult(BaseModel):
    events: list[dict] | None = None
    count: int | None = None
    fields: list[dict] | None = None


class QueryStatistics(BaseModel):
    elapsed: float | None = None
    rows_read: int | None = None
    bytes_read: int | None = None


class QueryResult(BaseModel):
    calculations: list[CalculationResult] | None = None
    events: EventsResult | None = None
    invocations: dict[str, list[dict]] | None = None
    statistics: QueryStatistics | None = None

    model_config = {"extra": "allow"}


class ApiResponse(BaseModel):
    success: bool
    errors: list[dict] | None = None
    messages: list[dict] | None = None
    result: dict | list | None = None

    model_config = {"extra": "allow"}


class Destination(ApiModel):
    slug: str | None = None
    name: str | None = None
    type: str | None = None
    endpoint: str | None = None
    enabled: bool | None = None
    headers: dict[str, str] | None = None

    model_config = {"extra": "allow"}


# ── Helpers ──────────────────────────────────────────────────────────────────────


def get_config() -> tuple[str, str]:
    account_id = os.environ.get("CF_ACCOUNT_ID", "")
    api_token = os.environ.get("CF_API_TOKEN", "")
    if not account_id:
        click.echo("Error: CF_ACCOUNT_ID environment variable is required", err=True)
        sys.exit(1)
    if not api_token:
        click.echo("Error: CF_API_TOKEN environment variable is required", err=True)
        sys.exit(1)
    return account_id, api_token


def api_headers(api_token: str) -> dict:
    return {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }


def parse_duration(s: str) -> timedelta:
    """Parse a duration string like '1h', '30m', '7d', '2h30m'."""
    units = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}
    total = timedelta()
    buf = ""
    for ch in s:
        if ch.isdigit():
            buf += ch
        elif ch in units and buf:
            total += timedelta(**{units[ch]: int(buf)})
            buf = ""
        else:
            raise click.BadParameter(f"Invalid duration: {s}")
    if buf:
        raise click.BadParameter(f"Invalid duration (trailing digits): {s}")
    return total


def parse_timeframe(since: str | None, until: str | None) -> Timeframe:
    """Return Timeframe for the query."""
    now = datetime.now(timezone.utc)
    if since:
        try:
            from_dt = datetime.fromisoformat(since).replace(tzinfo=timezone.utc)
        except ValueError:
            from_dt = now - parse_duration(since)
    else:
        from_dt = now - timedelta(hours=1)

    if until:
        try:
            to_dt = datetime.fromisoformat(until).replace(tzinfo=timezone.utc)
        except ValueError:
            to_dt = now - parse_duration(until)
    else:
        to_dt = now

    return Timeframe(from_=int(from_dt.timestamp() * 1000), to=int(to_dt.timestamp() * 1000))


def build_filter(f: str) -> Filter:
    """Parse a filter string into a Filter model.

    Supported syntaxes:
        key=value          eq
        key!=value         neq
        key>=value         gte
        key<=value         lte
        key>value          gt
        key<value          lt
        key~=value         regex
        key::value         includes
        key!:value         not_includes
        key^=value         starts_with
        key@=v1,v2,v3      in
        key@!=v1,v2,v3     not_in
        key?               exists  (no value needed)
        key!?              is_null (no value needed)
    """
    if f.endswith("!?"):
        return Filter(key=f[:-2].strip(), operation=FilterOperation.is_null)
    if f.endswith("?"):
        return Filter(key=f[:-1].strip(), operation=FilterOperation.exists)

    ops = [
        ("@!=", FilterOperation.not_in),
        ("@=", FilterOperation.in_),
        ("!=", FilterOperation.neq),
        (">=", FilterOperation.gte),
        ("<=", FilterOperation.lte),
        ("^=", FilterOperation.starts_with),
        ("~=", FilterOperation.regex),
        ("!:", FilterOperation.not_includes),
        ("::", FilterOperation.includes),
        (">", FilterOperation.gt),
        ("<", FilterOperation.lt),
        ("=", FilterOperation.eq),
    ]
    for symbol, operation in ops:
        if symbol in f:
            key, value = f.split(symbol, 1)
            key = key.strip()
            value = value.strip()
            try:
                float(value)
                vtype = FieldType.number
            except ValueError:
                vtype = FieldType.string
            return Filter(key=key, operation=operation, type=vtype, value=value)
    raise click.BadParameter(f"Invalid filter: {f}")


def build_filters(filters: tuple[str, ...], worker: str | None) -> list[Filter]:
    """Build filter list from CLI options."""
    filter_list = [build_filter(f) for f in filters]
    if worker:
        filter_list.append(
            Filter(key="$workers.scriptName", operation=FilterOperation.eq, type=FieldType.string, value=worker)
        )
    return filter_list


def build_params(
    filters: tuple[str, ...],
    worker: str | None,
    needle: str | None = None,
    filter_or: bool = False,
) -> QueryParameters:
    """Build QueryParameters from CLI options."""
    filter_list = build_filters(filters, worker)
    return QueryParameters(
        filterCombination="or" if filter_or else "and",
        filters=filter_list or None,
        needle=Needle(value=needle) if needle else None,
    )


def do_query(account_id: str, api_token: str, body: QueryBody) -> ApiResponse:
    url = f"{API_BASE}/{account_id}/workers/observability/telemetry/query"
    payload = body.model_dump()
    resp = httpx.post(url, headers=api_headers(api_token), json=payload, timeout=60)
    if resp.status_code != 200:
        click.echo(f"HTTP {resp.status_code}: {resp.text}", err=True)
        sys.exit(1)
    data = ApiResponse.model_validate(resp.json())
    if not data.success:
        click.echo(f"API error: {json.dumps(data.errors, indent=2)}", err=True)
        sys.exit(1)
    return data


def do_api(account_id: str, api_token: str, method: str, path: str, body: dict | None = None) -> ApiResponse:
    """Generic API call for non-query endpoints."""
    url = f"{API_BASE}/{account_id}/workers/observability/{path}"
    resp = httpx.request(method, url, headers=api_headers(api_token), json=body, timeout=30)
    if resp.status_code not in (200, 201, 204):
        click.echo(f"HTTP {resp.status_code}: {resp.text}", err=True)
        sys.exit(1)
    if resp.status_code == 204:
        return ApiResponse(success=True)
    data = ApiResponse.model_validate(resp.json())
    if not data.success:
        click.echo(f"API error: {json.dumps(data.errors, indent=2)}", err=True)
        sys.exit(1)
    return data


def format_timestamp(ts: int | float) -> str:
    """Format a millisecond epoch timestamp for display."""
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def format_event(evt: dict) -> str:
    """Format a single event for display."""
    te = TelemetryEvent.model_validate(evt)
    meta = te.metadata
    workers = te.workers

    parts = [format_timestamp(te.timestamp)]
    if workers.scriptName:
        parts.append(f"[{workers.scriptName}]")
    if meta.level:
        parts.append(meta.level.upper())
    status = (workers.event or {}).get("response", {}).get("status", "")
    if status:
        parts.append(f"status={status}")
    if workers.outcome and workers.outcome != "ok":
        parts.append(f"outcome={workers.outcome}")
    if meta.message:
        parts.append(meta.message)
    return " ".join(parts)


# ── Help text constants ──────────────────────────────────────────────────────────

FILTER_HELP = (
    "Filter: key=value, key!=value, key>=value, key~=regex, key::contains, "
    "key!:excludes, key^=startswith, key@=v1,v2 (in), key@!=v1,v2 (not in), "
    "key? (exists), key!? (is null). Repeatable."
)
SINCE_HELP = "Start time: duration (1h/30m/7d) or ISO datetime. Default: 1h."
UNTIL_HELP = "End time: duration or ISO datetime. Default: now."
WORKER_HELP = "Filter by worker script name (shortcut for -f '$workers.scriptName=NAME')."
NEEDLE_HELP = "Free-text search across log messages."
JSON_HELP = "Output raw JSON response."
FILTER_OR_HELP = "Combine filters with OR instead of AND."
CALC_HELP = (
    "Calculation: COUNT, UNIQ:field, AVG:field, SUM:field, MIN:field, MAX:field, "
    "MEDIAN:field, P01:field, P05:field, P10:field, P25:field, P75:field, "
    "P90:field, P95:field, P99:field, P999:field, STDDEV:field, VARIANCE:field. "
    "Default: COUNT. Repeatable."
)


# ── CLI ──────────────────────────────────────────────────────────────────────────

@click.group()
def cli():
    """Query Cloudflare Workers Observability logs.

    A CLI for the Cloudflare Workers Observability API. Fetch events,
    run aggregate queries, tail logs in real time, inspect field keys
    and values, and manage OpenTelemetry export destinations.

    Requires CF_ACCOUNT_ID and CF_API_TOKEN environment variables (or .env file).

    Run cf-logs COMMAND --help for detailed usage and examples.
    """
    pass


# ── events ──────────────────────────────────────────────────────────────────────

PRESETS: dict[str, list[str]] = {
    "errors": [
        "$metadata.level=error",
    ],
    "exceptions": [
        "$workers.outcome=exception",
    ],
    "5xx": [
        "$workers.event.response.status>=500",
    ],
    "4xx": [
        "$workers.event.response.status>=400",
        "$workers.event.response.status<500",
    ],
    "slow": [
        "$workers.wallTimeMs>=1000",
    ],
    "cold": [
        "$workers.coldStart=true",
    ],
    "cpu": [
        "$workers.outcome@=exceededCpu,exceededMemory",
    ],
}


@cli.command()
@click.option("--since", "-s", default=None, help=SINCE_HELP)
@click.option("--until", "-u", default=None, help=UNTIL_HELP)
@click.option("--filter", "-f", "filters", multiple=True, help=FILTER_HELP)
@click.option("--needle", "-n", default=None, help=NEEDLE_HELP)
@click.option("--worker", "-w", default=None, help=WORKER_HELP)
@click.option("--limit", "-l", default=100, help="Max events to return (max 2000). Default: 100.")
@click.option("--json-output", "-j", is_flag=True, help=JSON_HELP)
@click.option("--or", "filter_or", is_flag=True, help=FILTER_OR_HELP)
@click.option("--preset", "-p", type=click.Choice(list(PRESETS.keys())),
              default=None, help="Preset filter: errors, exceptions, 5xx, 4xx, slow, cold, cpu.")
def events(since, until, filters, needle, worker, limit, json_output, filter_or, preset):
    """Fetch log events.

    Retrieves individual log events ordered by timestamp. Each event
    includes metadata (level, message), worker info (script name,
    outcome, status), and custom fields.

    \b
    Presets (-p NAME):
      errors       log level = error
      exceptions   unhandled exceptions (outcome = exception)
      5xx          HTTP 5xx responses
      4xx          HTTP 4xx responses
      slow         slow requests (wall time >= 1s)
      cold         cold start requests
      cpu          exceeded CPU or memory limits

    \b
    Filter operators:
      key=value          equals
      key!=value         not equals
      key>value          greater than
      key>=value         greater than or equal
      key<value          less than
      key<=value         less than or equal
      key~=pattern       regex match
      key::value         contains substring
      key!:value         does not contain substring
      key^=value         starts with
      key@=v1,v2,v3      value in list
      key@!=v1,v2,v3     value not in list
      key?               field exists
      key!?              field is null

    \b
    Common fields:
      $workers.scriptName                worker name
      $workers.outcome                   ok, exception, exceededCpu, ...
      $workers.wallTimeMs                wall time in ms
      $workers.cpuTimeMs                 CPU time in ms
      $workers.eventType                 fetch, scheduled, cron, queue, ...
      $workers.event.response.status     HTTP status code
      $metadata.level                    log, warn, error, ...
      $metadata.message                  log message text

    \b
    Examples:
      cf-logs events                              # last hour
      cf-logs events -s 24h -w my-worker          # last 24h for a worker
      cf-logs events -p errors -w my-api           # errors for a worker
      cf-logs events -p 5xx -s 6h                 # 5xx in last 6 hours
      cf-logs events -p slow -w my-api -j         # slow requests as JSON
      cf-logs events -f '$workers.outcome=exception'
      cf-logs events -f '$workers.event.response.status>=500'
      cf-logs events -n "error" -l 500 -j         # search + JSON output
      cf-logs events -f '$metadata.level=error' --or -f '$workers.outcome=exception'
    """
    account_id, api_token = get_config()
    tf = parse_timeframe(since, until)

    # Merge preset filters with user filters
    all_filters = list(filters)
    if preset:
        all_filters.extend(PRESETS[preset])

    params = build_params(tuple(all_filters), worker, needle, filter_or)

    body = QueryBody(
        queryId=f"cli-{int(time.time())}",
        view=ViewType.events,
        limit=limit,
        timeframe=tf,
        parameters=params,
    )

    data = do_query(account_id, api_token, body)
    result = data.result or {}

    if json_output:
        click.echo(json.dumps(result, indent=2))
        return

    events_data = result.get("events", {})
    event_list = events_data.get("events", [])

    if not event_list:
        click.echo("No events found.")
        return

    click.echo(f"Found {events_data.get('count', len(event_list))} events:\n")
    for evt in event_list:
        click.echo(format_event(evt))


# ── query ───────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--since", "-s", default=None, help=SINCE_HELP)
@click.option("--until", "-u", default=None, help=UNTIL_HELP)
@click.option("--filter", "-f", "filters", multiple=True, help=FILTER_HELP)
@click.option("--worker", "-w", default=None, help=WORKER_HELP)
@click.option("--needle", "-n", default=None, help=NEEDLE_HELP)
@click.option("--calc", "-c", "calculations", multiple=True, help=CALC_HELP)
@click.option("--group-by", "-g", "group_bys", multiple=True,
              help="Group results by field. Repeatable. Example: '$workers.event.response.status'.")
@click.option("--having", "-H", "havings", multiple=True,
              help="Having clause to filter groups by calculation result. "
                   "Format: ALIAS>value, ALIAS>=value, ALIAS<value, ALIAS<=value, ALIAS=value. Repeatable.")
@click.option("--order", "-o", default=None,
              help="Order results by calculation alias. Format: ALIAS:asc or ALIAS:desc. Default: desc.")
@click.option("--limit", "-l", default=10, help="Max groups to return (max 2000). Default: 10.")
@click.option("--granularity", default=None, type=int,
              help="Time granularity in seconds for time-series buckets. Auto-detected if omitted.")
@click.option("--json-output", "-j", is_flag=True, help=JSON_HELP)
@click.option("--or", "filter_or", is_flag=True, help=FILTER_OR_HELP)
def query(since, until, filters, worker, needle, calculations, group_bys, havings,
          order, limit, granularity, json_output, filter_or):
    """Run aggregate queries (counts, percentiles, etc.).

    Computes aggregate calculations over log events. Supports grouping,
    ordering, having clauses, and time-series granularity.

    \b
    Examples:
      cf-logs query                                         # total event count
      cf-logs query -c COUNT -g '$workers.scriptName'       # count per worker
      cf-logs query -c P99:'$workers.wallTimeMs' -s 24h     # p99 latency
      cf-logs query -c AVG:'$workers.cpuTimeMs' -g '$workers.scriptName' -o AVG:desc
      cf-logs query -c COUNT -g '$workers.outcome' --granularity 300
    """
    account_id, api_token = get_config()
    tf = parse_timeframe(since, until)
    params = build_params(filters, worker, needle, filter_or)
    params.limit = limit

    calc_list = []
    for c in calculations:
        if ":" in c:
            op, key = c.split(":", 1)
            calc_list.append(Calculation(operator=CalculationOperator(op.lower()), key=key))
        else:
            calc_list.append(Calculation(operator=CalculationOperator(c.lower())))
    if not calc_list:
        calc_list = [Calculation(operator=CalculationOperator.count)]
    params.calculations = calc_list

    if group_bys:
        params.groupBys = [GroupBy(value=g) for g in group_bys]

    if havings:
        having_list = []
        having_ops = [(">=", "gte"), ("<=", "lte"), (">", "gt"), ("<", "lt"), ("=", "eq")]
        for h in havings:
            for sym, op in having_ops:
                if sym in h:
                    alias, val = h.split(sym, 1)
                    having_list.append(Having(key=alias.strip(), operation=op, value=float(val.strip())))
                    break
        if having_list:
            params.havings = having_list

    if order:
        parts = order.split(":")
        params.orderBy = OrderBy(
            value=parts[0],
            order=parts[1] if len(parts) > 1 else "desc",
        )

    body = QueryBody(
        queryId=f"cli-{int(time.time())}",
        view=ViewType.calculations,
        timeframe=tf,
        parameters=params,
        granularity=granularity,
    )

    data = do_query(account_id, api_token, body)
    result = data.result or {}

    if json_output:
        click.echo(json.dumps(result, indent=2))
        return

    calcs = result.get("calculations", [])
    if not calcs:
        click.echo("No results.")
        return

    for calc in calcs:
        cr = CalculationResult.model_validate(calc)
        name = cr.alias or cr.calculation or ""
        click.echo(f"--- {name} ---")
        for agg in cr.aggregates or []:
            value = agg.value if agg.value is not None else agg.count
            if agg.groups:
                group_str = ", ".join(f"{g.key}={g.value}" for g in agg.groups)
                click.echo(f"  {group_str}: {value}")
            else:
                click.echo(f"  {value}")


# ── invocations ─────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--since", "-s", default=None, help=SINCE_HELP)
@click.option("--until", "-u", default=None, help=UNTIL_HELP)
@click.option("--filter", "-f", "filters", multiple=True, help=FILTER_HELP)
@click.option("--needle", "-n", default=None, help=NEEDLE_HELP)
@click.option("--worker", "-w", default=None, help=WORKER_HELP)
@click.option("--limit", "-l", default=25, help="Max invocations to return (max 2000). Default: 25.")
@click.option("--json-output", "-j", is_flag=True, help=JSON_HELP)
@click.option("--or", "filter_or", is_flag=True, help=FILTER_OR_HELP)
def invocations(since, until, filters, needle, worker, limit, json_output, filter_or):
    """Fetch logs grouped by invocation (request).

    Shows all log entries grouped by their request/invocation ID,
    making it easy to trace a single request through its lifecycle.

    \b
    Examples:
      cf-logs invocations -w my-worker -s 15m
      cf-logs invocations -f '$workers.outcome=exception'
      cf-logs invocations -n "timeout" -j
    """
    account_id, api_token = get_config()
    tf = parse_timeframe(since, until)
    params = build_params(filters, worker, needle, filter_or)

    body = QueryBody(
        queryId=f"cli-{int(time.time())}",
        view=ViewType.invocations,
        limit=limit,
        timeframe=tf,
        parameters=params,
    )

    data = do_query(account_id, api_token, body)
    result = data.result or {}

    if json_output:
        click.echo(json.dumps(result, indent=2))
        return

    invocations_data = result.get("invocations", {})
    if not invocations_data:
        click.echo("No invocations found.")
        return

    for request_id, events_list in invocations_data.items():
        click.echo(f"\n{'='*60}")
        click.echo(f"Request: {request_id}")
        click.echo(f"{'='*60}")
        for evt in events_list:
            click.echo(f"  {format_event(evt)}")


# ── tail ────────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--since", "-s", default=None, help=SINCE_HELP)
@click.option("--until", "-u", default=None, help=UNTIL_HELP)
@click.option("--filter", "-f", "filters", multiple=True, help=FILTER_HELP)
@click.option("--worker", "-w", default=None, help=WORKER_HELP)
@click.option("--needle", "-n", default=None, help=NEEDLE_HELP)
@click.option("--limit", "-l", default=100, help="Max events per poll (max 2000). Default: 100.")
@click.option("--interval", "-i", default=5, help="Poll interval in seconds. Default: 5.")
@click.option("--json-output", "-j", is_flag=True, help=JSON_HELP)
@click.option("--or", "filter_or", is_flag=True, help=FILTER_OR_HELP)
def tail(since, until, filters, needle, worker, limit, interval, json_output, filter_or):
    """Poll for new log events (like tail -f).

    Continuously polls the API for new events and prints them as they
    arrive. Deduplicates by event ID. Press Ctrl+C to stop.

    \b
    Examples:
      cf-logs tail -w my-worker
      cf-logs tail -f '$workers.outcome=exception' -i 2
      cf-logs tail -n "error" -j
    """
    account_id, api_token = get_config()

    poll_since = since or "2m"

    click.echo("Tailing logs (Ctrl+C to stop)...\n", err=True)
    seen_ids: set[str] = set()

    try:
        while True:
            tf = parse_timeframe(poll_since, until)
            params = build_params(filters, worker, needle, filter_or)

            body = QueryBody(
                queryId=f"cli-tail-{int(time.time())}",
                view=ViewType.events,
                limit=limit,
                timeframe=tf,
                parameters=params,
            )

            try:
                data = do_query(account_id, api_token, body)
            except SystemExit:
                time.sleep(interval)
                continue

            result = data.result or {}
            event_list = result.get("events", {}).get("events", [])
            for evt in event_list:
                evt_id = (evt.get("$metadata") or {}).get("id", "")
                if evt_id in seen_ids:
                    continue
                seen_ids.add(evt_id)

                if json_output:
                    click.echo(json.dumps(evt))
                else:
                    click.echo(format_event(evt))

            if len(seen_ids) > 10000:
                seen_ids = set(list(seen_ids)[-5000:])

            time.sleep(interval)
    except KeyboardInterrupt:
        click.echo("\nStopped.", err=True)


# ── keys ────────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--since", "-s", default=None, help=SINCE_HELP)
@click.option("--until", "-u", default=None, help=UNTIL_HELP)
@click.option("--filter", "-f", "filters", multiple=True, help=FILTER_HELP)
@click.option("--worker", "-w", default=None, help=WORKER_HELP)
@click.option("--needle", "-n", default=None, help=NEEDLE_HELP)
@click.option("--search", "-S", "key_needle", default=None,
              help="Search for keys matching this substring.")
@click.option("--limit", "-l", default=1000, help="Max keys to return. Default: 1000.")
def keys(since, until, filters, worker, needle, key_needle, limit):
    """List available field keys.

    Shows all field keys present in your telemetry data within the
    given timeframe. Useful for discovering which fields are available
    for filtering and grouping.

    \b
    Examples:
      cf-logs keys                         # all keys in last hour
      cf-logs keys -S status               # keys matching 'status'
      cf-logs keys -w my-worker -s 24h     # keys for a specific worker
    """
    account_id, api_token = get_config()
    tf = parse_timeframe(since, until)

    filter_list = build_filters(filters, worker)

    body = KeysRequestBody(
        timeframe=tf,
        filters=filter_list or None,
        needle=Needle(value=needle) if needle else None,
        keyNeedle=Needle(value=key_needle) if key_needle else None,
        limit=limit,
    )

    data = do_api(account_id, api_token, "POST", "telemetry/keys", body.model_dump())

    for key_info in data.result or []:
        ki = KeyInfo.model_validate(key_info)
        click.echo(f"  {ki.key} ({ki.type})")


# ── values ──────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("key")
@click.option("--since", "-s", default=None, help=SINCE_HELP)
@click.option("--until", "-u", default=None, help=UNTIL_HELP)
@click.option("--filter", "-f", "filters", multiple=True, help=FILTER_HELP)
@click.option("--worker", "-w", default=None, help=WORKER_HELP)
@click.option("--needle", "-n", default=None, help="Search within values.")
@click.option("--limit", "-l", default=50, help="Max values to return. Default: 50.")
@click.option("--type", "-t", "key_type", default="string",
              type=click.Choice(["string", "number", "boolean"]),
              help="Type of the key. Default: string.")
def values(key, since, until, filters, worker, needle, limit, key_type):
    """List unique values for a field KEY.

    Retrieves the distinct values observed for a specific field key.
    Useful for discovering what values exist before filtering.

    \b
    Examples:
      cf-logs values '$workers.scriptName'
      cf-logs values '$workers.outcome' -s 24h
      cf-logs values '$workers.event.response.status' -t number -w my-worker
    """
    account_id, api_token = get_config()
    tf = parse_timeframe(since, until)

    filter_list = build_filters(filters, worker)

    body = ValuesRequestBody(
        timeframe=tf,
        key=key,
        type=FieldType(key_type),
        filters=filter_list or None,
        needle=Needle(value=needle) if needle else None,
        limit=limit,
    )

    data = do_api(account_id, api_token, "POST", "telemetry/values", body.model_dump())

    for val_info in data.result or []:
        vi = ValueInfo.model_validate(val_info)
        click.echo(f"  {vi.value}")


# ── destinations ────────────────────────────────────────────────────────────────

@cli.group("destinations")
def destinations_group():
    """Manage OpenTelemetry export destinations.

    Configure where your Workers telemetry data is exported to. Supports
    creating, listing, updating, and deleting OTLP export destinations.
    """
    pass


@destinations_group.command("list")
@click.option("--json-output", "-j", is_flag=True, help=JSON_HELP)
def destinations_list(json_output):
    """List all export destinations.

    Shows all configured OpenTelemetry export destinations with their
    name, type, enabled status, and slug identifier.
    """
    account_id, api_token = get_config()
    data = do_api(account_id, api_token, "GET", "destinations")

    if json_output:
        click.echo(json.dumps(data.result or [], indent=2))
        return

    result = data.result or []
    if not result:
        click.echo("No destinations configured.")
        return

    for dest_raw in result:
        dest = Destination.model_validate(dest_raw)
        name = dest.name or dest.slug or ""
        enabled = "enabled" if dest.enabled else "disabled"
        click.echo(f"  {name} ({dest.type}) [{enabled}] - {dest.slug}")


@destinations_group.command("create")
@click.option("--name", required=True, help="Destination name.")
@click.option("--type", "dest_type", required=True, help="Destination type (e.g. 'otlp').")
@click.option("--endpoint", required=True, help="Destination endpoint URL.")
@click.option("--header", "-H", "headers_list", multiple=True,
              help="Header as KEY=VALUE. Repeatable.")
@click.option("--json-output", "-j", is_flag=True, help=JSON_HELP)
def destinations_create(name, dest_type, endpoint, headers_list, json_output):
    """Create a new export destination.

    \b
    Example:
      cf-logs destinations create --name my-dest --type otlp \\
        --endpoint https://otel.example.com/v1/traces \\
        -H 'Authorization=Bearer token123'
    """
    account_id, api_token = get_config()

    dest = Destination(name=name, type=dest_type, endpoint=endpoint)
    if headers_list:
        headers_dict = {}
        for h in headers_list:
            k, v = h.split("=", 1)
            headers_dict[k.strip()] = v.strip()
        dest.headers = headers_dict

    data = do_api(account_id, api_token, "POST", "destinations", dest.model_dump())

    if json_output:
        click.echo(json.dumps(data.result or {}, indent=2))
    else:
        result = Destination.model_validate(data.result or {})
        click.echo(f"Created destination: {result.slug}")


@destinations_group.command("update")
@click.argument("slug")
@click.option("--name", default=None, help="New destination name.")
@click.option("--endpoint", default=None, help="New endpoint URL.")
@click.option("--enabled/--disabled", default=None, help="Enable or disable the destination.")
@click.option("--header", "-H", "headers_list", multiple=True,
              help="Header as KEY=VALUE. Repeatable (replaces all headers).")
@click.option("--json-output", "-j", is_flag=True, help=JSON_HELP)
def destinations_update(slug, name, endpoint, enabled, headers_list, json_output):
    """Update an export destination by SLUG.

    \b
    Example:
      cf-logs destinations update my-dest-slug --name 'New Name' --disabled
    """
    account_id, api_token = get_config()

    body: dict = {}
    if name is not None:
        body["name"] = name
    if endpoint is not None:
        body["endpoint"] = endpoint
    if enabled is not None:
        body["enabled"] = enabled
    if headers_list:
        headers_dict = {}
        for h in headers_list:
            k, v = h.split("=", 1)
            headers_dict[k.strip()] = v.strip()
        body["headers"] = headers_dict

    if not body:
        click.echo("Nothing to update. Provide at least one option.", err=True)
        sys.exit(1)

    data = do_api(account_id, api_token, "PATCH", f"destinations/{slug}", body)

    if json_output:
        click.echo(json.dumps(data.result or {}, indent=2))
    else:
        click.echo(f"Updated destination: {slug}")


@destinations_group.command("delete")
@click.argument("slug")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt.")
def destinations_delete(slug, yes):
    """Delete an export destination by SLUG.

    \b
    Example:
      cf-logs destinations delete my-dest-slug -y
    """
    if not yes:
        click.confirm(f"Delete destination '{slug}'?", abort=True)

    account_id, api_token = get_config()
    do_api(account_id, api_token, "DELETE", f"destinations/{slug}")
    click.echo(f"Deleted destination: {slug}")


if __name__ == "__main__":
    cli()
