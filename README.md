# cf-logs

CLI for querying Cloudflare Workers Observability logs.

## Setup

Requires Python 3.13+ and [uv](https://docs.astral.sh/uv/).

```bash
# Install dependencies
uv sync

# Set credentials (or create a .env file)
export CF_ACCOUNT_ID="your-account-id"
export CF_API_TOKEN="your-api-token"
```

The API token needs the **Workers Observability Read** permission (and **Write** for managing destinations).

## Usage

```
cf-logs COMMAND [OPTIONS]
```

Run `cf-logs COMMAND --help` for detailed usage and examples.

### Commands

| Command | Description |
|---------|-------------|
| `events` | Fetch individual log events |
| `query` | Run aggregate queries (counts, percentiles, etc.) |
| `invocations` | Fetch logs grouped by request |
| `tail` | Live-tail logs (like `tail -f`) |
| `keys` | List available field keys |
| `values` | List unique values for a field |
| `destinations` | Manage OpenTelemetry export destinations |

## Examples

### Fetch recent events

```bash
# Last hour of events (default)
cf-logs events

# Last 24 hours for a specific worker
cf-logs events -s 24h -w my-api

# Events with 5xx status codes
cf-logs events -f '$workers.event.response.status>=500'

# Search for "timeout" in log messages
cf-logs events -n "timeout"

# Exceptions or errors (OR filter)
cf-logs events -f '$workers.outcome=exception' -f '$metadata.level=error' --or

# Raw JSON output for piping to jq
cf-logs events -w my-api -j | jq '.events.events[].["$metadata"].message'
```

### Aggregate queries

```bash
# Total event count (default)
cf-logs query

# Count events per worker
cf-logs query -c COUNT -g '$workers.scriptName'

# P99 latency over the last 24 hours
cf-logs query -c P99:'$workers.wallTimeMs' -s 24h

# Average CPU time per worker, sorted descending
cf-logs query -c AVG:'$workers.cpuTimeMs' -g '$workers.scriptName' -o AVG:desc

# Error rate by status code
cf-logs query -c COUNT -g '$workers.event.response.status' -w my-api

# Multiple calculations at once
cf-logs query -c COUNT -c P50:'$workers.wallTimeMs' -c P99:'$workers.wallTimeMs'

# Time-series with 5-minute buckets
cf-logs query -c COUNT -g '$workers.outcome' --granularity 300 -j

# Only show groups with more than 100 events
cf-logs query -c COUNT -g '$workers.scriptName' -H 'COUNT>100'
```

### Trace a request

```bash
# View logs grouped by invocation
cf-logs invocations -w my-api -s 15m

# Find failed invocations
cf-logs invocations -f '$workers.outcome=exception' -s 1h
```

### Live tail

```bash
# Tail all logs
cf-logs tail

# Tail a specific worker
cf-logs tail -w my-api

# Tail errors only, poll every 2 seconds
cf-logs tail -f '$metadata.level=error' -i 2

# Tail as JSON for piping
cf-logs tail -w my-api -j | jq '.["$metadata"].message'
```

### Discover fields and values

```bash
# List all available field keys
cf-logs keys

# Search for keys related to "status"
cf-logs keys -S status

# List all worker names
cf-logs values '$workers.scriptName'

# List all outcomes for a worker
cf-logs values '$workers.outcome' -w my-api

# List HTTP status codes (numeric field)
cf-logs values '$workers.event.response.status' -t number
```

### Manage export destinations

```bash
# List configured destinations
cf-logs destinations list

# Create an OTLP destination
cf-logs destinations create \
  --name my-collector \
  --type otlp \
  --endpoint https://otel.example.com/v1/traces \
  -H 'Authorization=Bearer token123'

# Disable a destination
cf-logs destinations update my-collector-slug --disabled

# Delete a destination
cf-logs destinations delete my-collector-slug -y
```

## Filter syntax

Filters are passed with `-f` and support these operators:

| Syntax | Operator |
|--------|----------|
| `key=value` | equals |
| `key!=value` | not equals |
| `key>value` | greater than |
| `key>=value` | greater than or equal |
| `key<value` | less than |
| `key<=value` | less than or equal |
| `key~=pattern` | regex match |
| `key::value` | contains substring |
| `key!:value` | does not contain |
| `key^=value` | starts with |
| `key@=a,b,c` | value in list |
| `key@!=a,b,c` | value not in list |
| `key?` | field exists |
| `key!?` | field is null |

Multiple `-f` flags are combined with AND by default. Use `--or` to combine with OR.

## Common fields

| Field | Description |
|-------|-------------|
| `$workers.scriptName` | Worker script name |
| `$workers.outcome` | `ok`, `exception`, `exceededCpu`, `exceededMemory`, `canceled` |
| `$workers.wallTimeMs` | Wall clock time in ms |
| `$workers.cpuTimeMs` | CPU time in ms |
| `$workers.eventType` | `fetch`, `scheduled`, `cron`, `queue`, `alarm`, `email`, `rpc` |
| `$workers.executionModel` | `stateless` or `durableObject` |
| `$workers.event.response.status` | HTTP response status code |
| `$metadata.level` | `log`, `debug`, `info`, `warn`, `error` |
| `$metadata.message` | Log message text |
| `$metadata.requestId` | Request ID |
| `$metadata.traceId` | Trace ID |

## Calculation operators

Used with `-c` in the `query` command:

`COUNT`, `UNIQ`, `AVG`, `SUM`, `MIN`, `MAX`, `MEDIAN`, `STDDEV`, `VARIANCE`, `P01`, `P05`, `P10`, `P25`, `P75`, `P90`, `P95`, `P99`, `P999`

Format: `OPERATOR` (for count) or `OPERATOR:field` (for field aggregations).

## Time formats

The `--since` and `--until` options accept:

- **Durations**: `30s`, `5m`, `1h`, `7d`, `2w`, `1h30m`
- **ISO datetimes**: `2024-01-15T10:00:00`
