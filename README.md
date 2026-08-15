# StatsD Metrics Sink — `dev.mcpg.metrics.statsd`

> class `metrics_sink` · `native` · package `mcpg-plugin-metrics-statsd` · artifact `libmcpg_plugin_metrics_statsd.so` · Apache-2.0

Pushes MCP gateway metrics to a statsd-family collector. Each metric point the
gateway records is formatted as a statsd or DogStatsD line and written to a UDP
endpoint — statsd, DogStatsD, the Datadog Agent, Telegraf — or to stdout /
stderr for local inspection. Labels become DogStatsD tags, and metric names,
tag keys, and tag values are sanitised so nothing in a label can corrupt the
line format. Reach for it when your metrics pipeline is push-based and UDP-fed
rather than scrape-based; use the Prometheus sink instead when a scraper pulls
from the gateway.

## What it does
- Renders counters as `name:value|c`, gauges as `name:value|g`, and histograms
  as one line per observation with a configurable `|h`, `|ms`, or `|d` suffix.
- Falls back to the histogram's `sum` as a single line when a point carries
  aggregates but no individual observations.
- Appends labels as DogStatsD tags (`|#key:value,key2:value2`) when enabled;
  disable tagging for a vanilla statsd server that cannot parse them.
- Replaces statsd metasyntax — `:`, `|`, `@`, `#`, `,`, and whitespace — with
  `_` in names, tag keys, and tag values.
- Sends best-effort: a failed UDP write is logged at debug level and the point
  is dropped, never retried and never blocking the caller.
- Declares the `network_outbound` capability, consumed by the UDP destination.
- Fails closed at load: a malformed config, an unknown field, an empty UDP
  address, or an unbindable socket refuses the plugin instead of starting a
  gateway that silently reports nothing.

## Configuration
Loaded from the flat top-level `plugins:` list, then referenced by id from
`observability.metrics.sinks[]`. Both halves are required, and they carry
different things: the `plugins:` entry loads the artifact, grants the
capability, and holds the `config:` block the plugin is built from, while the
sinks entry is purely the routing list that decides which plugin ids receive
metric points.

```yaml
plugins:
  - id: dev.mcpg.metrics.statsd
    kind: native
    class: metrics_sink
    source:
      oci: ghcr.io/mcpg-dev/source-code/plugins/metrics-statsd:protocol-1
    granted_capabilities:
      - network_outbound
    config:
      destination:
        kind: udp
        address: "127.0.0.1:8125"
      prefix: mcpg
      emit_tags: true
      histogram_type: distribution

observability:
  enabled: true
  metrics:
    enabled: true
    sinks:
      - kind: dev.mcpg.metrics.statsd
```

| Field | Type | Default | Description |
|---|---|---|---|
| `destination` | object | *required* | `{kind: udp, address: "host:port"}`, `{kind: stdout}`, or `{kind: stderr}`. |
| `prefix` | string or null | `null` | Prepended to every metric name as `<prefix>.<name>`. An empty string is treated as absent. |
| `emit_tags` | bool | `true` | Append labels as DogStatsD tags. Set `false` for a statsd server without tag support. |
| `histogram_type` | `histogram` \| `timing` \| `distribution` | `histogram` | Wire suffix for histogram points: `\|h`, `\|ms`, or `\|d`. |

Unknown fields are rejected. `destination` has no default: a config block that
omits it refuses the plugin.

With the example above, a counter `requests_total{route=/mcp}` incremented by 1
is sent as `mcpg.requests_total:1|c|#route:/mcp`.

## Build
The `cdylib-export` feature is on by default, so a standalone build already
produces a loadable artifact; naming the feature explicitly keeps the command
unambiguous:

```bash
cargo build -p mcpg-plugin-metrics-statsd --features cdylib-export --release   # → target/release/libmcpg_plugin_metrics_statsd.so
```

## Testing
The unit suite covers line rendering for every metric kind and includes a
loopback UDP test that binds an ephemeral socket and asserts the exact datagram
bytes, so a formatting regression fails locally with no external collector:

```bash
cargo test -p mcpg-plugin-metrics-statsd
```

## Sign & load (production)
Sign the artifact, pin/verify via the entry's `signature:` block, and honour
revocations. See <https://mcpg.dev/docs/security/plugin-security>.

## See also
- Observability signals and how sinks fan out: <https://mcpg.dev/docs/reference/configuration>
- Plugin classes and the loading contract: <https://mcpg.dev/docs/plugins/plugins-and-protocol>
- Scrape-based metrics instead of push: `libs/plugins/observability/prometheus`
- The logs signal over syslog: `libs/plugins/observability/syslog`
