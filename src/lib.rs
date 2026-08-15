//! StatsD `metrics_sink` plugin (`dev.mcpg.metrics.statsd`).
//!
//! Formats each metric data point as a statsd / DogStatsD line and pushes it to
//! a UDP collector (statsd, DogStatsD, Telegraf, …) or to stdout / stderr.
//! Counters → `name:value|c`, gauges → `|g`, histograms → one `|h` (or `|ms` /
//! `|d`) sample per observation. Labels become DogStatsD tags (`|#k:v,…`) when
//! enabled. Best-effort delivery; pure formatter + `std::net` emit. Fails closed
//! on bad config.

use std::io::Write;
use std::net::UdpSocket;

use mcpg_plugin_protocol::capability::Capability;
use mcpg_plugin_protocol::telemetry::{MetricKind, MetricPoint, MetricValue};
use mcpg_plugin_protocol::{PluginManifest, firstparty_manifest};
use mcpg_plugin_sdk::ffi::SyncMetricsSink;
use serde::Deserialize;

const PLUGIN_ID: &str = "dev.mcpg.metrics.statsd";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum HistogramType {
    /// DogStatsD histogram (`|h`).
    #[default]
    Histogram,
    /// statsd timer (`|ms`).
    Timing,
    /// DogStatsD distribution (`|d`).
    Distribution,
}

impl HistogramType {
    fn suffix(self) -> &'static str {
        match self {
            HistogramType::Histogram => "h",
            HistogramType::Timing => "ms",
            HistogramType::Distribution => "d",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Destination {
    /// UDP datagram per emit (`address` = `host:port`, e.g. `127.0.0.1:8125`).
    Udp {
        address: String,
    },
    Stdout,
    Stderr,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatsdConfig {
    destination: Destination,
    /// Prepended to every metric name as `prefix.name`.
    #[serde(default)]
    prefix: Option<String>,
    /// Emit labels as DogStatsD tags (`|#k:v`). Disable for vanilla statsd.
    #[serde(default = "default_true")]
    emit_tags: bool,
    /// Wire type for histogram points.
    #[serde(default)]
    histogram_type: HistogramType,
}

fn default_true() -> bool {
    true
}

enum Emitter {
    Udp { socket: UdpSocket, address: String },
    Stdout,
    Stderr,
}

pub struct StatsdSink {
    manifest: PluginManifest,
    emitter: Emitter,
    prefix: Option<String>,
    emit_tags: bool,
    histogram_type: HistogramType,
}

/// Replace statsd metasyntax (`: | @ #` , whitespace, comma) with `_`.
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            ':' | '|' | '@' | '#' | ',' | '\n' | ' ' | '\t' => '_',
            other => other,
        })
        .collect()
}

/// Format an `f64` without scientific notation or a redundant `.0`.
fn fmt_f64(v: f64) -> String {
    // `{}` already prints `5` for 5.0 and `1.5` for 1.5; guard non-finite.
    if v.is_finite() {
        format!("{v}")
    } else {
        "0".to_owned()
    }
}

fn scalar(value: &MetricValue) -> Option<String> {
    match value {
        MetricValue::F64 { value } => Some(fmt_f64(*value)),
        MetricValue::I64 { value } => Some(value.to_string()),
        MetricValue::Histogram { .. } => None,
    }
}

impl StatsdSink {
    /// SDK factory. Fails closed: a bad config or an unbindable UDP socket
    /// panics (→ null handle → boot Err).
    pub fn from_config_json(config_json: &str) -> Self {
        let cfg: StatsdConfig = serde_json::from_str(config_json)
            .unwrap_or_else(|err| panic!("metrics-statsd: config JSON failed to parse: {err}"));
        let emitter = match &cfg.destination {
            Destination::Udp { address } => {
                if address.is_empty() {
                    panic!("metrics-statsd: udp address must not be empty");
                }
                let socket = UdpSocket::bind("0.0.0.0:0")
                    .unwrap_or_else(|e| panic!("metrics-statsd: failed to bind UDP socket: {e}"));
                Emitter::Udp {
                    socket,
                    address: address.clone(),
                }
            }
            Destination::Stdout => Emitter::Stdout,
            Destination::Stderr => Emitter::Stderr,
        };
        Self {
            manifest: firstparty_manifest! {
                id: PLUGIN_ID,
                name: "StatsD Metrics Sink",
                class: MetricsSink,
                capabilities: [Capability::NetworkOutbound],
            },
            emitter,
            prefix: cfg.prefix,
            emit_tags: cfg.emit_tags,
            histogram_type: cfg.histogram_type,
        }
    }

    fn tags(&self, metric: &MetricPoint) -> String {
        if !self.emit_tags || metric.labels.is_empty() {
            return String::new();
        }
        let body = metric
            .labels
            .iter()
            .map(|(k, v)| format!("{}:{}", sanitize(k), sanitize(v)))
            .collect::<Vec<_>>()
            .join(",");
        format!("|#{body}")
    }

    fn metric_name(&self, raw: &str) -> String {
        let name = sanitize(raw);
        match &self.prefix {
            Some(p) if !p.is_empty() => format!("{}.{name}", sanitize(p)),
            _ => name,
        }
    }

    /// Render a metric into one or more statsd lines (newline-joined).
    fn render(&self, metric: &MetricPoint) -> String {
        let name = self.metric_name(&metric.name);
        let tags = self.tags(metric);
        match metric.kind {
            MetricKind::Counter | MetricKind::Gauge => {
                let t = if metric.kind == MetricKind::Counter {
                    "c"
                } else {
                    "g"
                };
                let val = scalar(&metric.value).unwrap_or_else(|| "0".to_owned());
                format!("{name}:{val}|{t}{tags}")
            }
            MetricKind::Histogram => {
                let h = self.histogram_type.suffix();
                match &metric.value {
                    MetricValue::Histogram {
                        observations, sum, ..
                    } => {
                        if observations.is_empty() {
                            format!("{name}:{}|{h}{tags}", fmt_f64(*sum))
                        } else {
                            observations
                                .iter()
                                .map(|o| format!("{name}:{}|{h}{tags}", fmt_f64(*o)))
                                .collect::<Vec<_>>()
                                .join("\n")
                        }
                    }
                    // Kind says histogram but a scalar value was supplied.
                    other => {
                        let val = scalar(other).unwrap_or_else(|| "0".to_owned());
                        format!("{name}:{val}|{h}{tags}")
                    }
                }
            }
        }
    }
}

impl SyncMetricsSink for StatsdSink {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn emit(&self, metric: &MetricPoint) {
        let line = self.render(metric);
        match &self.emitter {
            Emitter::Udp { socket, address } => {
                if let Err(e) = socket.send_to(line.as_bytes(), address.as_str()) {
                    tracing::debug!(error = %e, "metrics-statsd: udp send failed; dropping point");
                }
            }
            Emitter::Stdout => {
                let mut out = std::io::stdout().lock();
                let _ = writeln!(out, "{line}");
            }
            Emitter::Stderr => {
                let mut out = std::io::stderr().lock();
                let _ = writeln!(out, "{line}");
            }
        }
    }
}

mcpg_plugin_sdk::declare_plugin! {
    plugin_id: "dev.mcpg.metrics.statsd",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[Capability::NetworkOutbound],
    entities: [
        metrics_sink as entity {
            inner_name: "",
            plugin_type: StatsdSink,
            factory: |cfg: &str, _host: ::mcpg_plugin_sdk::HostHandle| StatsdSink::from_config_json(cfg),
        },
    ],
}

#[cfg(test)]
mod tests;
