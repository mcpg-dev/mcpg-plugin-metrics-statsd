use std::collections::BTreeMap;
use std::net::UdpSocket;
use std::time::Duration;

use mcpg_plugin_protocol::telemetry::{MetricKind, MetricPoint, MetricValue};
use mcpg_plugin_sdk::ffi::SyncMetricsSink;
use serde_json::{Value, json};

use super::{PLUGIN_ID, StatsdSink};

fn build(cfg: Value) -> StatsdSink {
    StatsdSink::from_config_json(&cfg.to_string())
}

fn point(name: &str, kind: MetricKind, value: MetricValue) -> MetricPoint {
    MetricPoint {
        name: name.into(),
        unit: None,
        kind,
        value,
        labels: BTreeMap::new(),
        timestamp_ns: 0,
    }
}

fn counter(name: &str, v: i64) -> MetricPoint {
    point(name, MetricKind::Counter, MetricValue::I64 { value: v })
}

fn gauge(name: &str, v: f64) -> MetricPoint {
    point(name, MetricKind::Gauge, MetricValue::F64 { value: v })
}

fn histogram(name: &str, obs: Vec<f64>) -> MetricPoint {
    let sum = obs.iter().sum();
    point(
        name,
        MetricKind::Histogram,
        MetricValue::Histogram {
            count: obs.len() as u64,
            sum,
            observations: obs,
        },
    )
}

#[test]
fn manifest_is_correct() {
    use mcpg_plugin_protocol::PluginClass;
    use mcpg_plugin_protocol::capability::Capability;
    let p = build(json!({ "destination": { "kind": "stdout" } }));
    let m = SyncMetricsSink::manifest(&p);
    assert_eq!(m.id, PLUGIN_ID);
    assert_eq!(m.plugin_class, PluginClass::MetricsSink);
    assert!(
        m.required_capabilities
            .iter()
            .any(|c| matches!(c, Capability::NetworkOutbound))
    );
}

#[test]
fn counter_render() {
    let p = build(json!({ "destination": { "kind": "stdout" } }));
    assert_eq!(p.render(&counter("reqs", 5)), "reqs:5|c");
}

#[test]
fn gauge_render() {
    let p = build(json!({ "destination": { "kind": "stdout" } }));
    assert_eq!(p.render(&gauge("temp", 3.5)), "temp:3.5|g");
}

#[test]
fn histogram_render_one_line_per_observation() {
    let p = build(json!({ "destination": { "kind": "stdout" } }));
    assert_eq!(
        p.render(&histogram("lat", vec![1.0, 2.0])),
        "lat:1|h\nlat:2|h"
    );
}

#[test]
fn histogram_empty_observations_uses_sum() {
    let p = build(json!({ "destination": { "kind": "stdout" } }));
    let h = point(
        "lat",
        MetricKind::Histogram,
        MetricValue::Histogram {
            count: 0,
            sum: 10.0,
            observations: vec![],
        },
    );
    assert_eq!(p.render(&h), "lat:10|h");
}

#[test]
fn timing_histogram_type() {
    let p = build(json!({ "destination": { "kind": "stdout" }, "histogram_type": "timing" }));
    assert_eq!(p.render(&histogram("lat", vec![7.0])), "lat:7|ms");
}

#[test]
fn tags_appended_when_enabled() {
    let p = build(json!({ "destination": { "kind": "stdout" } }));
    let mut m = counter("reqs", 1);
    m.labels.insert("env".into(), "prod".into());
    m.labels.insert("svc".into(), "gw".into());
    assert_eq!(p.render(&m), "reqs:1|c|#env:prod,svc:gw");
}

#[test]
fn tags_omitted_when_disabled() {
    let p = build(json!({ "destination": { "kind": "stdout" }, "emit_tags": false }));
    let mut m = counter("reqs", 1);
    m.labels.insert("env".into(), "prod".into());
    assert_eq!(p.render(&m), "reqs:1|c");
}

#[test]
fn prefix_prepended() {
    let p = build(json!({ "destination": { "kind": "stdout" }, "prefix": "mcpg" }));
    assert_eq!(p.render(&counter("reqs", 5)), "mcpg.reqs:5|c");
}

#[test]
fn metasyntax_in_name_is_sanitized() {
    let p = build(json!({ "destination": { "kind": "stdout" } }));
    // ':' and '|' would corrupt the line — replaced with '_'.
    assert_eq!(p.render(&counter("a:b|c", 1)), "a_b_c:1|c");
}

#[test]
fn udp_loopback_emit_delivers_line() {
    let listener = UdpSocket::bind("127.0.0.1:0").unwrap();
    listener
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let p = build(json!({ "destination": { "kind": "udp", "address": addr } }));
    let m = counter("reqs", 42);
    let expected = p.render(&m);
    p.emit(&m);

    let mut buf = [0u8; 4096];
    let (n, _src) = listener
        .recv_from(&mut buf)
        .expect("datagram should arrive");
    assert_eq!(&buf[..n], expected.as_bytes());
}

#[test]
fn stdout_emit_does_not_panic() {
    let p = build(json!({ "destination": { "kind": "stderr" } }));
    p.emit(&counter("reqs", 1));
}

#[test]
#[should_panic(expected = "config JSON failed to parse")]
fn unknown_field_panics() {
    build(json!({ "destination": { "kind": "stdout" }, "bogus": 1 }));
}

#[test]
#[should_panic(expected = "config JSON failed to parse")]
fn malformed_config_panics() {
    StatsdSink::from_config_json("{ not json");
}

#[test]
#[should_panic(expected = "address must not be empty")]
fn empty_udp_address_panics() {
    build(json!({ "destination": { "kind": "udp", "address": "" } }));
}
