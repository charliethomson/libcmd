//! `libcmd.run` records `arg_count` / `pid` / `exit_code` on itself only.
//!
//! It is a DEBUG span, so under an INFO filter it doesn't exist and
//! `Span::current()` inside `run` is the *caller's* span. Recording through
//! `Span::current()` would then write libcmd's values onto any caller span
//! that happens to declare the same field names.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use libcmd::{CommandError, run};
use tokio_util::sync::CancellationToken;
use tracing::{
    Instrument, Subscriber,
    field::{Field, Visit},
    span::{Id, Record},
};
use tracing_subscriber::{
    Layer,
    filter::LevelFilter,
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
};

/// Every `(span name, field name)` pair recorded after span creation, plus
/// the visitor method each value arrived through.
#[derive(Clone, Default)]
struct Recorded(Arc<Mutex<Vec<(String, String, Kind)>>>);

/// Which `Visit` method a value arrived through. tracing-opentelemetry has
/// no `record_u64`, so an unsigned value reaches the exporter as a string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    I64,
    U64,
    Other,
}

impl Recorded {
    fn on(&self, span: &str) -> Vec<String> {
        let mut fields: Vec<String> = self
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|(s, _, _)| s == span)
            .map(|(_, f, _)| f.clone())
            .collect();
        fields.sort();
        fields
    }

    fn kinds(&self, span: &str) -> Vec<(String, Kind)> {
        let mut fields: Vec<(String, Kind)> = self
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|(s, _, _)| s == span)
            .map(|(_, f, k)| (f.clone(), *k))
            .collect();
        fields.sort_by(|a, b| a.0.cmp(&b.0));
        fields
    }
}

struct FieldNames(Vec<(String, Kind)>);

impl Visit for FieldNames {
    fn record_i64(&mut self, field: &Field, _: i64) {
        self.0.push((field.name().to_string(), Kind::I64));
    }

    fn record_u64(&mut self, field: &Field, _: u64) {
        self.0.push((field.name().to_string(), Kind::U64));
    }

    fn record_debug(&mut self, field: &Field, _: &dyn std::fmt::Debug) {
        self.0.push((field.name().to_string(), Kind::Other));
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Recorded {
    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let name = ctx.span(id).expect("span exists").name().to_string();
        let mut fields = FieldNames(Vec::new());
        values.record(&mut fields);
        let mut out = self.0.lock().unwrap();
        out.extend(fields.0.into_iter().map(|(f, k)| (name.clone(), f, k)));
    }
}

fn subscriber(level: LevelFilter) -> (Recorded, tracing::subscriber::DefaultGuard) {
    let recorded = Recorded::default();
    let subscriber = tracing_subscriber::registry()
        .with(level)
        .with(recorded.clone());
    (recorded, tracing::subscriber::set_default(subscriber))
}

/// A caller span that happens to declare libcmd's field names.
fn caller_span() -> tracing::Span {
    tracing::info_span!(
        "caller",
        arg_count = tracing::field::Empty,
        pid = tracing::field::Empty,
        exit_code = tracing::field::Empty,
    )
}

async fn exit_3() {
    let exit = run("sh", None, CancellationToken::new(), |cmd| {
        cmd.arg("-c").arg("exit 3");
    })
    .instrument(caller_span())
    .await
    .unwrap();
    assert_eq!(exit.exit_code.unwrap().code, Some(3));
}

#[cfg(unix)]
#[tokio::test]
async fn filtered_out_run_span_leaves_the_callers_fields_alone() {
    let (recorded, _guard) = subscriber(LevelFilter::INFO);
    exit_3().await;
    assert_eq!(recorded.on("caller"), Vec::<String>::new());
    assert_eq!(recorded.on("libcmd.run"), Vec::<String>::new());
}

#[cfg(unix)]
#[tokio::test]
async fn enabled_run_span_records_on_itself() {
    let (recorded, _guard) = subscriber(LevelFilter::DEBUG);
    exit_3().await;
    assert_eq!(recorded.on("caller"), Vec::<String>::new());
    assert_eq!(recorded.on("libcmd.run"), ["arg_count", "exit_code", "pid"]);
}

/// Span numbers are i64 so they export as OTLP ints, not strings (TTR-64).
#[cfg(unix)]
#[tokio::test]
async fn run_span_numbers_are_i64() {
    let (recorded, _guard) = subscriber(LevelFilter::DEBUG);
    exit_3().await;
    assert_eq!(
        recorded.kinds("libcmd.run"),
        [
            ("arg_count".to_string(), Kind::I64),
            ("exit_code".to_string(), Kind::I64),
            ("pid".to_string(), Kind::I64),
        ]
    );
}

/// The event loop's arms return their value as the `select!`'s tail
/// expression; cancellation must still end the run promptly.
#[cfg(unix)]
#[tokio::test]
async fn cancellation_ends_a_running_command() {
    let token = CancellationToken::new();
    let cancel = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.cancel();
    });
    let started = Instant::now();
    let result = run("sleep", None, token, |cmd| {
        cmd.arg("30");
    })
    .await;
    assert!(matches!(result, Err(CommandError::Cancelled)), "{result:?}");
    assert!(started.elapsed() < Duration::from_secs(10));
}

/// Output lines keep the loop going (`Continue`) until the exit arm breaks it.
#[cfg(unix)]
#[tokio::test]
async fn output_lines_are_collected_before_exit() {
    let exit = run("sh", None, CancellationToken::new(), |cmd| {
        cmd.arg("-c").arg("echo one; echo two >&2; echo three");
    })
    .await
    .unwrap();
    assert_eq!(exit.stdout_lines, ["one", "three"]);
    assert_eq!(exit.stderr_lines, ["two"]);
    assert_eq!(exit.exit_code.unwrap().code, Some(0));
}
