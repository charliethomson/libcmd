//! `run` must not put the command's argv in its logs by default: argv
//! routinely carries signed input URLs.

use std::{
    io::Write,
    sync::{Arc, Mutex},
};

use libcmd::{ArgsDisplay, RunOptions, run, run_with_options};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::fmt::MakeWriter;

const SECRET_URL: &str = "https://cdn.example.invalid/stream.flv?expire=1&sign=deadbeef";

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Capture {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Capture {
    type Writer = Capture;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Capture everything up to DEBUG (the ring and per-line events sit at
/// DEBUG/TRACE and are not exported by default).
fn capture(level: tracing::Level) -> (Capture, tracing::subscriber::DefaultGuard) {
    let capture = Capture::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(level)
        .with_writer(capture.clone())
        .with_ansi(false)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (capture, guard)
}

#[cfg(unix)]
#[tokio::test]
async fn default_run_logs_arg_count_not_args() {
    let (capture, _guard) = capture(tracing::Level::DEBUG);

    let exit = run("echo", None, CancellationToken::new(), |cmd| {
        cmd.arg(SECRET_URL);
    })
    .await
    .unwrap();
    assert_eq!(exit.stdout_lines, vec![SECRET_URL.to_string()]);

    let logs = capture.text();
    assert!(logs.contains("Executing command"), "{logs}");
    assert!(logs.contains("arg_count=1"), "{logs}");
    assert!(logs.contains("libcmd.run"), "{logs}");
    assert!(!logs.contains("cdn.example.invalid"), "{logs}");
}

#[cfg(unix)]
#[tokio::test]
async fn non_zero_exit_is_info_and_keeps_stderr_off_info() {
    let (capture, _guard) = capture(tracing::Level::INFO);

    let exit = run("sh", None, CancellationToken::new(), |cmd| {
        cmd.arg("-c")
            .arg(format!("echo 'Input from {SECRET_URL}' >&2; exit 3"));
    })
    .await
    .unwrap();
    assert_eq!(exit.exit_code.unwrap().code, Some(3));

    let logs = capture.text();
    assert!(
        logs.contains("INFO") && logs.contains("non-zero exit code"),
        "{logs}"
    );
    assert!(!logs.contains("ERROR"), "{logs}");
    assert!(!logs.contains("WARN"), "{logs}");
    assert!(!logs.contains("cdn.example.invalid"), "{logs}");
}

#[cfg(unix)]
#[tokio::test]
async fn redactor_opt_in_controls_rendered_args() {
    let (capture, _guard) = capture(tracing::Level::INFO);

    let options = RunOptions::new().args_display(ArgsDisplay::redacted(|args| {
        args.iter()
            .map(|a| {
                let a = a.to_string_lossy();
                if a.starts_with("http") {
                    "<url>".to_string()
                } else {
                    a.into_owned()
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }));
    run_with_options("echo", None, CancellationToken::new(), options, |cmd| {
        cmd.arg("-n").arg(SECRET_URL);
    })
    .await
    .unwrap();

    let logs = capture.text();
    assert!(logs.contains("args=-n <url>"), "{logs}");
    assert!(!logs.contains("cdn.example.invalid"), "{logs}");
}
