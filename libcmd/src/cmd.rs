use std::{
    ops::ControlFlow,
    path::Path,
    process::{ExitStatus, Stdio},
    sync::Arc,
};

use liberror::AnyError;
use libring::RingBuffer;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, Lines},
    process::Command,
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;
use tracing::Level;
use valuable::Valuable;

use crate::{CommandExitCode, read::reader_or_never, write::writer_or_never};

#[derive(Debug, Clone, Serialize, Deserialize, Valuable, Error)]
pub enum CommandError {
    #[error("Command execution cancelled")]
    Cancelled,

    #[error("Failed to spawn command process: {inner_error}")]
    BadSpawn { inner_error: AnyError },

    #[error("Command process exited with error: {inner_error}")]
    BadExit { inner_error: AnyError },

    #[error("Failed to acquire permit for command execution: {inner_error}")]
    Acquire { inner_error: AnyError },
}

#[derive(Clone)]
pub struct CommandExitWorking {
    pub stdout_lines: RingBuffer<String>,
    pub stderr_lines: RingBuffer<String>,
    pub exit_code: Option<CommandExitCode>,
}
#[derive(Debug, Clone, Serialize, Deserialize, Valuable)]
pub struct CommandExit {
    pub stdout_lines: Vec<String>,
    pub stderr_lines: Vec<String>,
    pub exit_code: Option<CommandExitCode>,
}
impl From<CommandExitWorking> for CommandExit {
    fn from(value: CommandExitWorking) -> Self {
        Self {
            stdout_lines: value.stdout_lines.to_vec(),
            stderr_lines: value.stderr_lines.to_vec(),
            exit_code: value.exit_code,
        }
    }
}

#[derive(Clone, Debug, Valuable)]
pub struct CommandMonitorServer {
    #[valuable(skip)]
    stdout_tx: Arc<tokio::sync::mpsc::Sender<String>>,
    #[valuable(skip)]
    stderr_tx: Arc<tokio::sync::mpsc::Sender<String>>,
    #[valuable(skip)]
    stdin_rx: Arc<Mutex<tokio::sync::mpsc::Receiver<String>>>,
}
impl CommandMonitorServer {
    fn new(
        stdout_tx: tokio::sync::mpsc::Sender<String>,
        stderr_tx: tokio::sync::mpsc::Sender<String>,
        stdin_rx: tokio::sync::mpsc::Receiver<String>,
    ) -> Self {
        Self {
            stdout_tx: Arc::new(stdout_tx),
            stderr_tx: Arc::new(stderr_tx),
            stdin_rx: Arc::new(Mutex::new(stdin_rx)),
        }
    }
}

#[derive(Debug)]
pub enum CommandMonitorMessage {
    Stdout { line: String },
    Stderr { line: String },
}
#[derive(Clone, Debug, Valuable)]
pub struct CommandMonitorClient {
    #[valuable(skip)]
    stdout_rx: Arc<Mutex<tokio::sync::mpsc::Receiver<String>>>,
    #[valuable(skip)]
    stderr_rx: Arc<Mutex<tokio::sync::mpsc::Receiver<String>>>,
    #[valuable(skip)]
    stdin_tx: Arc<tokio::sync::mpsc::Sender<String>>,
}

impl CommandMonitorClient {
    pub async fn recv(&mut self) -> Option<CommandMonitorMessage> {
        tokio::select! {
            delivery = async { self.stdout_rx.lock().await.recv().await } => {
                delivery.map(|line| CommandMonitorMessage::Stdout { line })
            }
            delivery = async { self.stderr_rx.lock().await.recv().await } => {
                delivery.map(|line| CommandMonitorMessage::Stderr { line })
            }
        }
    }

    /// Send a line (without newlines) to the process' stdin
    pub async fn send(&self, line: &str) {
        // TODO: Handle closed channel
        let _ = self.stdin_tx.send(line.to_string()).await;
    }

    fn new(
        stdout_rx: tokio::sync::mpsc::Receiver<String>,
        stderr_rx: tokio::sync::mpsc::Receiver<String>,
        stdin_tx: tokio::sync::mpsc::Sender<String>,
    ) -> Self {
        Self {
            stdout_rx: Arc::new(Mutex::new(stdout_rx)),
            stderr_rx: Arc::new(Mutex::new(stderr_rx)),
            stdin_tx: Arc::new(stdin_tx),
        }
    }
}

pub struct CommandMonitor {
    pub server: CommandMonitorServer,
    pub client: CommandMonitorClient,
}
impl Default for CommandMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandMonitor {
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let (stdout_tx, stdout_rx) = tokio::sync::mpsc::channel(capacity);
        let (stderr_tx, stderr_rx) = tokio::sync::mpsc::channel(capacity);
        let (stdin_tx, stdin_rx) = tokio::sync::mpsc::channel(capacity);

        let server = CommandMonitorServer::new(stdout_tx, stderr_tx, stdin_rx);
        let client = CommandMonitorClient::new(stdout_rx, stderr_rx, stdin_tx);

        Self { server, client }
    }
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(100)
    }
}

#[derive(Valuable)]
struct CommandContext<
    StdoutReader: AsyncBufRead + Unpin + Send,
    StderrReader: AsyncBufRead + Unpin + Send,
    StdinWriter: AsyncWrite + Unpin + Send,
> {
    #[valuable(skip)]
    child: tokio::process::Child,
    #[valuable(skip)]
    stdout: Lines<StdoutReader>,
    #[valuable(skip)]
    stderr: Lines<StderrReader>,
    #[valuable(skip)]
    stdin: StdinWriter,
    #[valuable(skip)]
    cancellation_token: CancellationToken,

    server: Option<CommandMonitorServer>,
    #[valuable(skip)]
    result: CommandExitWorking,
}
impl<
    StdoutReader: AsyncBufRead + Unpin + Send,
    StderrReader: AsyncBufRead + Unpin + Send,
    StdinWriter: AsyncWrite + Unpin + Send,
> CommandContext<StdoutReader, StderrReader, StdinWriter>
{
    fn new(
        child: tokio::process::Child,
        server: Option<CommandMonitorServer>,
        stdout: Lines<StdoutReader>,
        stderr: Lines<StderrReader>,
        stdin: StdinWriter,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            child,
            stdout,
            stderr,
            server,
            stdin,
            cancellation_token,

            result: CommandExitWorking {
                stdout_lines: RingBuffer::new(100),
                stderr_lines: RingBuffer::new(100),
                exit_code: None,
            },
        }
    }

    #[tracing::instrument(level=Level::DEBUG, "command_context::on_exited", skip(self))]
    fn on_exited(
        &mut self,
        exit_result: Result<ExitStatus, std::io::Error>,
    ) -> ControlFlow<Result<CommandExitWorking, CommandError>> {
        match exit_result {
            Ok(status) => {
                self.result.exit_code = Some(status.into());
                if status.success() {
                    tracing::debug!(
                        exit_code = ?status.code(),
                        stdout_lines = self.result.stdout_lines.len(),
                        stderr_lines = self.result.stderr_lines.len(),
                        "Command process completed successfully"
                    );
                } else {
                    tracing::error!(
                        exit_code = ?status.code(),
                        stdout_lines = self.result.stdout_lines.len(),
                        stderr_lines = self.result.stderr_lines.len(),
                        stderr = ?self.result.stderr_lines,
                        "Command process completed with non-zero exit code"
                    );
                }
                ControlFlow::Break(Ok(self.result.clone()))
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "Failed to wait for command process"
                );
                ControlFlow::Break(Err(CommandError::BadExit {
                    inner_error: e.into(),
                }))
            }
        }
    }

    #[tracing::instrument(level=Level::DEBUG, "command_context::on_cancelled", skip(self))]
    async fn on_cancelled(&mut self) -> ControlFlow<Result<CommandExit, CommandError>> {
        tracing::warn!("Cancellation requested, terminating command process");

        if let Err(e) = self.child.kill().await {
            tracing::error!(
                error = %e,
                "Failed to kill command process during cancellation"
            );
        } else {
            tracing::debug!("Successfully killed command process");
        }

        ControlFlow::Break(Err(CommandError::Cancelled))
    }

    #[tracing::instrument(level=Level::TRACE, "command_context::on_stdout_line", skip(self, line), fields(line_preview = %line.chars().take(100).collect::<String>()))]
    async fn on_stdout_line(
        &mut self,
        line: String,
    ) -> ControlFlow<Result<CommandExit, CommandError>> {
        self.result.stdout_lines.push(line.clone());
        tracing::trace!(line = %line, "Command wrote to stdout");

        let Some(server) = self.server.clone() else {
            return ControlFlow::Continue(());
        };

        if let Err(e) = server.stdout_tx.send(line.clone()).await {
            tracing::error!(
                error = %e,
                line = %line,
                "Failed to send stdout line to monitor channel"
            );
        }

        ControlFlow::Continue(())
    }

    #[tracing::instrument(level=Level::DEBUG, "command_context::on_stderr_line", skip(self, line), fields(line_preview = %line.chars().take(100).collect::<String>()))]
    async fn on_stderr_line(
        &mut self,
        line: String,
    ) -> ControlFlow<Result<CommandExit, CommandError>> {
        self.result.stderr_lines.push(line.clone());
        tracing::debug!(line = %line, "Command wrote to stderr");

        let Some(server) = self.server.clone() else {
            return ControlFlow::Continue(());
        };

        if let Err(e) = server.stderr_tx.send(line.clone()).await {
            tracing::error!(
                error = %e,
                line = %line,
                "Failed to send stderr line to monitor channel"
            );
        }

        ControlFlow::Continue(())
    }

    #[tracing::instrument(level=Level::DEBUG, "command_context::tick", skip(self))]
    async fn tick(&mut self) -> ControlFlow<Result<CommandExit, CommandError>> {
        tokio::select! {
            exit_result = self.child.wait() => return self.on_exited(exit_result).map_break(|r| r.map(CommandExit::from)),
            () = self.cancellation_token.cancelled() => return self.on_cancelled().await,
            Ok(Some(line)) = self.stdout.next_line() => return self.on_stdout_line(line).await,
            Ok(Some(line)) = self.stderr.next_line() => return self.on_stderr_line(line).await,
            Some(stdin_line) = async {
                match &self.server {
                    Some(server) => server.stdin_rx.lock().await.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                // Handle stdin input
                if let Err(e) = self.stdin.write_all(stdin_line.trim().as_bytes()).await {
                    tracing::error!(error = %e, "Failed to write to stdin");
                }
                if let Err(e) = self.stdin.write_all(b"\n").await {
                    tracing::error!(error = %e, "Failed to write newline to stdin");
                }
                if let Err(e) = self.stdin.flush().await {
                    tracing::error!(error = %e, "Failed to flush stdin");
                }

                ControlFlow::Continue(())
            }
        }
    }
}

#[tracing::instrument("libcmd::run", skip(prepare, command, server, cancellation_token), fields(command_path = %command.as_ref().display()))]
pub async fn run<Cmd: AsRef<Path>, Prepare>(
    command: Cmd,
    server: Option<CommandMonitorServer>,
    cancellation_token: CancellationToken,
    prepare: Prepare,
) -> Result<CommandExit, CommandError>
where
    Prepare: FnOnce(&mut Command),
{
    tracing::debug!(
        command_path = %command.as_ref().display(),
        has_monitor = server.is_some(),
        "Preparing to execute command"
    );

    let mut cmd = Command::new(command.as_ref());

    prepare(&mut cmd);

    tracing::info!(
        command_path = %command.as_ref().display(),
        args = ?cmd.as_std().get_args().collect::<Vec<_>>(),
        "Executing command"
    );

    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    cmd.stdin(Stdio::piped());

    let mut child = cmd
        .spawn()
        .map_err(|e| {
            tracing::error!(
                command_path = %command.as_ref().display(),
                error = %e,
                "Failed to spawn command process"
            );
            CommandError::BadSpawn {
                inner_error: e.into(),
            }
        })
        .inspect(|child| {
            tracing::debug!(
                pid = ?child.id(),
                "Command process spawned successfully"
            );
        })?;

    let stdout = reader_or_never(child.stdout.take());
    let stdout = BufReader::new(stdout).lines();

    let stderr = reader_or_never(child.stderr.take());
    let stderr = BufReader::new(stderr).lines();

    let stdin = writer_or_never(child.stdin.take());

    tracing::trace!("Starting command event loop");

    let mut context = CommandContext::new(child, server, stdout, stderr, stdin, cancellation_token);

    loop {
        if let ControlFlow::Break(result) = context.tick().await {
            return result
                .inspect(|exit| {
                    tracing::debug!(
                        exit_code = ?exit.exit_code,
                        "Command execution completed"
                    );
                })
                .inspect_err(|e| {
                    tracing::error!(
                        error = %e,
                        "Command execution failed"
                    );
                });
        }
    }
}
