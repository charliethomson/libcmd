use serde::{Deserialize, Serialize};
use valuable::Valuable;

#[derive(Debug, Clone, Serialize, Deserialize, Valuable)]
pub struct CommandExitCode {
    pub success: bool,
    pub code: Option<i32>,
    pub signal: Option<i32>,
    pub core_dumped: Option<bool>,
    pub stopped_signal: Option<i32>,
    pub continued: Option<bool>,
}
impl From<std::process::ExitStatus> for CommandExitCode {
    fn from(value: std::process::ExitStatus) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            Self {
                success: value.success(),
                code: value.code(),
                signal: value.signal(),
                core_dumped: Some(value.core_dumped()),
                stopped_signal: value.stopped_signal(),
                continued: Some(value.continued()),
            }
        }
        #[cfg(not(unix))]
        {
            return Self {
                success: value.success(),
                code: value.code(),
                signal: None,
                core_dumped: None,
                stopped_signal: None,
                continued: None,
            };
        }
    }
}
