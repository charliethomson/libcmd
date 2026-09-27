use std::{ffi::OsStr, fmt, sync::Arc};

/// Renders a command's argument list for the `Executing command` log.
pub type ArgsRedactor = Arc<dyn Fn(&[&OsStr]) -> String + Send + Sync>;

/// How [`crate::run_with_options`] renders the command's arguments when it
/// logs `Executing command`.
///
/// Argument lists routinely carry input URLs (signed CDN links, tokens), so the
/// default is [`ArgsDisplay::Hidden`]: only the program and the argument count
/// are logged.
#[derive(Clone, Default)]
pub enum ArgsDisplay {
    /// Log only the argument count.
    #[default]
    Hidden,
    /// Log every argument verbatim. Only for callers whose arguments are known
    /// to be free of URLs, tokens and user input.
    Full,
    /// Log the string the caller's redactor returns for the argument list.
    Redacted(ArgsRedactor),
}

impl ArgsDisplay {
    /// Convenience constructor for [`ArgsDisplay::Redacted`].
    pub fn redacted<F>(redactor: F) -> Self
    where
        F: Fn(&[&OsStr]) -> String + Send + Sync + 'static,
    {
        Self::Redacted(Arc::new(redactor))
    }

    pub(crate) fn render(&self, args: &[&OsStr]) -> Option<String> {
        match self {
            Self::Hidden => None,
            Self::Full => Some(format!("{args:?}")),
            Self::Redacted(redactor) => Some(redactor(args)),
        }
    }
}

impl fmt::Debug for ArgsDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hidden => f.write_str("Hidden"),
            Self::Full => f.write_str("Full"),
            Self::Redacted(_) => f.write_str("Redacted(..)"),
        }
    }
}

/// Options for [`crate::run_with_options`]. [`crate::run`] uses the defaults.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct RunOptions {
    pub args_display: ArgsDisplay,
}

impl RunOptions {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set how the arguments are rendered in the `Executing command` log.
    #[must_use]
    pub fn args_display(mut self, args_display: ArgsDisplay) -> Self {
        self.args_display = args_display;
        self
    }
}
