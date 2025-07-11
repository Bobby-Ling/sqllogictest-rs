//! Sqllogictest runner.

use std::collections::{BTreeMap, HashSet};
use std::fmt::{Debug, Display};
use std::path::Path;
use std::process::{Command, ExitStatus, Output};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use std::vec;

use async_trait::async_trait;
use futures::executor::block_on;
use futures::{stream, Future, FutureExt, StreamExt};
use itertools::Itertools;
use md5::Digest;
use owo_colors::OwoColorize;
use rand::Rng;
use similar::{Change, ChangeTag, TextDiff};
use tempfile::TempDir;

use crate::parser::*;
use crate::substitution::Substitution;
use crate::{ColumnType, Connections, MakeConnection};

/// Type-erased error type.
type AnyError = Arc<dyn std::error::Error + Send + Sync>;

/// Output of a record.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RecordOutput<T: ColumnType> {
    /// No output. Occurs when the record is skipped or not a `query`, `statement`, or `system`
    /// command.
    Nothing,
    /// The output of a `query`.
    Query {
        types: Vec<T>,
        rows: Vec<Vec<String>>,
        error: Option<AnyError>,
    },
    /// The output of a `statement`.
    Statement { count: u64, error: Option<AnyError> },
    /// The output of a `system` command.
    #[non_exhaustive]
    System {
        stdout: Option<String>,
        error: Option<AnyError>,
    },
}

#[non_exhaustive]
pub enum DBOutput<T: ColumnType> {
    Rows {
        types: Vec<T>,
        rows: Vec<Vec<String>>,
    },
    /// A statement in the query has completed.
    ///
    /// The number of rows modified or selected is returned.
    ///
    /// If the test case doesn't specify `statement count <n>`, the number is simply ignored.
    StatementComplete(u64),
}

/// The async database to be tested.
#[async_trait]
pub trait AsyncDB {
    /// The error type of SQL execution.
    type Error: std::error::Error + Send + Sync + 'static;
    /// The type of result columns
    type ColumnType: ColumnType;

    /// Async run a SQL query and return the output.
    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error>;

    /// Shutdown the connection gracefully.
    async fn shutdown(&mut self);

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        ""
    }

    /// [`Runner`] calls this function to perform sleep.
    ///
    /// The default implementation is `std::thread::sleep`, which is universal to any async runtime
    /// but would block the current thread. If you are running in tokio runtime, you should override
    /// this by `tokio::time::sleep`.
    async fn sleep(dur: Duration) {
        std::thread::sleep(dur);
    }

    /// [`Runner`] calls this function to run a system command.
    ///
    /// The default implementation is `std::process::Command::output`, which is universal to any
    /// async runtime but would block the current thread. If you are running in tokio runtime, you
    /// should override this by `tokio::process::Command::output`.
    async fn run_command(mut command: Command) -> std::io::Result<std::process::Output> {
        command.output()
    }
}

/// The database to be tested.
pub trait DB {
    /// The error type of SQL execution.
    type Error: std::error::Error + Send + Sync + 'static;
    /// The type of result columns
    type ColumnType: ColumnType;

    /// Run a SQL query and return the output.
    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error>;

    /// Shutdown the connection gracefully.
    fn shutdown(&mut self) {}

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        ""
    }
}

/// Compat-layer for the new AsyncDB and DB trait
#[async_trait]
impl<D> AsyncDB for D
where
    D: DB + Send,
{
    type Error = D::Error;
    type ColumnType = D::ColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        D::run(self, sql)
    }

    async fn shutdown(&mut self) {
        D::shutdown(self);
    }

    fn engine_name(&self) -> &str {
        D::engine_name(self)
    }
}

/// The error type for running sqllogictest.
///
/// For colored error message, use `self.display()`.
#[derive(thiserror::Error, Clone)]
#[error("{kind}\nat {loc}\n")]
pub struct TestError {
    kind: TestErrorKind,
    loc: Location,
}

impl TestError {
    pub fn display(&self, colorize: bool, verbose: bool) -> TestErrorDisplay<'_> {
        TestErrorDisplay {
            err: self,
            colorize,
            verbose: verbose,
        }
    }
}

/// Overrides the `Display` implementation of [`TestError`] to support controlling colorization.
pub struct TestErrorDisplay<'a> {
    err: &'a TestError,
    colorize: bool,
    verbose: bool,
}

impl Display for TestErrorDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}\nat {}\n",
            self.err.kind.display(self.colorize, self.verbose),
            self.err.loc
        )
    }
}

/// For colored error message, use `self.display()`.
#[derive(Clone, Debug, thiserror::Error)]
pub struct ParallelTestError {
    errors: Vec<TestError>,
}

impl Display for ParallelTestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "parallel test failed")?;
        write!(f, "Caused by:")?;
        for i in &self.errors {
            writeln!(f, "{i}")?;
        }
        Ok(())
    }
}

/// Overrides the `Display` implementation of [`ParallelTestError`] to support controlling
/// colorization.
pub struct ParallelTestErrorDisplay<'a> {
    err: &'a ParallelTestError,
    colorize: bool,
    verbose: bool,
}

impl Display for ParallelTestErrorDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "parallel test failed")?;
        write!(f, "Caused by:")?;
        for i in &self.err.errors {
            writeln!(f, "{}", i.display(self.colorize, self.verbose))?;
        }
        Ok(())
    }
}

impl ParallelTestError {
    pub fn display(&self, colorize: bool, verbose: bool) -> ParallelTestErrorDisplay<'_> {
        ParallelTestErrorDisplay {
            err: self,
            colorize,
            verbose: verbose,
        }
    }
}

impl std::fmt::Debug for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl TestError {
    /// Returns the corresponding [`TestErrorKind`] for this error.
    pub fn kind(&self) -> TestErrorKind {
        self.kind.clone()
    }

    /// Returns the location from which the error originated.
    pub fn location(&self) -> Location {
        self.loc.clone()
    }
}

#[derive(Debug, Clone)]
pub enum RecordKind {
    Statement,
    Query,
}

impl std::fmt::Display for RecordKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordKind::Statement => write!(f, "statement"),
            RecordKind::Query => write!(f, "query"),
        }
    }
}

/// The error kind for running sqllogictest.
///
/// For colored error message, use `self.display()`.
#[derive(thiserror::Error, Debug, Clone)]
#[non_exhaustive]
pub enum TestErrorKind {
    #[error("parse error: {0}")]
    ParseError(ParseErrorKind),
    #[error("{kind} is expected to fail, but actually succeed:\n[SQL] {sql}")]
    Ok { sql: String, kind: RecordKind },
    #[error("{kind} failed: {err}\n[SQL] {sql}")]
    Fail {
        sql: String,
        err: AnyError,
        kind: RecordKind,
    },
    #[error("system command failed: {err}\n[CMD] {command}")]
    SystemFail { command: String, err: AnyError },
    #[error(
        "system command stdout mismatch:\n[command] {command}\n[Diff] (-expected|+actual)\n{}",
        TextDiff::from_lines(.expected_stdout, .actual_stdout).iter_all_changes().format_with("\n", |diff, f| format_diff(&diff, f, false))
    )]
    SystemStdoutMismatch {
        command: String,
        expected_stdout: String,
        actual_stdout: String,
    },
    // Remember to also update [`TestErrorKindDisplay`] if this message is changed.
    #[error("{kind} is expected to fail with error:\n\t{expected_err}\nbut got error:\n\t{err}\n[SQL] {sql}")]
    ErrorMismatch {
        sql: String,
        err: AnyError,
        expected_err: String,
        kind: RecordKind,
    },
    #[error("statement is expected to affect {expected} rows, but actually {actual}\n[SQL] {sql}")]
    StatementResultMismatch {
        sql: String,
        expected: u64,
        actual: String,
    },
    // Remember to also update [`TestErrorKindDisplay`] if this message is changed.
    #[error(
        "query result mismatch:\n[SQL] {sql}\n[Diff] (-expected|+actual)\n{}",
        TextDiff::from_lines(.expected, .actual).iter_all_changes().format_with("\n", |diff, f| format_diff(&diff, f, false))
    )]
    QueryResultMismatch {
        sql: String,
        expected: String,
        actual: String,
    },
    #[error(
        "query columns mismatch:\n[SQL] {sql}\n{}",
        format_column_diff(expected, actual, false)
    )]
    QueryResultColumnsMismatch {
        sql: String,
        expected: String,
        actual: String,
    },
}

impl From<ParseError> for TestError {
    fn from(e: ParseError) -> Self {
        TestError {
            kind: TestErrorKind::ParseError(e.kind()),
            loc: e.location(),
        }
    }
}

impl TestErrorKind {
    fn at(self, loc: Location) -> TestError {
        TestError { kind: self, loc }
    }

    pub fn display(&self, colorize: bool, verbose: bool) -> TestErrorKindDisplay<'_> {
        TestErrorKindDisplay {
            error: self,
            colorize,
            verbose: verbose
        }
    }
}

/// Overrides the `Display` implementation of [`TestErrorKind`] to support controlling colorization.
pub struct TestErrorKindDisplay<'a> {
    error: &'a TestErrorKind,
    colorize: bool,
    verbose: bool,
}

impl Display for TestErrorKindDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.colorize {
            return write!(f, "{}", self.error);
        }
        match self.error {
            TestErrorKind::ErrorMismatch {
                sql,
                err,
                expected_err,
                kind,
            } => write!(
                f,
                "{kind} is expected to fail with error:\n\t{}\nbut got error:\n\t{}\n[SQL] {sql}",
                expected_err.bright_green(),
                err.bright_red(),
            ),
            TestErrorKind::QueryResultMismatch {
                sql,
                expected,
                actual,
            } => {
                if self.verbose {
                    write!(
                        f,
                        "query result mismatch:\n[SQL] {sql}\n[Expected]\n{expected}\n[Actual]\n{actual}\n[Diff] ({}|{})\n{}",
                        "-expected".bright_red(),
                        "+actual".bright_green(),
                        TextDiff::from_lines(expected, actual)
                            .iter_all_changes()
                            .format_with("\n", |diff, f| format_diff(&diff, f, true))
                    )
                } else {
                    write!(
                        f,
                        "query result mismatch:\n[SQL] {sql}\n[Diff] ({}|{})\n{}",
                        "-expected".bright_red(),
                        "+actual".bright_green(),
                        TextDiff::from_lines(expected, actual)
                            .iter_all_changes()
                            .format_with("\n", |diff, f| format_diff(&diff, f, true))
                    )
                }
            },
            TestErrorKind::QueryResultColumnsMismatch {
                sql,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "query columns mismatch:\n[SQL] {sql}\n{}",
                    format_column_diff(expected, actual, true)
                )
            }
            TestErrorKind::SystemStdoutMismatch {
                command,
                expected_stdout,
                actual_stdout,
            } => {
                write!(
                    f,
                    "system command stdout mismatch:\n[command] {command}\n[Diff] (-expected|+actual)\n{}",
                    TextDiff::from_lines(expected_stdout, actual_stdout)
                        .iter_all_changes()
                        .format_with("\n", |diff, f|{ format_diff(&diff, f, true)})
                )
            }
            _ => write!(f, "{}", self.error),
        }
    }
}

fn format_diff(
    diff: &Change<&str>,
    f: &mut dyn FnMut(&dyn std::fmt::Display) -> std::fmt::Result,
    colorize: bool,
) -> std::fmt::Result {
    match diff.tag() {
        ChangeTag::Equal => f(&diff
            .value()
            .lines()
            .format_with("\n", |line, f| f(&format_args!("    {line}")))),
        ChangeTag::Insert => f(&diff.value().lines().format_with("\n", |line, f| {
            if colorize {
                f(&format_args!("+   {line}").bright_green())
            } else {
                f(&format_args!("+   {line}"))
            }
        })),
        ChangeTag::Delete => f(&diff.value().lines().format_with("\n", |line, f| {
            if colorize {
                f(&format_args!("-   {line}").bright_red())
            } else {
                f(&format_args!("-   {line}"))
            }
        })),
    }
}

fn format_column_diff(expected: &str, actual: &str, colorize: bool) -> String {
    let (expected, actual) = TextDiff::from_chars(expected, actual)
        .iter_all_changes()
        .fold(
            ("".to_string(), "".to_string()),
            |(expected, actual), change| match change.tag() {
                ChangeTag::Equal => (
                    format!("{}{}", expected, change.value()),
                    format!("{}{}", actual, change.value()),
                ),
                ChangeTag::Delete => (
                    if colorize {
                        format!("{}[{}]", expected, change.value().bright_red())
                    } else {
                        format!("{}[{}]", expected, change.value())
                    },
                    actual,
                ),
                ChangeTag::Insert => (
                    expected,
                    if colorize {
                        format!("{}[{}]", actual, change.value().bright_green())
                    } else {
                        format!("{}[{}]", actual, change.value())
                    },
                ),
            },
        );
    format!("[Expected] {expected}\n[Actual  ] {actual}")
}

/// Normalizer will be used by [`Runner`] to normalize the result values
///
/// # Default
///
/// By default, the ([`default_normalizer`]) will be used to normalize values.
pub type Normalizer = fn(s: &String) -> String;

/// Trim and replace multiple whitespaces with one.
#[allow(clippy::ptr_arg)]
pub fn default_normalizer(s: &String) -> String {
    s.trim().split_ascii_whitespace().join(" ")
}

/// Validator will be used by [`Runner`] to validate the output.
///
/// # Default
///
/// By default, the ([`default_validator`]) will be used compare normalized results.
pub type Validator =
    fn(normalizer: Normalizer, actual: &[Vec<String>], expected: &[String]) -> bool;

/// Validator specifically for TextWise mode that compares raw text without normalization
pub fn textwise_validator(
    _normalizer: Normalizer,
    actual: &[Vec<String>],
    expected: &[String],
) -> bool {
    // For TextWise mode, we compare the raw text directly without any normalization
    let actual_rows: Vec<String> = actual
        .iter()
        .map(|strs| {
            if strs.len() == 1 {
                // For single-column output (like query plans), use the raw string directly
                strs[0].clone()
            } else {
                // For multi-column output, join with spaces
                strs.join(" ")
            }
        })
        .collect();

    actual_rows == expected
}

pub fn default_validator(
    normalizer: Normalizer,
    actual: &[Vec<String>],
    expected: &[String],
) -> bool {
    // Support ignore marker <slt:ignore> to skip volatile parts of output.
    const IGNORE_MARKER: &str = "<slt:ignore>";
    let contains_ignore_marker = expected.iter().any(|line| line.contains(IGNORE_MARKER));

    // Normalize expected lines.
    // If ignore marker present, perform fragment-based matching on the full snapshot.
    if contains_ignore_marker {
        // If ignore marker present, perform fragment-based matching on the full snapshot.
        // The actual results might contain \n, and may not be a normal "row", which is not suitable to normalize.
        let expected_results = expected;
        let actual_rows = actual
            .iter()
            .map(|strs| strs.iter().join(" "))
            .collect_vec();

        let expected_snapshot = expected_results.join("\n");
        let actual_snapshot = actual_rows.join("\n");
        let fragments: Vec<&str> = expected_snapshot.split(IGNORE_MARKER).collect();
        let mut pos = 0;
        for frag in fragments {
            if frag.is_empty() {
                continue;
            }
            if let Some(idx) = actual_snapshot[pos..].find(frag) {
                pos += idx + frag.len();
            } else {
                tracing::error!(
                    "mismatch at: {}\nexpected: {}\nactual: {}",
                    pos,
                    frag,
                    &actual_snapshot[pos..]
                );
                return false;
            }
        }
        return true;
    }

    let expected_results = expected.iter().map(normalizer).collect_vec();
    // Default, we compare normalized results. Whitespace characters are ignored.
    let normalized_rows = actual
        .iter()
        .map(|strs| strs.iter().map(normalizer).join(" "))
        .collect_vec();

    normalized_rows == expected_results
}

/// [`Runner`] uses this validator to check that the expected column types match an actual output.
///
/// # Default
///
/// By default ([`default_column_validator`]), columns are not validated.
pub type ColumnTypeValidator<T> = fn(actual: &Vec<T>, expected: &Vec<T>) -> bool;

/// The default validator always returns success for any inputs of expected and actual sets of
/// columns.
pub fn default_column_validator<T: ColumnType>(_: &Vec<T>, _: &Vec<T>) -> bool {
    true
}

/// The strict validator checks:
/// - the number of columns is as expected
/// - each column has the same type as expected
#[allow(clippy::ptr_arg)]
pub fn strict_column_validator<T: ColumnType>(actual: &Vec<T>, expected: &Vec<T>) -> bool {
    actual.len() == expected.len()
        && !actual
            .iter()
            .zip(expected.iter())
            .any(|(actual_column, expected_column)| actual_column != expected_column)
}

/// Decide whether a test file should be run. Useful for partitioning tests into multiple
/// parallel machines to speed up test runs.
pub trait Partitioner: Send + Sync + 'static {
    /// Returns true if the given file name matches the partition and should be run.
    fn matches(&self, file_name: &str) -> bool;
}

impl<F> Partitioner for F
where
    F: Fn(&str) -> bool + Send + Sync + 'static,
{
    fn matches(&self, file_name: &str) -> bool {
        self(file_name)
    }
}

/// The default partitioner matches all files.
pub fn default_partitioner(_file_name: &str) -> bool {
    true
}

#[derive(Default)]
pub(crate) struct RunnerLocals {
    /// The temporary directory. Test cases can use `__TEST_DIR__` to refer to this directory.
    /// Lazily initialized and cleaned up when dropped.
    test_dir: OnceLock<TempDir>,
    /// Runtime variables for substitution.
    variables: BTreeMap<String, String>,
}

impl RunnerLocals {
    pub fn test_dir(&self) -> String {
        let test_dir = self
            .test_dir
            .get_or_init(|| TempDir::new().expect("failed to create testdir"));
        test_dir.path().to_string_lossy().into_owned()
    }

    fn set_var(&mut self, key: String, value: String) {
        self.variables.insert(key, value);
    }

    pub fn get_var(&self, key: &str) -> Option<&String> {
        self.variables.get(key)
    }

    pub fn vars(&self) -> &BTreeMap<String, String> {
        &self.variables
    }
}

/// Sqllogictest runner.
pub struct Runner<D: AsyncDB, M: MakeConnection<Conn = D>> {
    conn: Connections<D, M>,
    // validator is used for validate if the result of query equals to expected.
    validator: Validator,
    // normalizer is used to normalize the result text
    normalizer: Normalizer,
    column_type_validator: ColumnTypeValidator<D::ColumnType>,
    partitioner: Arc<dyn Partitioner>,
    substitution_on: bool,
    sort_mode: Option<SortMode>,
    result_mode: Option<ResultMode>,
    /// 0 means never hashing
    hash_threshold: usize,
    /// Labels for condition `skipif` and `onlyif`.
    labels: HashSet<String>,
    /// Local variables/context for the runner.
    locals: RunnerLocals,
    /// Whether to continue running after statement failures
    continue_on_error: bool,
    /// Whether to use verbose error output
    verbose: bool,
}

impl<D: AsyncDB, M: MakeConnection<Conn = D>> Runner<D, M> {
    /// Create a new test runner on the database, with the given connection maker.
    ///
    /// See [`MakeConnection`] for more details.
    pub fn new(make_conn: M) -> Self {
        Runner {
            validator: default_validator,
            normalizer: default_normalizer,
            column_type_validator: default_column_validator,
            partitioner: Arc::new(default_partitioner),
            substitution_on: false,
            sort_mode: None,
            result_mode: None,
            hash_threshold: 0,
            labels: HashSet::new(),
            conn: Connections::new(make_conn),
            locals: RunnerLocals::default(),
            continue_on_error: true,
            verbose: true,
        }
    }

    /// Add a label for condition `skipif` and `onlyif`.
    pub fn add_label(&mut self, label: &str) {
        self.labels.insert(label.to_string());
    }

    /// Set a local variable for substitution.
    pub fn set_var(&mut self, key: String, value: String) {
        self.locals.set_var(key, value);
    }

    pub fn with_normalizer(&mut self, normalizer: Normalizer) {
        self.normalizer = normalizer;
    }
    pub fn with_validator(&mut self, validator: Validator) {
        self.validator = validator;
    }

    pub fn with_column_validator(&mut self, validator: ColumnTypeValidator<D::ColumnType>) {
        self.column_type_validator = validator;
    }

    /// Set the partitioner for the runner. Only files that match the partitioner will be run.
    ///
    /// This only takes effect when running tests in parallel.
    pub fn with_partitioner(&mut self, partitioner: impl Partitioner + 'static) {
        self.partitioner = Arc::new(partitioner);
    }

    pub fn with_hash_threshold(&mut self, hash_threshold: usize) {
        self.hash_threshold = hash_threshold;
    }

    /// Set whether to continue running after statement failures
    pub fn with_continue_on_error(&mut self, continue_on_error: bool) {
        self.continue_on_error = continue_on_error;
    }

    /// Set whether to use verbose error output
    pub fn with_verbose(&mut self, verbose: bool) {
        self.verbose = verbose;
    }

    pub async fn apply_record(
        &mut self,
        record: Record<D::ColumnType>,
    ) -> RecordOutput<D::ColumnType> {
        tracing::debug!(?record, "testing");
        /// Returns whether we should skip this record, according to given `conditions`.
        fn should_skip(
            labels: &HashSet<String>,
            engine_name: &str,
            conditions: &[Condition],
        ) -> bool {
            conditions.iter().any(|c| {
                c.should_skip(
                    labels
                        .iter()
                        .map(|l| l.as_str())
                        // attach the engine name to the labels
                        .chain(Some(engine_name).filter(|n| !n.is_empty())),
                )
            })
        }

        match record {
            Record::Statement {
                conditions,
                connection,
                sql,

                // compare result in run_async
                expected: _,
                loc: _,
                retry: _,
            } => {
                let sql = match self.may_substitute(sql, true) {
                    Ok(sql) => sql,
                    Err(error) => {
                        return RecordOutput::Statement {
                            count: 0,
                            error: Some(error),
                        }
                    }
                };

                let conn = match self.conn.get(connection).await {
                    Ok(conn) => conn,
                    Err(e) => {
                        return RecordOutput::Statement {
                            count: 0,
                            error: Some(Arc::new(e)),
                        }
                    }
                };
                if should_skip(&self.labels, conn.engine_name(), &conditions) {
                    return RecordOutput::Nothing;
                }

                let ret = conn.run(&sql).await;
                match ret {
                    Ok(out) => match out {
                        DBOutput::Rows { types, rows } => RecordOutput::Query {
                            types,
                            rows,
                            error: None,
                        },
                        DBOutput::StatementComplete(count) => {
                            RecordOutput::Statement { count, error: None }
                        }
                    },
                    Err(e) => RecordOutput::Statement {
                        count: 0,
                        error: Some(Arc::new(e)),
                    },
                }
            }
            Record::System {
                conditions,
                command,
                loc: _,
                stdout: expected_stdout,
                retry: _,
            } => {
                if should_skip(&self.labels, "", &conditions) {
                    return RecordOutput::Nothing;
                }

                let mut command = match self.may_substitute(command, false) {
                    Ok(command) => command,
                    Err(error) => {
                        return RecordOutput::System {
                            stdout: None,
                            error: Some(error),
                        }
                    }
                };

                let is_background = command.trim().ends_with('&');
                if is_background {
                    command = command.trim_end_matches('&').trim().to_string();
                }

                let mut cmd = if cfg!(target_os = "windows") {
                    let mut cmd = std::process::Command::new("cmd");
                    cmd.arg("/C").arg(&command);
                    cmd
                } else {
                    let mut cmd = std::process::Command::new("bash");
                    cmd.arg("-c").arg(&command);
                    cmd
                };

                if is_background {
                    // Spawn a new process, but don't wait for stdout, otherwise it will block until
                    // the process exits.
                    let error: Option<AnyError> = match cmd.spawn() {
                        Ok(_) => None,
                        Err(e) => Some(Arc::new(e)),
                    };
                    tracing::info!(target:"sqllogictest::system_command", command, "background system command spawned");
                    return RecordOutput::System {
                        error,
                        stdout: None,
                    };
                }

                cmd.stdout(std::process::Stdio::piped());
                cmd.stderr(std::process::Stdio::piped());

                let result = D::run_command(cmd).await;
                #[derive(thiserror::Error, Debug)]
                #[error(
                    "process exited unsuccessfully: {status}\nstdout: {stdout}\nstderr: {stderr}"
                )]
                struct SystemError {
                    status: ExitStatus,
                    stdout: String,
                    stderr: String,
                }

                let mut actual_stdout = None;
                let error: Option<AnyError> = match result {
                    Ok(Output {
                        status,
                        stdout,
                        stderr,
                    }) => {
                        let stdout = String::from_utf8_lossy(&stdout).to_string();
                        let stderr = String::from_utf8_lossy(&stderr).to_string();
                        tracing::info!(target:"sqllogictest::system_command", command, ?status, stdout, stderr, "system command executed");
                        if status.success() {
                            if expected_stdout.is_some() {
                                actual_stdout = Some(stdout);
                            }
                            None
                        } else {
                            Some(Arc::new(SystemError {
                                status,
                                stdout,
                                stderr,
                            }))
                        }
                    }
                    Err(error) => {
                        tracing::error!(target:"sqllogictest::system_command", command, ?error, "failed to run system command");
                        Some(Arc::new(error))
                    }
                };

                RecordOutput::System {
                    error,
                    stdout: actual_stdout,
                }
            }
            Record::Query {
                conditions,
                connection,
                sql,

                // compare result in run_async
                expected,
                loc: _,
                retry: _,
            } => {
                let sql = match self.may_substitute(sql, true) {
                    Ok(sql) => sql,
                    Err(error) => {
                        return RecordOutput::Query {
                            error: Some(error),
                            types: vec![],
                            rows: vec![],
                        }
                    }
                };

                let conn = match self.conn.get(connection).await {
                    Ok(conn) => conn,
                    Err(e) => {
                        return RecordOutput::Query {
                            error: Some(Arc::new(e)),
                            types: vec![],
                            rows: vec![],
                        }
                    }
                };
                if should_skip(&self.labels, conn.engine_name(), &conditions) {
                    return RecordOutput::Nothing;
                }

                let (types, mut rows) = match conn.run(&sql).await {
                    Ok(out) => match out {
                        DBOutput::Rows { types, rows } => (types, rows),
                        DBOutput::StatementComplete(count) => {
                            return RecordOutput::Statement { count, error: None };
                        }
                    },
                    Err(e) => {
                        return RecordOutput::Query {
                            error: Some(Arc::new(e)),
                            types: vec![],
                            rows: vec![],
                        };
                    }
                };

                let sort_mode = match expected {
                    QueryExpect::Results { sort_mode, .. } => sort_mode,
                    QueryExpect::Error(_) => None,
                }
                .or(self.sort_mode);

                let mut value_sort = false;
                match sort_mode {
                    None | Some(SortMode::NoSort) => {}
                    Some(SortMode::RowSort) => {
                        rows.sort_unstable();
                    }
                    Some(SortMode::ValueSort) => {
                        rows = rows
                            .iter()
                            .flat_map(|row| row.iter())
                            .map(|s| vec![s.to_owned()])
                            .collect();
                        rows.sort_unstable();
                        value_sort = true;
                    }
                };

                let num_values = if value_sort {
                    rows.len()
                } else {
                    rows.len() * types.len()
                };

                if self.hash_threshold > 0 && num_values > self.hash_threshold {
                    let mut md5 = md5::Md5::new();
                    for line in &rows {
                        for value in line {
                            md5.update(value.as_bytes());
                            md5.update(b"\n");
                        }
                    }
                    let hash = format!("{:2x}", md5.finalize());
                    rows = vec![vec![format!(
                        "{} values hashing to {}",
                        rows.len() * rows[0].len(),
                        hash
                    )]];
                }

                RecordOutput::Query {
                    error: None,
                    types,
                    rows,
                }
            }
            Record::Sleep { duration, .. } => {
                D::sleep(duration).await;
                RecordOutput::Nothing
            }
            Record::Control(control) => {
                match control {
                    Control::SortMode(sort_mode) => {
                        self.sort_mode = Some(sort_mode);
                    }
                    Control::ResultMode(result_mode) => {
                        self.result_mode = Some(result_mode);
                    }
                    Control::Substitution(on_off) => self.substitution_on = on_off,
                }

                RecordOutput::Nothing
            }
            Record::HashThreshold { loc: _, threshold } => {
                self.hash_threshold = threshold as usize;
                RecordOutput::Nothing
            }
            Record::Halt { loc: _ } => {
                tracing::error!("halt record encountered. It's likely a bug of the runtime.");
                RecordOutput::Nothing
            }
            Record::Include { .. }
            | Record::Newline
            | Record::Comment(_)
            | Record::Subtest { .. }
            | Record::Injected(_)
            | Record::Condition(_)
            | Record::Connection(_) => RecordOutput::Nothing,
        }
    }

    /// Run a single record.
    pub async fn run_async(
        &mut self,
        record: Record<D::ColumnType>,
    ) -> Result<RecordOutput<D::ColumnType>, TestError> {
        let retry = match &record {
            Record::Statement { retry, .. } => retry.clone(),
            Record::Query { retry, .. } => retry.clone(),
            Record::System { retry, .. } => retry.clone(),
            _ => None,
        };
        if retry.is_none() {
            return self.run_async_no_retry(record).await;
        }

        // Retry for `retry.attempts` times. The parser ensures that `retry.attempts` must > 0.
        let retry = retry.unwrap();
        let mut last_error = None;
        for _ in 0..retry.attempts {
            let result = self.run_async_no_retry(record.clone()).await;
            if result.is_ok() {
                return result;
            }
            tracing::warn!(target:"sqllogictest::retry", backoff = ?retry.backoff, error = ?result, "retrying");
            D::sleep(retry.backoff).await;
            last_error = result.err();
        }

        Err(last_error.unwrap())
    }

    /// Run a single record without retry.
    async fn run_async_no_retry(
        &mut self,
        record: Record<D::ColumnType>,
    ) -> Result<RecordOutput<D::ColumnType>, TestError> {
        let result = self.apply_record(record.clone()).await;

        match (record, &result) {
            (_, RecordOutput::Nothing) => {}
            // Tolerate the mismatched return type...
            (
                Record::Statement {
                    sql, expected, loc, ..
                },
                RecordOutput::Query {
                    error: None, rows, ..
                },
            ) => {
                if let StatementExpect::Error(_) = expected {
                    return Err(TestErrorKind::Ok {
                        sql,
                        kind: RecordKind::Query,
                    }
                    .at(loc));
                }
                if let StatementExpect::Count(expected_count) = expected {
                    if expected_count != rows.len() as u64 {
                        return Err(TestErrorKind::StatementResultMismatch {
                            sql,
                            expected: expected_count,
                            actual: format!("returned {} rows", rows.len()),
                        }
                        .at(loc));
                    }
                }
            }
            (
                Record::Query {
                    loc, sql, expected, ..
                },
                RecordOutput::Statement { error: None, .. },
            ) => match expected {
                QueryExpect::Error(_) => {
                    return Err(TestErrorKind::Ok {
                        sql,
                        kind: RecordKind::Query,
                    }
                    .at(loc))
                }
                QueryExpect::Results { results, .. } if !results.is_empty() => {
                    return Err(TestErrorKind::QueryResultMismatch {
                        sql,
                        expected: results.join("\n"),
                        actual: "".to_string(),
                    }
                    .at(loc))
                }
                QueryExpect::Results { .. } => {}
            },
            (
                Record::Statement {
                    loc,
                    connection: _,
                    conditions: _,
                    sql,
                    expected,
                    retry: _,
                },
                RecordOutput::Statement { count, error },
            ) => match (error, expected) {
                (None, StatementExpect::Error(_)) => {
                    return Err(TestErrorKind::Ok {
                        sql,
                        kind: RecordKind::Statement,
                    }
                    .at(loc))
                }
                (None, StatementExpect::Count(expected_count)) => {
                    if expected_count != *count {
                        return Err(TestErrorKind::StatementResultMismatch {
                            sql,
                            expected: expected_count,
                            actual: format!("affected {count} rows"),
                        }
                        .at(loc));
                    }
                }
                (None, StatementExpect::Ok) => {}
                (Some(e), StatementExpect::Error(expected_error)) => {
                    if !expected_error.is_match(&e.to_string()) {
                        return Err(TestErrorKind::ErrorMismatch {
                            sql,
                            err: Arc::clone(e),
                            expected_err: expected_error.to_string(),
                            kind: RecordKind::Statement,
                        }
                        .at(loc));
                    }
                }
                (Some(e), StatementExpect::Count(_) | StatementExpect::Ok) => {
                    return Err(TestErrorKind::Fail {
                        sql,
                        err: Arc::clone(e),
                        kind: RecordKind::Statement,
                    }
                    .at(loc));
                }
            },
            (
                Record::Query {
                    loc,
                    conditions: _,
                    connection: _,
                    sql,
                    expected,
                    retry: _,
                },
                RecordOutput::Query { types, rows, error },
            ) => {
                match (error, expected) {
                    (None, QueryExpect::Error(_)) => {
                        return Err(TestErrorKind::Ok {
                            sql,
                            kind: RecordKind::Query,
                        }
                        .at(loc));
                    }
                    (Some(e), QueryExpect::Error(expected_error)) => {
                        if !expected_error.is_match(&e.to_string()) {
                            return Err(TestErrorKind::ErrorMismatch {
                                sql,
                                err: Arc::clone(e),
                                expected_err: expected_error.to_string(),
                                kind: RecordKind::Query,
                            }
                            .at(loc));
                        }
                    }
                    (Some(e), QueryExpect::Results { .. }) => {
                        return Err(TestErrorKind::Fail {
                            sql,
                            err: Arc::clone(e),
                            kind: RecordKind::Query,
                        }
                        .at(loc));
                    }
                    (
                        None,
                        QueryExpect::Results {
                            types: expected_types,
                            results: expected_results,
                            sort_mode,
                            ..
                        },
                    ) => {
                        if !(self.column_type_validator)(types, &expected_types) {
                            return Err(TestErrorKind::QueryResultColumnsMismatch {
                                sql,
                                expected: expected_types.iter().map(|c| c.to_char()).join(""),
                                actual: types.iter().map(|c| c.to_char()).join(""),
                            }
                            .at(loc));
                        }

                        let actual_results = match self.result_mode {
                            Some(ResultMode::ValueWise) => rows
                                .iter()
                                .flat_map(|strs| strs.iter())
                                .map(|str| vec![str.to_string()])
                                .collect_vec(),
                            Some(ResultMode::TextWise) => rows.clone(),
                            // default to rowwise
                            _ => rows.clone(),
                        };

                        // Apply the same sorting to expected results as was applied to actual results
                        let mut sorted_expected_results = expected_results.clone();
                        let effective_sort_mode = sort_mode.or(self.sort_mode);
                        match effective_sort_mode {
                            Some(SortMode::RowSort) => {
                                sorted_expected_results.sort_unstable();
                            }
                            Some(SortMode::ValueSort) => {
                                // For ValueSort, each value becomes a separate "row"
                                let mut all_values: Vec<String> = sorted_expected_results
                                    .iter()
                                    .flat_map(|line| line.split_whitespace().map(|s| s.to_string()))
                                    .collect();
                                all_values.sort_unstable();
                                sorted_expected_results = all_values;
                            }
                            _ => {} // NoSort or None - keep original order
                        }

                        // Use different validator for TextWise mode
                        let validation_result = match self.result_mode {
                            Some(ResultMode::TextWise) => textwise_validator(self.normalizer, &actual_results, &sorted_expected_results),
                            _ => (self.validator)(self.normalizer, &actual_results, &sorted_expected_results),
                        };

                        if !validation_result {
                            let output_rows =
                                rows.iter().map(|strs| strs.iter().join(" ")).collect_vec();
                            return Err(TestErrorKind::QueryResultMismatch {
                                sql,
                                expected: sorted_expected_results.join("\n"),
                                actual: output_rows.join("\n"),
                            }
                            .at(loc));
                        }
                    }
                };
            }
            (
                Record::System {
                    loc,
                    conditions: _,
                    command,
                    stdout: expected_stdout,
                    retry: _,
                },
                RecordOutput::System {
                    error,
                    stdout: actual_stdout,
                },
            ) => {
                if let Some(err) = error {
                    return Err(TestErrorKind::SystemFail {
                        command,
                        err: Arc::clone(err),
                    }
                    .at(loc));
                }
                match (expected_stdout, actual_stdout) {
                    (None, _) => {}
                    (Some(expected_stdout), actual_stdout) => {
                        let actual_stdout = actual_stdout.clone().unwrap_or_default();
                        // TODO: support newlines contained in expected_stdout
                        if expected_stdout != actual_stdout.trim() {
                            return Err(TestErrorKind::SystemStdoutMismatch {
                                command,
                                expected_stdout,
                                actual_stdout,
                            }
                            .at(loc));
                        }
                    }
                }
            }
            _ => unreachable!(),
        }

        Ok(result)
    }

    /// Run a single record.
    ///
    /// Returns the output of the record if successful.
    pub fn run(
        &mut self,
        record: Record<D::ColumnType>,
    ) -> Result<RecordOutput<D::ColumnType>, TestError> {
        futures::executor::block_on(self.run_async(record))
    }

    /// Run multiple records.
    ///
    /// The runner will stop early once a halt record is seen.
    ///
    /// To acquire the result of each record, manually call `run_async` for each record instead.
    pub async fn run_multi_async(
        &mut self,
        records: impl IntoIterator<Item = Record<D::ColumnType>>,
    ) -> Result<(), TestError> {
        for record in records.into_iter() {
            if let Record::Halt { .. } = record {
                break;
            }

            if self.continue_on_error {
                if let Err(e) = self.run_async(record).await {
                    // Log the error but continue
                    eprintln!("Error (continuing): {}", e);
                    continue;
                }
            } else {
                self.run_async(record).await?;
            }
        }
        Ok(())
    }

    /// Run multiple records.
    ///
    /// The runner will stop early once a halt record is seen.
    ///
    /// To acquire the result of each record, manually call `run` for each record instead.
    pub fn run_multi(
        &mut self,
        records: impl IntoIterator<Item = Record<D::ColumnType>>,
    ) -> Result<(), TestError> {
        block_on(self.run_multi_async(records))
    }

    /// Run a sqllogictest script.
    pub async fn run_script_async(&mut self, script: &str) -> Result<(), TestError> {
        let records = parse(script).expect("failed to parse sqllogictest");
        self.run_multi_async(records).await
    }

    /// Run a sqllogictest script with a given script name.
    pub async fn run_script_with_name_async(
        &mut self,
        script: &str,
        name: impl Into<Arc<str>>,
    ) -> Result<(), TestError> {
        let records = parse_with_name(script, name).expect("failed to parse sqllogictest");
        self.run_multi_async(records).await
    }

    /// Run a sqllogictest file.
    pub async fn run_file_async(&mut self, filename: impl AsRef<Path>) -> Result<(), TestError> {
        let records = parse_file(filename)?;
        self.run_multi_async(records).await
    }

    /// Run a sqllogictest script.
    pub fn run_script(&mut self, script: &str) -> Result<(), TestError> {
        block_on(self.run_script_async(script))
    }

    /// Run a sqllogictest script with a given script name.
    pub fn run_script_with_name(
        &mut self,
        script: &str,
        name: impl Into<Arc<str>>,
    ) -> Result<(), TestError> {
        block_on(self.run_script_with_name_async(script, name))
    }

    /// Run a sqllogictest file.
    pub fn run_file(&mut self, filename: impl AsRef<Path>) -> Result<(), TestError> {
        block_on(self.run_file_async(filename))
    }

    /// accept the tasks, spawn jobs task to run slt test. the tasks are (AsyncDB, slt filename)
    /// pairs.
    // TODO: This is not a good interface, as the `make_conn` passed to `new` is unused but we
    // accept a new `conn_builder` here. May change `MakeConnection` to support specifying the
    // database name in the future.
    pub async fn run_parallel_async<Fut>(
        &mut self,
        glob: &str,
        hosts: Vec<String>,
        conn_builder: fn(String, String) -> Fut,
        jobs: usize,
    ) -> Result<(), ParallelTestError>
    where
        Fut: Future<Output = D>,
    {
        let files = glob::glob(glob).expect("failed to read glob pattern");
        let mut tasks = vec![];

        for (idx, file) in files.enumerate() {
            // for every slt file, we create a database against table conflict
            let file = file.unwrap();
            let filename = file.to_str().expect("not a UTF-8 filename");

            // Skip files that don't match the partitioner.
            if !self.partitioner.matches(filename) {
                continue;
            }

            let db_name = filename.replace([' ', '.', '-', '/'], "_");

            self.conn
                .run_default(&format!("CREATE DATABASE {db_name};"))
                .await
                .expect("create db failed");
            let target = hosts[idx % hosts.len()].clone();

            let mut locals = RunnerLocals::default();
            locals.set_var("__DATABASE__".to_owned(), db_name.clone());

            let mut tester = Runner {
                conn: Connections::new(move || {
                    conn_builder(target.clone(), db_name.clone()).map(Ok)
                }),
                validator: self.validator,
                normalizer: self.normalizer,
                column_type_validator: self.column_type_validator,
                partitioner: self.partitioner.clone(),
                substitution_on: self.substitution_on,
                sort_mode: self.sort_mode,
                result_mode: self.result_mode,
                hash_threshold: self.hash_threshold,
                labels: self.labels.clone(),
                locals,
                continue_on_error: self.continue_on_error,
                verbose: self.verbose,
            };

            tasks.push(async move {
                let filename = file.to_string_lossy().to_string();
                tester.run_file_async(filename).await
            })
        }

        let tasks = stream::iter(tasks).buffer_unordered(jobs);
        let errors: Vec<_> = tasks
            .filter_map(|result| async { result.err() })
            .collect()
            .await;
        if errors.is_empty() {
            Ok(())
        } else {
            Err(ParallelTestError { errors })
        }
    }

    /// sync version of `run_parallel_async`
    pub fn run_parallel<Fut>(
        &mut self,
        glob: &str,
        hosts: Vec<String>,
        conn_builder: fn(String, String) -> Fut,
        jobs: usize,
    ) -> Result<(), ParallelTestError>
    where
        Fut: Future<Output = D>,
    {
        block_on(self.run_parallel_async(glob, hosts, conn_builder, jobs))
    }

    /// Substitute the input SQL or command with [`Substitution`], if enabled by `control
    /// substitution`.
    ///
    /// If `subst_env_vars`, we will use the `subst` crate to support extensive substitutions, incl.
    /// `$NAME`, `${NAME}`, `${NAME:default}`. The cost is that we will have to use escape
    /// characters, e.g., `\$` & `\\`.
    ///
    /// Otherwise, we just do simple string substitution for `__TEST_DIR__` and `__NOW__`.
    /// This is useful for `system` commands: The shell can do the environment variables, and we can
    /// write strings like `\n` without escaping.
    fn may_substitute(&self, input: String, subst_env_vars: bool) -> Result<String, AnyError> {
        if self.substitution_on {
            Substitution::new(&self.locals, subst_env_vars)
                .substitute(&input)
                .map_err(|e| Arc::new(e) as AnyError)
        } else {
            Ok(input)
        }
    }

    /// Updates a test file with the output produced by a Database. It is an utility function
    /// wrapping [`update_test_file_with_runner`].
    ///
    /// Specifically, it will create `"{filename}.temp"` to buffer the updated records and then
    /// override the original file with it.
    ///
    /// Some other notes:
    /// - empty lines at the end of the file are cleaned.
    /// - `halt` and `include` are correctly handled.
    pub async fn update_test_file(
        &mut self,
        filename: impl AsRef<Path>,
        col_separator: &str,
        validator: Validator,
        normalizer: Normalizer,
        column_type_validator: ColumnTypeValidator<D::ColumnType>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use std::io::{Read, Seek, SeekFrom, Write};
        use std::path::PathBuf;

        use fs_err::{File, OpenOptions};

        fn create_outfile(filename: impl AsRef<Path>) -> std::io::Result<(PathBuf, File)> {
            let filename = filename.as_ref();
            let outfilename = format!(
                "{}{:010}{}",
                filename.file_name().unwrap().to_str().unwrap().to_owned(),
                rand::thread_rng().gen_range(0..10_000_000),
                ".temp"
            );
            let outfilename = filename.parent().unwrap().join(outfilename);
            // create a temp file in read-write mode
            let outfile = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .read(true)
                .open(&outfilename)?;
            Ok((outfilename, outfile))
        }

        fn override_with_outfile(
            filename: &String,
            outfilename: &PathBuf,
            outfile: &mut File,
        ) -> std::io::Result<()> {
            // check whether outfile ends with multiple newlines, which happens if
            // - the last record is statement/query
            // - the original file ends with multiple newlines

            const N: usize = 8;
            let mut buf = [0u8; N];
            loop {
                outfile.seek(SeekFrom::End(-(N as i64))).unwrap();
                outfile.read_exact(&mut buf).unwrap();
                let num_newlines = buf.iter().rev().take_while(|&&b| b == b'\n').count();
                assert!(num_newlines > 0);

                if num_newlines > 1 {
                    // if so, remove the last ones
                    outfile
                        .set_len(outfile.metadata().unwrap().len() - num_newlines as u64 + 1)
                        .unwrap();
                }

                if num_newlines == 1 || num_newlines < N {
                    break;
                }
            }

            outfile.flush()?;
            fs_err::rename(outfilename, filename)?;

            Ok(())
        }

        struct Item {
            filename: String,
            outfilename: PathBuf,
            outfile: File,
            halt: bool,
        }

        let filename = filename.as_ref();
        let records = parse_file(filename)?;

        let (outfilename, outfile) = create_outfile(filename)?;
        let mut stack = vec![Item {
            filename: filename.to_string_lossy().to_string(),
            outfilename,
            outfile,
            halt: false,
        }];

        for record in records {
            let Item {
                filename,
                outfilename,
                outfile,
                halt,
            } = stack.last_mut().unwrap();

            match &record {
                Record::Injected(Injected::BeginInclude(filename)) => {
                    let (outfilename, outfile) = create_outfile(filename)?;
                    stack.push(Item {
                        filename: filename.clone(),
                        outfilename,
                        outfile,
                        halt: false,
                    });
                }
                Record::Injected(Injected::EndInclude(_)) => {
                    override_with_outfile(filename, outfilename, outfile)?;
                    stack.pop();
                }
                _ => {
                    if *halt {
                        writeln!(outfile, "{record}")?;
                        continue;
                    }
                    if matches!(record, Record::Halt { .. }) {
                        *halt = true;
                        writeln!(outfile, "{record}")?;
                        tracing::info!(
                            "halt record found, all following records will be written AS IS"
                        );
                        continue;
                    }
                    let record_output = self.apply_record(record.clone()).await;
                    let record = update_record_with_output(
                        &record,
                        &record_output,
                        col_separator,
                        validator,
                        normalizer,
                        column_type_validator,
                    )
                    .unwrap_or(record);
                    writeln!(outfile, "{record}")?;
                }
            }
        }

        let Item {
            filename,
            outfilename,
            outfile,
            halt: _,
        } = stack.last_mut().unwrap();
        override_with_outfile(filename, outfilename, outfile)?;

        Ok(())
    }
}

impl<D: AsyncDB, M: MakeConnection<Conn = D>> Runner<D, M> {
    /// Shutdown all connections in the runner.
    pub async fn shutdown_async(&mut self) {
        tracing::debug!("shutting down runner...");
        self.conn.shutdown_all().await;
    }

    /// Shutdown all connections in the runner.
    pub fn shutdown(&mut self) {
        block_on(self.shutdown_async());
    }
}

/// Updates the specified [`Record`] with the [`QueryOutput`] produced
/// by a Database, returning `Some(new_record)`.
///
/// If an update is not supported or not necessary, returns `None`
pub fn update_record_with_output<T: ColumnType>(
    record: &Record<T>,
    record_output: &RecordOutput<T>,
    col_separator: &str,
    validator: Validator,
    normalizer: Normalizer,
    column_type_validator: ColumnTypeValidator<T>,
) -> Option<Record<T>> {
    match (record.clone(), record_output) {
        (_, RecordOutput::Nothing) => None,
        // statement, query
        (
            Record::Statement {
                sql,
                loc,
                conditions,
                connection,
                expected: mut expected @ (StatementExpect::Ok | StatementExpect::Count(_)),
                retry,
            },
            RecordOutput::Query {
                error: None, rows, ..
            },
        ) => {
            // statement ok
            // SELECT ...
            //
            // This case can be used when we want to only ensure the query succeeds,
            // but don't care about the output.
            // DuckDB has a few of these.

            if let StatementExpect::Count(expected_count) = &mut expected {
                *expected_count = rows.len() as u64;
            }

            Some(Record::Statement {
                sql,
                loc,
                conditions,
                connection,
                expected,
                retry,
            })
        }
        // query, statement
        (
            Record::Query {
                sql,
                loc,
                conditions,
                connection,
                expected: _,
                retry,
            },
            RecordOutput::Statement { error: None, count },
        ) => Some(Record::Statement {
            sql,
            loc,
            conditions,
            connection,
            expected: StatementExpect::Count(*count),
            retry,
        }),
        // statement, statement
        (
            Record::Statement {
                loc,
                conditions,
                connection,
                sql,
                expected,
                retry,
            },
            RecordOutput::Statement { count, error },
        ) => match (error, expected) {
            // Ok
            (None, expected) => Some(Record::Statement {
                sql,
                loc,
                conditions,
                connection,
                expected: match expected {
                    StatementExpect::Count(_) => StatementExpect::Count(*count),
                    StatementExpect::Error(_) | StatementExpect::Ok => StatementExpect::Ok,
                },
                retry,
            }),
            // Error match
            (Some(e), StatementExpect::Error(expected_error))
                if expected_error.is_match(&e.to_string()) =>
            {
                None
            }
            // Error mismatch, update expected error
            (Some(e), r) => {
                let reference = match &r {
                    StatementExpect::Error(e) => Some(e),
                    StatementExpect::Count(_) | StatementExpect::Ok => None,
                };
                Some(Record::Statement {
                    sql,
                    expected: StatementExpect::Error(ExpectedError::from_actual_error(
                        reference,
                        &e.to_string(),
                    )),
                    loc,
                    conditions,
                    connection,
                    retry,
                })
            }
        },
        // query, query
        (
            Record::Query {
                loc,
                conditions,
                connection,
                sql,
                expected,
                retry,
            },
            RecordOutput::Query { types, rows, error },
        ) => match (error, expected) {
            // Error match
            (Some(e), QueryExpect::Error(expected_error))
                if expected_error.is_match(&e.to_string()) =>
            {
                None
            }
            // Error mismatch
            (Some(e), r) => {
                let reference = match &r {
                    QueryExpect::Error(e) => Some(e),
                    QueryExpect::Results { .. } => None,
                };
                Some(Record::Query {
                    sql,
                    expected: QueryExpect::Error(ExpectedError::from_actual_error(
                        reference,
                        &e.to_string(),
                    )),
                    loc,
                    conditions,
                    connection,
                    retry,
                })
            }
            (None, expected) => {
                let results = match &expected {
                    // If validation is successful, we respect the original file's expected results.
                    QueryExpect::Results {
                        results: expected_results,
                        ..
                    } if validator(normalizer, rows, expected_results) => expected_results.clone(),
                    _ => rows.iter().map(|cols| cols.join(col_separator)).collect(),
                };
                let types = match &expected {
                    // If validation is successful, we respect the original file's expected types.
                    QueryExpect::Results {
                        types: expected_types,
                        ..
                    } if column_type_validator(types, expected_types) => expected_types.clone(),
                    _ => types.clone(),
                };
                Some(Record::Query {
                    sql,
                    loc,
                    conditions,
                    connection,
                    expected: match expected {
                        QueryExpect::Results {
                            sort_mode,
                            label,
                            result_mode,
                            ..
                        } => QueryExpect::Results {
                            results,
                            types,
                            sort_mode,
                            result_mode,
                            label,
                        },
                        QueryExpect::Error(_) => QueryExpect::Results {
                            results,
                            types,
                            sort_mode: None,
                            result_mode: None,
                            label: None,
                        },
                    },
                    retry,
                })
            }
        },
        (
            Record::System {
                loc,
                conditions,
                command,
                stdout: _,
                retry,
            },
            RecordOutput::System {
                stdout: actual_stdout,
                error,
            },
        ) => {
            if let Some(error) = error {
                tracing::error!(
                    ?error,
                    command,
                    "system command failed while updating the record. It will be unchanged."
                );
            }
            Some(Record::System {
                loc,
                conditions,
                command,
                stdout: actual_stdout.clone(),
                retry,
            })
        }

        // No update possible, return the original record
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DefaultColumnType;

    #[test]
    fn test_query_replacement_no_changes() {
        let record = "query   I?\n\
                    select * from foo;\n\
                    ----\n\
                    3      4";
        TestCase {
            // keep the input values
            input: record,

            // Model a run that produced a 3,4 as output
            record_output: query_output(
                &[&["3", "4"]],
                vec![DefaultColumnType::Integer, DefaultColumnType::Any],
            ),

            expected: Some(record),
        }
        .run()
    }

    #[test]
    fn test_query_replacement() {
        TestCase {
            // input should be ignored
            input: "query III\n\
                    select * from foo;\n\
                    ----\n\
                    1 2",

            // Model a run that produced a 3,4 as output
            record_output: query_output(
                &[&["3", "4"]],
                vec![DefaultColumnType::Integer, DefaultColumnType::Any],
            ),

            expected: Some(
                "query I?\n\
                 select * from foo;\n\
                 ----\n\
                 3 4",
            ),
        }
        .run()
    }

    #[test]
    fn test_query_replacement_no_input() {
        TestCase {
            // input has no query results
            input: "query\n\
                    select * from foo;\n\
                    ----",

            // Model a run that produced a 3,4 as output
            record_output: query_output(
                &[&["3", "4"]],
                vec![DefaultColumnType::Integer, DefaultColumnType::Any],
            ),

            expected: Some(
                "query I?\n\
                 select * from foo;\n\
                 ----\n\
                 3 4",
            ),
        }
        .run()
    }

    #[test]
    fn test_query_replacement_no_output() {
        TestCase {
            // input has no query results
            input: "query III\n\
                    select * from foo;\n\
                    ----",

            // Model nothing was output
            record_output: RecordOutput::Nothing,

            // No update
            expected: None,
        }
        .run()
    }

    #[test]
    fn test_query_replacement_error() {
        TestCase {
            // input has no query results
            input: "query III\n\
                    select * from foo;\n\
                    ----",

            // Model a run that produced a "MyAwesomeDB Error"
            record_output: query_output_error("MyAwesomeDB Error"),

            expected: Some(
                "query error TestError: MyAwesomeDB Error\n\
                 select * from foo;\n",
            ),
        }
        .run()
    }

    #[test]
    fn test_query_replacement_error_multiline() {
        TestCase {
            // input has no query results
            input: "query III\n\
                    select * from foo;\n\
                    ----",

            // Model a run that produced a "MyAwesomeDB Error"
            record_output: query_output_error("MyAwesomeDB Error\n\nCaused by:\n  Inner Error"),

            expected: Some(
                "query error
select * from foo;
----
TestError: MyAwesomeDB Error

Caused by:
  Inner Error",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_query_output() {
        TestCase {
            // input has no query results
            input: "statement ok\n\
                    create table foo;",

            // Model a run that produced a 3,4 as output
            record_output: query_output(
                &[&["3", "4"]],
                vec![DefaultColumnType::Integer, DefaultColumnType::Any],
            ),

            expected: Some(
                "statement ok\n\
                 create table foo;",
            ),
        }
        .run()
    }

    #[test]
    fn test_query_statement_output() {
        TestCase {
            // input has no query results
            input: "query III\n\
                    select * from foo;\n\
                    ----",

            // Model a run that produced a statement output
            record_output: statement_output(3),

            expected: Some(
                "statement count 3\n\
                 select * from foo;",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_output() {
        TestCase {
            // statement that has no output
            input: "statement ok\n\
                    insert into foo values(2);",

            // Model a run that produced a statement output
            record_output: statement_output(3),

            // Note the the output does not include 3 (statement
            // count) Rationale is if the record is statement count
            // <n>, n will be updated to real count. If the record is
            // statement ok (which means we don't care the number of
            // affected rows), it won't be updated.
            expected: Some(
                "statement ok\n\
                 insert into foo values(2);",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_error_to_ok() {
        TestCase {
            // statement expected error
            input: "statement error\n\
                    insert into foo values(2);",

            // Model a run that produced a statement output
            record_output: statement_output(3),

            expected: Some(
                "statement ok\n\
                 insert into foo values(2);",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_error_no_error() {
        TestCase {
            // statement expected error
            input: "statement error\n\
                    insert into foo values(2);",

            // Model a run that produced an error message
            record_output: statement_output_error("foo"),

            // Input didn't have an expected error, so output is not to expect the message, then no
            // update
            expected: None,
        }
        .run()
    }

    #[test]
    fn test_statement_error_new_error() {
        TestCase {
            // statement expected error
            input: "statement error bar\n\
                    insert into foo values(2);",

            // Model a run that produced an error message
            record_output: statement_output_error("foo"),

            // expect the output includes foo
            expected: Some(
                "statement error TestError: foo\n\
                 insert into foo values(2);",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_error_new_error_multiline() {
        TestCase {
            // statement expected error
            input: "statement error bar\n\
                    insert into foo values(2);",

            // Model a run that produced an error message
            record_output: statement_output_error("foo\n\nCaused by:\n  Inner Error"),

            // expect the output includes foo
            expected: Some(
                "statement error
insert into foo values(2);
----
TestError: foo

Caused by:
  Inner Error",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_error_ok_to_error() {
        TestCase {
            // statement was ok
            input: "statement ok\n\
                    insert into foo values(2);",

            // Model a run that produced an error message
            record_output: statement_output_error("foo"),

            // expect the output includes foo
            expected: Some(
                "statement error TestError: foo\n\
                 insert into foo values(2);",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_error_ok_to_error_multiline() {
        TestCase {
            // statement was ok
            input: "statement ok\n\
                    insert into foo values(2);",

            // Model a run that produced an error message
            record_output: statement_output_error("foo\n\nCaused by:\n  Inner Error"),

            // expect the output includes foo
            expected: Some(
                "statement error
insert into foo values(2);
----
TestError: foo

Caused by:
  Inner Error",
            ),
        }
        .run()
    }

    #[test]
    fn test_statement_error_special_chars() {
        TestCase {
            // statement expected error
            input: "statement error tbd\n\
                    inser into foo values(2);",

            // Model a run that produced an error message that contains regex special characters
            record_output: statement_output_error("The operation (inser) is not supported. Did you mean [insert]?"),

            // expect the output includes foo
            expected: Some(
                "statement error TestError: The operation \\(inser\\) is not supported\\. Did you mean \\[insert\\]\\?\n\
                 inser into foo values(2);",
            ),
        }
            .run()
    }

    #[test]
    fn test_statement_keep_error_regex_when_matches() {
        TestCase {
            // statement expected error
            input: "statement error TestError: The operation \\([a-z]+\\) is not supported.*\n\
                    inser into foo values(2);",

            // Model a run that produced an error message that contains regex special characters
            record_output: statement_output_error(
                "The operation (inser) is not supported. Did you mean [insert]?",
            ),

            // no update expected
            expected: None,
        }
        .run()
    }

    #[test]
    fn test_query_error_special_chars() {
        TestCase {
            // statement expected error
            input: "query error tbd\n\
                    selec *;",

            // Model a run that produced an error message that contains regex special characters
            record_output: query_output_error("The operation (selec) is not supported. Did you mean [select]?"),

            // expect the output includes foo
            expected: Some(
                "query error TestError: The operation \\(selec\\) is not supported\\. Did you mean \\[select\\]\\?\n\
                 selec *;",
            ),
        }
            .run()
    }

    #[test]
    fn test_query_error_special_chars_when_matches() {
        TestCase {
            // statement expected error
            input: "query error TestError: The operation \\([a-z]+\\) is not supported.*\n\
                    selec *;",

            // Model a run that produced an error message that contains regex special characters
            record_output: query_output_error(
                "The operation (selec) is not supported. Did you mean [select]?",
            ),

            // no update expected
            expected: None,
        }
        .run()
    }

    #[derive(Debug)]
    struct TestCase<'a> {
        input: &'a str,
        record_output: RecordOutput<DefaultColumnType>,
        expected: Option<&'a str>,
    }

    impl TestCase<'_> {
        #[track_caller]
        fn run(self) {
            let Self {
                input,
                record_output,
                expected,
            } = self;
            println!("TestCase");
            println!("**input:\n{input}\n");
            println!("**record_output:\n{record_output:#?}\n");
            println!("**expected:\n{}\n", expected.unwrap_or(""));
            let input = parse_to_record(input);
            let expected = expected.map(parse_to_record);
            let output = update_record_with_output(
                &input,
                &record_output,
                " ",
                default_validator,
                default_normalizer,
                strict_column_validator,
            );

            assert_eq!(
                &output,
                &expected,
                "\n\noutput:\n\n{}\n\nexpected:\n\n{}",
                output
                    .as_ref()
                    .map(|r| r.to_string())
                    .unwrap_or_else(|| "None".into()),
                expected
                    .as_ref()
                    .map(|r| r.to_string())
                    .unwrap_or_else(|| "None".into()),
            );
        }
    }

    fn parse_to_record(s: &str) -> Record<DefaultColumnType> {
        let mut records = parse(s).unwrap();
        assert_eq!(records.len(), 1);
        records.pop().unwrap()
    }

    /// Returns a RecordOutput that models the successful execution of a query
    fn query_output(
        rows: &[&[&str]],
        types: Vec<DefaultColumnType>,
    ) -> RecordOutput<DefaultColumnType> {
        let rows = rows
            .iter()
            .map(|cols| cols.iter().map(|c| c.to_string()).collect::<Vec<_>>())
            .collect::<Vec<_>>();

        RecordOutput::Query {
            types,
            rows,
            error: None,
        }
    }

    /// Returns a RecordOutput that models the error of a query
    fn query_output_error(error_message: &str) -> RecordOutput<DefaultColumnType> {
        RecordOutput::Query {
            types: vec![],
            rows: vec![],
            error: Some(Arc::new(TestError(error_message.to_string()))),
        }
    }

    fn statement_output(count: u64) -> RecordOutput<DefaultColumnType> {
        RecordOutput::Statement { count, error: None }
    }

    /// RecordOutput that models a statement with error
    fn statement_output_error(error_message: &str) -> RecordOutput<DefaultColumnType> {
        RecordOutput::Statement {
            count: 0,
            error: Some(Arc::new(TestError(error_message.to_string()))),
        }
    }

    #[derive(Debug)]
    struct TestError(String);
    impl std::error::Error for TestError {}
    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestError: {}", self.0)
        }
    }

    #[test]
    fn test_default_validator_ignore_simple() {
        let normalizer = default_normalizer;
        let actual = vec![vec!["foo".to_string(), "bar".to_string()]];
        let expected = vec!["foo<slt:ignore>bar".to_string()];
        assert!(default_validator(normalizer, &actual, &expected));
    }

    #[test]
    fn test_default_validator_ignore_multiple_fragments() {
        let normalizer = default_normalizer;
        let actual = vec![vec![
            "one".to_string(),
            "two".to_string(),
            "three".to_string(),
        ]];
        let expected = vec!["one<slt:ignore>three".to_string()];
        assert!(default_validator(normalizer, &actual, &expected));
    }

    #[test]
    fn test_default_validator_ignore_fail() {
        let normalizer = default_normalizer;
        let actual = vec![vec![
            "alpha".to_string(),
            "beta".to_string(),
            "gamma".to_string(),
        ]];
        let expected = vec!["alpha<slt:ignore>delta".to_string()];
        assert!(!default_validator(normalizer, &actual, &expected));
    }
}
