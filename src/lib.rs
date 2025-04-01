#[cfg(feature = "json")]
pub mod json;

#[macro_use]
mod macros;

#[cfg(feature = "regex")]
pub mod pattern;

use std::{
    collections::HashSet,
    env,
    ffi::{OsStr, OsString},
    future,
    io,
    path::{Path, PathBuf},
    pin,
    process::{Output, Stdio},
    task::Poll,
};

use clap::{Parser, Subcommand};
use futures::stream::{self, FuturesUnordered, Stream, StreamExt, TryStreamExt};
use thiserror::Error;

use tokio::{
    fs,
    process::Command,
    time::{self, Duration},
};

use tokio_stream::wrappers::ReadDirStream;

#[derive(Parser)]
#[command(name = "subdo", version, about = "A CLI for applying a command to directories within a directory", long_about = None)]
pub struct Cli {
    /// A path to specify for the parent directory
    #[arg(short, long)]
    path: Option<PathBuf>,
    /// The patterns denoting which children directories to ignore
    #[arg(short, long, value_name = "PATTERN", value_delimiter = ' ', num_args = 1..)]
    ignore: Vec<OsString>,
    /// Applies the command to the tree with the path as root
    #[arg(short, long, default_value_t = false)]
    recursive: bool,
    /// Max number of concurrent tasks
    #[arg(short, long, default_value_t = num_cpus::get().min(u16::MAX as usize) as u16, value_parser = clap::value_parser!(u16).range(1..))]
    jobs: u16,
    /// Max duration for any given process
    #[arg(short, long, value_parser = humantime::parse_duration)]
    timeout: Option<Duration>,
    #[cfg(feature = "json")]
    /// Optional JSON representation
    #[arg(short, long, value_enum, default_value_t = json::Mode::Standard)]
    mode: json::Mode,
    /// The command to execute
    #[command(subcommand)]
    command: External,
}

#[derive(Subcommand)]
enum External {
    #[command(external_subcommand)]
    Command(Vec<OsString>),
}

pub struct CliParsed {
    command: (OsString, Vec<OsString>),
    ignored_subdirectories: IgnoredEntries,
    recursive: bool,
    jobs: usize,
    timeout: Option<Duration>,
    #[cfg(feature = "json")]
    mode: json::Mode,
}

#[derive(Default)]
struct IgnoredEntries {
    exact: HashSet<PathBuf>,
    #[cfg(feature = "regex")]
    pattern: pattern::IgnoredPattern,
}

pub enum IgnoredEntry {
    Exact(PathBuf),
    #[cfg(feature = "regex")]
    Pattern(regex::Regex),
}

#[derive(Debug, Error)]
pub enum CliError {
    #[error("vacant specified command")]
    Command,
    #[error("current directory is unavailable as: {0}")]
    CurrentDirectory(io::Error),
    #[error("an ignored directory ({entry}) is invalid as: {origin}", entry = .0.display(), origin = .1)]
    IgnoredDirectories(PathBuf, io::Error),
    #[cfg(feature = "regex")]
    #[error("ignored pattern is unavailable as: {0}")]
    IgnoredPattern(#[from] pattern::PatternError),
}

#[derive(Debug, Error)]
pub enum ProcessError {
    #[error("invalid directory entry from likely modification")]
    ModifiedEntry,
    #[error("subdirectories for {entry} are unavailable as: {origin}", .entry = .entry.display())]
    SubDirectories { entry: PathBuf, origin: io::Error },
    #[error("process {process} for {entry} made unavailable as: {origin}", process = .process.to_string_lossy(), entry = .entry.display())]
    ProcessSpawn { process: OsString, entry: PathBuf, origin: io::Error },
    #[error("process {process} for {entry} has unavailable output as: {origin}", process = .process.to_string_lossy(), entry = .entry.display())]
    ProcessOutput { process: OsString, entry: PathBuf, origin: io::Error },
    #[error("process {process} for {entry} could not complete in {duration}", process = .process.to_string_lossy(), entry = .entry.display())]
    Timeout { process: OsString, entry: PathBuf, duration: String },
}

impl Cli {
    pub async fn parse() -> Result<(PathBuf, CliParsed), CliError> {
        let cli: Cli = Parser::parse();

        #[cfg(feature = "json")]
        let mode = cli.mode;
        let timeout = cli.timeout;
        let jobs = usize::from(cli.jobs);
        let recursive = cli.recursive;

        let External::Command(command) = cli.command;
        let mut command = command.into_iter();

        let command = command
            .next()
            .map(|command_standalone| (command_standalone, command.collect::<Vec<_>>()))
            .ok_or(CliError::Command)?;

        let directory = cli
            .path
            .map(Ok)
            .unwrap_or_else(env::current_dir)
            .map_err(CliError::CurrentDirectory)?;

        let ignored_subdirectories = stream::iter(cli.ignore.into_iter())
            .map(|ignored_entry| async {
                #[cfg(feature = "regex")]
                if let Some(pattern) = IgnoredEntry::pattern(&ignored_entry).await {
                    return Ok(pattern?);
                }

                let mut path = PathBuf::from(ignored_entry);

                if path.is_relative() {
                    let mut ignored_directory = directory.clone();
                    ignored_directory.push(path);
                    path = ignored_directory;
                }

                fs::canonicalize(&path)
                    .await
                    .map(IgnoredEntry::Exact)
                    .map_err(|error| CliError::IgnoredDirectories(path, error))
            })
            .buffer_unordered(jobs)
            .try_collect::<IgnoredEntries>()
            .await?
            .finish();

        Ok((directory, CliParsed {
            command,
            ignored_subdirectories,
            recursive,
            jobs,
            timeout,
            #[cfg(feature = "json")]
            mode,
        }))
    }
}

impl CliParsed {
    #[cfg(feature = "json")]
    pub fn mode(&self) -> json::Mode {
        self.mode
    }

    pub async fn process(&self,
        root: PathBuf,
        consumer: impl AsyncFnMut(Result<(PathBuf, Output), ProcessError>),
    ) {
        if self.recursive {
            self.process_recursive(root, consumer).await;
        } else {
            self.process_standard(root, consumer).await;
        }
    }

    async fn process_standard(
        &self,
        root: PathBuf,
        mut consumer: impl AsyncFnMut(Result<(PathBuf, Output), ProcessError>),
    ) {
        let directory = match CliParsed::process_directory(&root, &self.ignored_subdirectories).await {
            Ok(root_directory) => root_directory,
            Err(error) => {
                consumer(Err(error)).await;
                return;
            },
        };

        let outputs = directory
            .map(|entry| async { CliParsed::process_output(entry?, &self.command.0, &self.command.1, self.timeout).await })
            .buffer_unordered(self.jobs);

        let mut outputs = pin::pin!(outputs);

        while let Some(yielded) = outputs.next().await {
            consumer(yielded).await;
        }
    }

    async fn process_recursive(
        &self,
        root: PathBuf,
        mut consumer: impl AsyncFnMut(Result<(PathBuf, Output), ProcessError>),
    ) {
        let root_directory = match CliParsed::process_directory(&root, &self.ignored_subdirectories).await {
            Ok(root_directory) => root_directory,
            Err(error) => {
                consumer(Err(error)).await;
                return;
            },
        };

        let mut directories = Vec::from([Box::pin(root_directory)]);
        let mut outputs = FuturesUnordered::new();

        let root_output = CliParsed::process_output(root, &self.command.0, &self.command.1, self.timeout);
        outputs.push(root_output);

        while let Some(current_directory) = directories.last_mut() {
            let entry = match current_directory.next().await {
                Some(Ok(entry)) => entry,
                Some(Err(error)) => {
                    consumer(Err(error)).await;
                    continue;
                },
                None => {
                    directories.pop();
                    continue;
                },
            };

            directories.push(match CliParsed::process_directory(&entry, &self.ignored_subdirectories).await {
                Ok(subdirectory) => Box::pin(subdirectory),
                Err(error) => {
                    consumer(Err(error)).await;
                    continue;
                },
            });

            if outputs.len() >= self.jobs {
                let yielded = outputs
                    .next()
                    .await
                    .unwrap();

                consumer(yielded).await;

                while let Some(yielded) = future::poll_fn(|ctx| match outputs.poll_next_unpin(ctx) {
                    Poll::Ready(Some(yielded)) => Poll::Ready(Some(yielded)),
                    _ => Poll::Ready(None),
                }).await {
                    consumer(yielded).await;
                }
            }

            outputs.push(CliParsed::process_output(entry, &self.command.0, &self.command.1, self.timeout));
        }

        while let Some(yielded) = outputs.next().await {
            consumer(yielded).await;
        }
    }

    async fn process_directory<'a>(path: &Path, ignored: &'a IgnoredEntries) -> Result<impl Stream<Item = Result<PathBuf, ProcessError>> + use<'a>, ProcessError> {
        let directory = match fs::read_dir(path).await {
            Ok(directory) => directory,
            Err(error) => return Err(ProcessError::SubDirectories {
                entry: path.to_owned(),
                origin: error,
            }),
        };

        let directory = ReadDirStream::new(directory)
            .filter_map(|entry| async {
                let Ok(entry) = entry else {
                    return Some(Err(ProcessError::ModifiedEntry));
                };

                let Ok(metadata) = entry.metadata().await else {
                    return Some(Err(ProcessError::ModifiedEntry));
                };

                let path = entry.path();

                match (metadata.is_dir(), ignored.as_valid(&path).await) {
                    (false, _) | (_, Ok(None)) => None,
                    (true, Err(error)) => Some(Err(error)),
                    (true, Ok(Some(path_canonicalized))) => Some(Ok(path_canonicalized)),
                }
            });

        Ok(directory)
    }

    async fn process_output(path: PathBuf, command: &OsStr, args: impl IntoIterator<Item = impl AsRef<OsStr>>, timeout: Option<Duration>) -> Result<(PathBuf, Output), ProcessError> {
        let child = match Command::new(command)
            .current_dir(&path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::null())
            .args(args)
            .spawn()
        {
            Ok(child) => child,
            Err(error) => return Err(ProcessError::ProcessSpawn {
                process: command.to_owned(),
                entry: path,
                origin: error,
            }),
        };

        let output = match timeout {
            Some(duration) => match time::timeout(duration, child.wait_with_output()).await {
                Ok(output) => output,
                Err(_) => return Err(ProcessError::Timeout {
                    process: command.to_owned(),
                    entry: path,
                    duration: humantime::format_duration(duration).to_string(),
                }),
            },
            None => child.wait_with_output().await,
        };

        match output {
            Ok(output) => Ok((path, output)),
            Err(error) => Err(ProcessError::ProcessOutput {
                process: command.to_owned(),
                entry: path,
                origin: error,
            }),
        }
    }
}

impl IgnoredEntries {
    fn insert(&mut self, entry: IgnoredEntry) {
        match entry {
            #[cfg(feature = "regex")]
            IgnoredEntry::Pattern(pattern) => self.pattern.push(pattern),
            IgnoredEntry::Exact(path) => {
                self.exact.insert(path);
            },
        };
    }

    fn finish(self) -> IgnoredEntries {
        IgnoredEntries {
            exact: self.exact,
            #[cfg(feature = "regex")]
            pattern: self.pattern.compiled(),
        }
    }

    async fn as_valid(&self, path: &Path) -> Result<Option<PathBuf>, ProcessError> {
        let path = fs::canonicalize(path)
            .await
            .map_err(|_| ProcessError::ModifiedEntry)?;

        if self.exact.contains(&path) {
            return Ok(None);
        }

        #[cfg(feature = "regex")]
        {
            let file_name = path
                .file_name()
                .unwrap()
                .to_string_lossy();

            if self.pattern.is_match(file_name.as_ref()) {
                return Ok(None);
            }
        }

        Ok(Some(path))
    }
}

impl Extend<IgnoredEntry> for IgnoredEntries {
    fn extend<T: IntoIterator<Item = IgnoredEntry>>(&mut self, iter: T) {
        for entry in iter {
            self.insert(entry);
        }
    }
}
