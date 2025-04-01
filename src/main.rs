use std::{
    path::PathBuf,
    process::Output
};

use anyhow::{Context, Error};
use subdo::{Cli, CliParsed, ProcessError};

use tokio::{
    io::{self, AsyncWriteExt, Stderr, Stdout},
    sync::{Mutex, MutexGuard}
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let (entries, cli) = Cli::parse()
        .await
        .context("Failed to parse CLI")?;

    #[cfg(feature = "json")]
    match cli.mode() {
        subdo::json::Mode::Json => return execute_json(&cli, entries, serde_json::to_string).await,
        subdo::json::Mode::JsonPretty => return execute_json(&cli, entries, serde_json::to_string_pretty).await,
        subdo::json::Mode::Standard => (),
    }

    execute_standard(&cli, entries).await;
    Ok(())
}

async fn execute_standard(cli: &CliParsed, entries: PathBuf) {
    let consume = async |
        processed: Result<(PathBuf, Output), ProcessError>,
        stdout: &mut MutexGuard<'_, Stdout>,
        stderr: &mut MutexGuard<'_, Stderr>,
    | {
        let (entry, output) = match processed {
            Ok(processed) => processed,
            Err(error) => {
                let error = Error::from(error).context("Failed to execute command");
                subdo::async_write!(stderr, "{:?}\n", error);
                return;
            },
        };

        subdo::async_write!(stdout, "{}:\n", entry.display());
        subdo::async_write!(as [u8] => stdout, &output.stdout);

        if output.stderr.is_empty() {
            return;
        }

        subdo::async_write!(stderr, "\nWarning:\n");
        subdo::async_write!(as [u8] => stderr, &output.stderr);
    };

    let consumer = async |
        processed: Result<(PathBuf, Output), ProcessError>,
        stdout: &Mutex<Stdout>,
        stderr: &Mutex<Stderr>,
        first_write: &mut bool,
    | {
        let mut stdout = stdout.lock().await;
        let mut stderr = stderr.lock().await;

        if !*first_write {
            subdo::async_write!(stdout, "\n");
        }

        consume(processed, &mut stdout, &mut stderr).await;
        *first_write = false;
    };

    let finish = async |
        stdout: &Mutex<Stdout>,
        stderr: &Mutex<Stderr>,
    | {
        subdo::async_write!(flush => stdout.lock().await);
        subdo::async_write!(flush => stderr.lock().await);
    };

    let mut first_write = true;
    let stdout = &Mutex::new(io::stdout());
    let stderr = &Mutex::new(io::stderr());

    cli.process(entries, async |processed| {
        consumer(processed, stdout, stderr, &mut first_write).await
    })
    .await;

    finish(stdout, stderr).await;
}

#[cfg(feature = "json")]
async fn execute_json<
    F: FnOnce(&subdo::json::ProcessedEntries) -> Result<String, serde_json::Error>,
>(cli: &CliParsed, entries: PathBuf, formatter: F) -> anyhow::Result<()>
{
    let consumer = async |
        processed: Result<(PathBuf, Output), ProcessError>,
        processed_entries: &mut subdo::json::ProcessedEntries,
    | {
        processed_entries.insert(processed);
    };

    let finish = async |
        processed_entries: &subdo::json::ProcessedEntries,
    | {
        let json = formatter(processed_entries).context("Failed to JSONify outputs")?;

        let mut stdout = io::stdout();
        subdo::async_write!(stdout, "{}\n", json);
        subdo::async_write!(flush => stdout);

        Ok::<_, Error>(())
    };

    let mut processed_entries = subdo::json::ProcessedEntries::default();

    cli.process(entries, async |processed| {
        consumer(processed, &mut processed_entries).await;
    })
    .await;

    finish(&processed_entries).await
}
