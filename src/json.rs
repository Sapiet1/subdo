use std::{
    collections::HashMap,
    path::PathBuf,
    process::Output,
};

use crate::ProcessError;
use clap::ValueEnum;
use serde::Serialize;

#[derive(ValueEnum, Clone, Copy)]
pub enum Mode {
    Json,
    JsonPretty,
    Standard,
}

#[derive(Default, Serialize)]
pub struct ProcessedEntries {
    unknown: usize,
    processed: HashMap<PathBuf, ProcessedEntry>,
}

#[derive(Serialize)]
#[serde(tag = "status")]
pub enum ProcessedEntry {
    Error(ProcessedError),
    Ok { stdout: String, stderr: String },
}

#[derive(Serialize)]
#[serde(tag = "type")]
pub enum ProcessedError {
    Spawn { process: String, code: Option<i32>, description: String },
    Output { process: String, code: Option<i32>, description: String },
    Timeout { process: String, duration: String },
    SubDirectories { code: Option<i32>, description: String }
}

impl ProcessedEntries {
    pub fn insert(&mut self, processed: Result<(PathBuf, Output), ProcessError>) {
        match processed {
            Ok((entry, output)) => {
                self.processed.insert(entry, ProcessedEntry::Ok {
                    stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                });
            },
            Err(ProcessError::ModifiedEntry) => self.unknown += 1,
            Err(ProcessError::ProcessSpawn { process, entry, origin }) => {
                self.processed.insert(entry, ProcessedEntry::Error(ProcessedError::Spawn {
                    process: process.to_string_lossy().into_owned(),
                    code: origin.raw_os_error(),
                    description: origin.to_string(),
                }));
            },
            Err(ProcessError::ProcessOutput { process, entry, origin }) => {
                self.processed.insert(entry, ProcessedEntry::Error(ProcessedError::Output {
                    process: process.to_string_lossy().into_owned(),
                    code: origin.raw_os_error(),
                    description: origin.to_string(),
                }));
            },
            Err(ProcessError::Timeout { process, entry, duration }) => {
                self.processed.insert(entry, ProcessedEntry::Error(ProcessedError::Timeout {
                    process: process.to_string_lossy().into_owned(),
                    duration,
                }));
            },
            Err(ProcessError::SubDirectories { entry, origin }) => {
                self.processed.insert(entry, ProcessedEntry::Error(ProcessedError::SubDirectories {
                    code: origin.raw_os_error(),
                    description: origin.to_string(),
                }));
            },
        }
    }
}

impl Extend<Result<(PathBuf, Output), ProcessError>> for ProcessedEntries {
    fn extend<T: IntoIterator<Item = Result<(PathBuf, Output), ProcessError>>>(&mut self, iter: T) {
        for processed in iter {
            self.insert(processed);
        }
    }
}
