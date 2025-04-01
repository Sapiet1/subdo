use std::{
    ffi::OsStr,
    mem,
};

use crate::IgnoredEntry;
use regex::Regex;
use thiserror::Error;
use tokio::task;


#[derive(Error, Debug)]
pub enum PatternError {
    #[error("the pattern ({0}) is invalid")]
    Invalid(String),
    #[error("the pattern ({0}) could not compile")]
    Miscompilation(String),
}

pub(crate) enum IgnoredPattern {
    Compiled(Regex),
    Raw(Vec<Regex>),
}

impl IgnoredEntry {
    pub(crate) async fn pattern(ignored_entry: &OsStr) -> Option<Result<IgnoredEntry, PatternError>> {
        let [b'#', pattern @ ..] = ignored_entry.as_encoded_bytes() else {
            return None;
        };

        let pattern = String::from_utf8_lossy(pattern).into_owned();

        let handle = task::spawn_blocking({
            let pattern = pattern.clone();
            move || Regex::new(&pattern)
        });

        Some(match handle.await {
            Ok(Ok(pattern)) => Ok(IgnoredEntry::Pattern(pattern)),
            Ok(Err(_)) => Err(PatternError::Invalid(pattern)),
            Err(_) => Err(PatternError::Miscompilation(pattern)),
        })
    }
}

impl IgnoredPattern {
    pub(crate) fn is_match(&self, input: &str) -> bool {
        match self {
            IgnoredPattern::Compiled(pattern) => !pattern.as_str().is_empty() && pattern.is_match(input),
            IgnoredPattern::Raw(patterns) => {
                patterns
                    .iter()
                    .any(|pattern| pattern.is_match(input))
            },
        }
    }

    pub(crate) fn push(&mut self, pattern: Regex) {
        match mem::take(self) {
            IgnoredPattern::Compiled(base) => *self = IgnoredPattern::Raw(vec![base, pattern]),
            IgnoredPattern::Raw(mut base) => {
                base.push(pattern);
                *self = IgnoredPattern::Raw(base);
            },
        }
    }

    pub(crate) fn compiled(self) -> IgnoredPattern {
        let IgnoredPattern::Raw(base) = &self else {
            return self;
        };

        let mut patterns = base
            .iter()
            .map(Regex::as_str)
            .filter(|pattern| !pattern.is_empty());

        let mut pattern_completed = String::new();

        if let Some(pattern) = patterns.next() {
            pattern_completed.push_str(pattern);

            for pattern in patterns {
                pattern_completed.push('|');
                pattern_completed.push_str(pattern);
            }
        }

        match Regex::new(&pattern_completed) {
            Ok(pattern_completed) => IgnoredPattern::Compiled(pattern_completed),
            Err(_) => self,
        }
    }
}

impl Default for IgnoredPattern {
    fn default() -> Self {
        IgnoredPattern::Raw(Vec::new())
    }
}
