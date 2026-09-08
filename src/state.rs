use crate::executor::ExecutorResult;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const ATTEMPTS_DIRECTORY: &str = "attempts";
const LOCK_FILE: &str = "runner.lock";
const RECORD_VERSION: u8 = 1;
const MAX_TASK_KEY_BYTES: usize = 512;
const MAX_TASK_REVISION_BYTES: usize = 64;
const MAX_RECORD_BYTES: u64 = 128 * 1024;

#[derive(Clone)]
pub(crate) struct AttemptStore {
    state_directory: PathBuf,
}

pub(crate) struct LockedAttemptStore {
    attempts_directory: PathBuf,
    _lock: File,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedAttempt {
    version: u8,
    run_id: String,
    task_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    task_revision: Option<String>,
    state: AttemptState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<ExecutorResult>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum AttemptState {
    Prepared,
    LaunchIntent,
    ResultStored,
    Finalized,
}

impl AttemptStore {
    pub(crate) fn new(state_directory: PathBuf) -> Self {
        Self { state_directory }
    }

    pub(crate) fn acquire(&self) -> Result<LockedAttemptStore, String> {
        crate::config::ensure_private_directory(&self.state_directory)
            .map_err(|_| "cannot prepare private state directory".to_owned())?;
        let attempts_directory = self.state_directory.join(ATTEMPTS_DIRECTORY);
        crate::config::ensure_private_directory(&attempts_directory)
            .map_err(|_| "cannot prepare private attempts directory".to_owned())?;
        let lock = open_private_lock(&self.state_directory.join(LOCK_FILE))?;
        lock_exclusively(&lock)?;
        Ok(LockedAttemptStore {
            attempts_directory,
            _lock: lock,
        })
    }
}

impl LockedAttemptStore {
    pub(crate) fn prepare(&self, task_key: &str) -> Result<PreparedAttempt, String> {
        self.prepare_for_revision(task_key, None)
    }

    pub(crate) fn prepare_revision(
        &self,
        task_key: &str,
        task_revision: &str,
    ) -> Result<PreparedAttempt, String> {
        validate_task_revision(task_revision)?;
        self.prepare_for_revision(task_key, Some(task_revision))
    }

    fn prepare_for_revision(
        &self,
        task_key: &str,
        task_revision: Option<&str>,
    ) -> Result<PreparedAttempt, String> {
        validate_task_key(task_key)?;
        loop {
            let run_id = generate_run_id()?;
            let attempt = PreparedAttempt {
                version: RECORD_VERSION,
                run_id: run_id.clone(),
                task_key: task_key.to_owned(),
                task_revision: task_revision.map(str::to_owned),
                state: AttemptState::Prepared,
                completed_at: None,
                result: None,
            };
            match persist_new_record(&self.attempts_directory, &run_id, &attempt) {
                Ok(()) => return Ok(attempt),
                Err(PersistError::Collision) => continue,
                Err(PersistError::Failure(message)) => return Err(message),
            }
        }
    }

    pub(crate) fn load(&self, run_id: &str) -> Result<PreparedAttempt, String> {
        validate_run_id(run_id)?;
        let path = self.attempts_directory.join(format!("{run_id}.json"));
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    "prepared attempt does not exist".to_owned()
                } else {
                    "cannot safely open prepared attempt".to_owned()
                }
            })?;
        validate_private_file(&file, "prepared attempt")?;
        if file
            .metadata()
            .map_err(|_| "cannot inspect prepared attempt".to_owned())?
            .len()
            > MAX_RECORD_BYTES
        {
            return Err("prepared attempt is oversized".to_owned());
        }
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)
            .map_err(|_| "cannot read prepared attempt".to_owned())?;
        let attempt: PreparedAttempt = serde_json::from_slice(&contents)
            .map_err(|_| "prepared attempt is invalid".to_owned())?;
        if attempt.version != RECORD_VERSION
            || attempt.run_id != run_id
            || validate_task_key(&attempt.task_key).is_err()
            || attempt
                .task_revision
                .as_deref()
                .is_some_and(|revision| validate_task_revision(revision).is_err())
            || !attempt.has_valid_state()
        {
            return Err("prepared attempt is invalid".to_owned());
        }
        Ok(attempt)
    }

    pub(crate) fn list_prepared(&self) -> Result<Vec<PreparedAttempt>, String> {
        let entries = fs::read_dir(&self.attempts_directory)
            .map_err(|_| "cannot list prepared attempts".to_owned())?;
        let mut run_ids = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|_| "cannot list prepared attempts".to_owned())?;
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| "prepared attempt filename is invalid".to_owned())?;
            if name.starts_with('.') && name.ends_with(".tmp") {
                continue;
            }
            let run_id = name
                .strip_suffix(".json")
                .ok_or_else(|| "prepared attempt filename is invalid".to_owned())?;
            validate_run_id(run_id)?;
            run_ids.push(run_id.to_owned());
        }
        run_ids.sort_unstable();
        run_ids.iter().map(|run_id| self.load(run_id)).collect()
    }

    pub(crate) fn task_revision_has_launch_intent(
        &self,
        task_key: &str,
        task_revision: &str,
    ) -> Result<bool, String> {
        validate_task_key(task_key)?;
        validate_task_revision(task_revision)?;
        Ok(self.list_prepared()?.iter().any(|attempt| {
            attempt.task_key == task_key
                && attempt.state != AttemptState::Prepared
                && attempt
                    .task_revision
                    .as_deref()
                    .is_none_or(|known| known == task_revision)
        }))
    }

    pub(crate) fn prepared_for_revision(
        &self,
        task_key: &str,
        task_revision: &str,
    ) -> Result<Option<PreparedAttempt>, String> {
        validate_task_key(task_key)?;
        validate_task_revision(task_revision)?;
        let mut matching = self.list_prepared()?.into_iter().filter(|attempt| {
            attempt.task_key == task_key
                && attempt.task_revision.as_deref() == Some(task_revision)
                && attempt.state == AttemptState::Prepared
        });
        let prepared = matching.next();
        if matching.next().is_some() {
            return Err("prepared attempt authority is ambiguous".to_owned());
        }
        Ok(prepared)
    }

    pub(crate) fn prepared_before_launch(&self) -> Result<Vec<PreparedAttempt>, String> {
        let prepared: Vec<_> = self
            .list_prepared()?
            .into_iter()
            .filter(|attempt| attempt.state == AttemptState::Prepared)
            .collect();
        let mut authorities = HashSet::new();
        if prepared.iter().any(|attempt| {
            !authorities.insert((attempt.task_key.as_str(), attempt.task_revision.as_deref()))
        }) {
            return Err("prepared attempt authority is ambiguous".to_owned());
        }
        Ok(prepared)
    }

    pub(crate) fn launch_intents(&self) -> Result<Vec<PreparedAttempt>, String> {
        Ok(self
            .list_prepared()?
            .into_iter()
            .filter(|attempt| attempt.state == AttemptState::LaunchIntent)
            .collect())
    }

    pub(crate) fn record_launch_intent(&self, run_id: &str) -> Result<(), String> {
        let mut attempt = self.load(run_id)?;
        if attempt.state != AttemptState::Prepared || attempt.result.is_some() {
            return Err("prepared attempt is not launchable".to_owned());
        }
        attempt.state = AttemptState::LaunchIntent;
        replace_record(&self.attempts_directory, run_id, &attempt)
    }

    pub(crate) fn store_result(
        &self,
        run_id: &str,
        completed_at: &str,
        result: ExecutorResult,
    ) -> Result<(), String> {
        if !result.is_valid() {
            return Err("executor result is invalid".to_owned());
        }
        validate_completed_at(completed_at)?;
        let mut attempt = self.load(run_id)?;
        if attempt.state != AttemptState::LaunchIntent || attempt.result.is_some() {
            return Err("executor result cannot be stored for this attempt".to_owned());
        }
        attempt.state = AttemptState::ResultStored;
        attempt.result = Some(result);
        attempt.completed_at = Some(completed_at.to_owned());
        replace_record(&self.attempts_directory, run_id, &attempt)
    }

    pub(crate) fn result_stored(&self) -> Result<Vec<PreparedAttempt>, String> {
        Ok(self
            .list_prepared()?
            .into_iter()
            .filter(|attempt| attempt.state == AttemptState::ResultStored)
            .collect())
    }

    pub(crate) fn mark_finalized(&self, run_id: &str) -> Result<(), String> {
        let mut attempt = self.load(run_id)?;
        if attempt.state != AttemptState::ResultStored || attempt.result.is_none() {
            return Err("attempt cannot be marked finalized".to_owned());
        }
        attempt.state = AttemptState::Finalized;
        replace_record(&self.attempts_directory, run_id, &attempt)
    }
}

impl PreparedAttempt {
    fn has_valid_state(&self) -> bool {
        match self.state {
            AttemptState::Prepared | AttemptState::LaunchIntent => {
                self.result.is_none() && self.completed_at.is_none()
            }
            AttemptState::ResultStored | AttemptState::Finalized => {
                self.result.as_ref().is_some_and(ExecutorResult::is_valid)
                    && self
                        .completed_at
                        .as_deref()
                        .is_some_and(|value| validate_completed_at(value).is_ok())
            }
        }
    }

    pub(crate) fn run_id(&self) -> &str {
        &self.run_id
    }

    pub(crate) fn task_key(&self) -> &str {
        &self.task_key
    }

    pub(crate) fn task_revision(&self) -> Option<&str> {
        self.task_revision.as_deref()
    }

    pub(crate) fn result(&self) -> Option<&ExecutorResult> {
        self.result.as_ref()
    }

    pub(crate) fn completed_at(&self) -> Option<&str> {
        self.completed_at.as_deref()
    }
}

fn validate_completed_at(value: &str) -> Result<(), String> {
    OffsetDateTime::parse(value, &Rfc3339)
        .map(|_| ())
        .map_err(|_| "attempt completion time is invalid".to_owned())
}

fn replace_record(directory: &Path, run_id: &str, attempt: &PreparedAttempt) -> Result<(), String> {
    let temporary_path = directory.join(format!(".{run_id}.tmp"));
    let final_path = directory.join(format!("{run_id}.json"));
    let _ = fs::remove_file(&temporary_path);
    let mut temporary = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temporary_path)
        .map_err(|_| "cannot update prepared attempt".to_owned())?;
    let outcome = (|| {
        serde_json::to_writer(&mut temporary, attempt)
            .map_err(|_| "cannot encode prepared attempt".to_owned())?;
        temporary
            .write_all(b"\n")
            .and_then(|_| temporary.sync_all())
            .map_err(|_| "cannot persist prepared attempt".to_owned())?;
        fs::rename(&temporary_path, &final_path)
            .map_err(|_| "cannot publish prepared attempt update".to_owned())?;
        sync_directory(directory).map_err(|_| "cannot persist prepared attempt".to_owned())
    })();
    if outcome.is_err() {
        let _ = fs::remove_file(temporary_path);
    }
    outcome
}

fn open_private_lock(path: &Path) -> Result<File, String> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| "cannot safely open runner process lock".to_owned())?;
    validate_regular_file(&file, "runner process lock")?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|_| "cannot secure runner process lock".to_owned())?;
    Ok(file)
}

fn lock_exclusively(file: &File) -> Result<(), String> {
    // SAFETY: flock only observes the valid descriptor retained by `file`.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.kind() == std::io::ErrorKind::WouldBlock {
        Err("another runner already holds the state directory lock".to_owned())
    } else {
        Err("cannot acquire runner process lock".to_owned())
    }
}

enum PersistError {
    Collision,
    Failure(String),
}

fn persist_new_record(
    directory: &Path,
    run_id: &str,
    attempt: &PreparedAttempt,
) -> Result<(), PersistError> {
    let temporary_path = directory.join(format!(".{run_id}.tmp"));
    let final_path = directory.join(format!("{run_id}.json"));
    let mut temporary = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temporary_path)
        .map_err(|_| PersistError::Failure("cannot create prepared attempt".to_owned()))?;
    let result = (|| {
        serde_json::to_writer(&mut temporary, attempt)
            .map_err(|_| PersistError::Failure("cannot encode prepared attempt".to_owned()))?;
        temporary
            .write_all(b"\n")
            .and_then(|_| temporary.sync_all())
            .map_err(|_| PersistError::Failure("cannot persist prepared attempt".to_owned()))?;
        fs::hard_link(&temporary_path, &final_path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                PersistError::Collision
            } else {
                PersistError::Failure("cannot publish prepared attempt".to_owned())
            }
        })?;
        sync_directory(directory)
            .map_err(|_| PersistError::Failure("cannot persist prepared attempt".to_owned()))?;
        Ok(())
    })();
    let _ = fs::remove_file(&temporary_path);
    let _ = sync_directory(directory);
    result
}

fn sync_directory(path: &Path) -> std::io::Result<()> {
    File::open(path)?.sync_all()
}

fn validate_regular_file(file: &File, description: &str) -> Result<(), String> {
    let metadata = file
        .metadata()
        .map_err(|_| format!("cannot inspect {description}"))?;
    if !metadata.is_file() {
        return Err(format!("{description} is not a regular file"));
    }
    Ok(())
}

fn validate_private_file(file: &File, description: &str) -> Result<(), String> {
    validate_regular_file(file, description)?;
    let mode = file
        .metadata()
        .map_err(|_| format!("cannot inspect {description}"))?
        .mode()
        & 0o777;
    if mode != 0o600 {
        return Err(format!("{description} is not private"));
    }
    Ok(())
}

fn validate_task_key(task_key: &str) -> Result<(), String> {
    if task_key.is_empty()
        || task_key.len() > MAX_TASK_KEY_BYTES
        || task_key.chars().any(char::is_control)
    {
        return Err(
            "task key must be non-empty, bounded, and contain no control characters".to_owned(),
        );
    }
    Ok(())
}

fn validate_task_revision(revision: &str) -> Result<(), String> {
    if revision.is_empty() || revision.len() > MAX_TASK_REVISION_BYTES || !revision.is_ascii() {
        return Err("task revision must be non-empty, bounded, and ASCII".to_owned());
    }
    Ok(())
}

fn generate_run_id() -> Result<String, String> {
    let mut bytes = [0_u8; 16];
    let mut filled = 0;
    while filled < bytes.len() {
        // SAFETY: the remaining slice is valid writable memory for the given
        // byte count; getrandom does not retain the pointer.
        let result = unsafe {
            libc::getrandom(bytes[filled..].as_mut_ptr().cast(), bytes.len() - filled, 0)
        };
        if result > 0 {
            filled += result as usize;
        } else if result == -1
            && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
        {
            continue;
        } else {
            return Err("cannot generate run ID".to_owned());
        }
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    ))
}

fn validate_run_id(run_id: &str) -> Result<(), String> {
    let bytes = run_id.as_bytes();
    if bytes.len() != 36
        || bytes[8] != b'-'
        || bytes[13] != b'-'
        || bytes[18] != b'-'
        || bytes[23] != b'-'
        || bytes
            .iter()
            .enumerate()
            .any(|(index, byte)| !matches!(index, 8 | 13 | 18 | 23) && !byte.is_ascii_hexdigit())
    {
        return Err("run ID is invalid".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Outcome;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    fn temporary_directory() -> PathBuf {
        let suffix = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "agent-handover-state-test-{}-{suffix}",
            std::process::id()
        ))
    }

    fn successful_result() -> ExecutorResult {
        ExecutorResult {
            outcome: Outcome::Done,
            summary: "completed".to_owned(),
            actions: vec!["updated placeholder".to_owned()],
            warnings: Vec::new(),
        }
    }

    #[test]
    fn rejects_a_second_runner_until_the_first_releases_the_lock() {
        let directory = temporary_directory();
        let first_store = AttemptStore::new(directory.clone());
        let second_store = AttemptStore::new(directory.clone());

        let first_lock = first_store.acquire().unwrap();
        assert_eq!(
            second_store.acquire().err().as_deref(),
            Some("another runner already holds the state directory lock")
        );
        drop(first_lock);
        second_store.acquire().unwrap();

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn prepared_attempt_survives_a_new_store_instance() {
        let directory = temporary_directory();
        let expected_run_id = {
            let locked = AttemptStore::new(directory.clone()).acquire().unwrap();
            let attempt = locked.prepare("task-placeholder").unwrap();
            assert_eq!(attempt.task_key(), "task-placeholder");
            attempt.run_id().to_owned()
        };

        let reopened = AttemptStore::new(directory.clone()).acquire().unwrap();
        let attempts = reopened.list_prepared().unwrap();
        assert_eq!(attempts.len(), 1);
        let attempt = &attempts[0];
        assert_eq!(attempt.run_id(), expected_run_id);
        assert_eq!(attempt.task_key(), "task-placeholder");
        drop(reopened);

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn every_prepared_attempt_has_a_distinct_immutable_run_id() {
        let directory = temporary_directory();
        let locked = AttemptStore::new(directory.clone()).acquire().unwrap();

        let first = locked.prepare("task-placeholder").unwrap();
        let second = locked.prepare("task-placeholder").unwrap();

        assert_ne!(first.run_id(), second.run_id());
        assert!(locked.load(first.run_id()).is_ok());
        assert!(locked.load(second.run_id()).is_ok());
        drop(locked);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn reopening_before_launch_intent_preserves_the_first_launch_transition() {
        let directory = temporary_directory();
        let run_id = {
            let locked = AttemptStore::new(directory.clone()).acquire().unwrap();
            locked
                .prepare_revision("task-placeholder", "revision-1")
                .unwrap()
                .run_id()
                .to_owned()
        };

        let reopened = AttemptStore::new(directory.clone()).acquire().unwrap();
        assert!(
            !reopened
                .task_revision_has_launch_intent("task-placeholder", "revision-1")
                .unwrap()
        );
        reopened.record_launch_intent(&run_id).unwrap();
        assert!(
            reopened
                .task_revision_has_launch_intent("task-placeholder", "revision-1")
                .unwrap()
        );
        assert_eq!(
            reopened.record_launch_intent(&run_id).unwrap_err(),
            "prepared attempt is not launchable"
        );
        drop(reopened);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn multiple_prepared_records_for_one_revision_are_never_chosen_implicitly() {
        let directory = temporary_directory();
        let locked = AttemptStore::new(directory.clone()).acquire().unwrap();
        locked
            .prepare_revision("task-placeholder", "revision-1")
            .unwrap();
        locked
            .prepare_revision("task-placeholder", "revision-1")
            .unwrap();

        assert_eq!(
            locked
                .prepared_for_revision("task-placeholder", "revision-1")
                .unwrap_err(),
            "prepared attempt authority is ambiguous"
        );
        drop(locked);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn reopening_after_launch_intent_never_permits_another_launch_transition() {
        let directory = temporary_directory();
        let run_id = {
            let locked = AttemptStore::new(directory.clone()).acquire().unwrap();
            let attempt = locked
                .prepare_revision("task-placeholder", "revision-1")
                .unwrap();
            locked.record_launch_intent(attempt.run_id()).unwrap();
            attempt.run_id().to_owned()
        };

        let reopened = AttemptStore::new(directory.clone()).acquire().unwrap();
        assert!(
            reopened
                .task_revision_has_launch_intent("task-placeholder", "revision-1")
                .unwrap()
        );
        assert_eq!(
            reopened.record_launch_intent(&run_id).unwrap_err(),
            "prepared attempt is not launchable"
        );
        drop(reopened);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn validated_result_survives_reopening_without_another_launch_transition() {
        let directory = temporary_directory();
        let run_id = {
            let locked = AttemptStore::new(directory.clone()).acquire().unwrap();
            let attempt = locked
                .prepare_revision("task-placeholder", "revision-1")
                .unwrap();
            locked.record_launch_intent(attempt.run_id()).unwrap();
            locked
                .store_result(
                    attempt.run_id(),
                    "2026-01-01T00:01:00Z",
                    successful_result(),
                )
                .unwrap();
            attempt.run_id().to_owned()
        };

        let reopened = AttemptStore::new(directory.clone()).acquire().unwrap();
        assert_eq!(
            reopened.load(&run_id).unwrap().result(),
            Some(&successful_result())
        );
        assert_eq!(
            reopened.record_launch_intent(&run_id).unwrap_err(),
            "prepared attempt is not launchable"
        );
        drop(reopened);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn only_validated_results_can_cross_the_durable_result_boundary() {
        let directory = temporary_directory();
        let locked = AttemptStore::new(directory.clone()).acquire().unwrap();
        let attempt = locked.prepare("task-placeholder").unwrap();
        locked.record_launch_intent(attempt.run_id()).unwrap();
        let mut invalid = successful_result();
        invalid.summary = "  ".to_owned();

        assert_eq!(
            locked
                .store_result(attempt.run_id(), "2026-01-01T00:01:00Z", invalid)
                .unwrap_err(),
            "executor result is invalid"
        );
        let mut oversized = successful_result();
        oversized.actions = vec!["x".repeat(64 * 1024)];
        assert_eq!(
            locked
                .store_result(attempt.run_id(), "2026-01-01T00:01:00Z", oversized)
                .unwrap_err(),
            "executor result is invalid"
        );
        assert_eq!(
            locked
                .store_result(attempt.run_id(), "not-a-timestamp", successful_result())
                .unwrap_err(),
            "attempt completion time is invalid"
        );
        assert!(locked.load(attempt.run_id()).unwrap().result().is_none());
        drop(locked);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn inconsistent_persisted_attempt_states_fail_with_content_free_diagnostics() {
        let directory = temporary_directory();
        let locked = AttemptStore::new(directory.clone()).acquire().unwrap();
        let attempt = locked.prepare("task-placeholder").unwrap();
        let path = directory
            .join(ATTEMPTS_DIRECTORY)
            .join(format!("{}.json", attempt.run_id()));
        let mut record = serde_json::to_value(&attempt).unwrap();
        record["state"] = serde_json::json!("result_stored");
        fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();

        assert_eq!(
            locked.load(attempt.run_id()).unwrap_err(),
            "prepared attempt is invalid"
        );
        drop(locked);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn prepared_record_is_private_and_contains_only_local_authority_fields() {
        let directory = temporary_directory();
        let locked = AttemptStore::new(directory.clone()).acquire().unwrap();
        let attempt = locked.prepare("task-placeholder").unwrap();
        let path = directory
            .join(ATTEMPTS_DIRECTORY)
            .join(format!("{}.json", attempt.run_id()));
        let metadata = fs::metadata(&path).unwrap();
        let contents = fs::read_to_string(path).unwrap();

        assert_eq!(metadata.mode() & 0o777, 0o600);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&contents).unwrap(),
            serde_json::json!({
                "version": 1,
                "run_id": attempt.run_id(),
                "task_key": "task-placeholder",
                "state": "prepared"
            })
        );
        for excluded in ["prompt", "instruction", "output", "secret", "/home/"] {
            assert!(!contents.contains(excluded));
        }
        drop(locked);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn state_diagnostics_do_not_expose_host_specific_paths() {
        let directory = temporary_directory();
        let state_path = directory.join("host-specific-value");
        fs::create_dir_all(&directory).unwrap();
        File::create(&state_path).unwrap();

        let error = AttemptStore::new(state_path).acquire().err().unwrap();

        assert_eq!(error, "cannot prepare private state directory");
        assert!(!error.contains("host-specific-value"));
        fs::remove_dir_all(directory).unwrap();
    }
}
