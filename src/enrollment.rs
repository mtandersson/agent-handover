use serde::Deserialize;
use std::fs;
use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const TOKEN_FILE: &str = "notion-webhook-verification-token";
static NEXT_TEMPORARY_FILE: AtomicU64 = AtomicU64::new(0);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VerificationPayload {
    verification_token: String,
}

pub trait TokenStore {
    fn persist(&self, token: &str, rotate: bool) -> Result<(), String>;
}

pub trait TokenSource {
    fn load(&self) -> Result<String, String>;
}

pub struct FileTokenStore {
    state_directory: PathBuf,
}

impl FileTokenStore {
    pub fn new(state_directory: PathBuf) -> Self {
        Self { state_directory }
    }

    fn persist_with_operations<O: CommitOperations>(
        &self,
        token: &str,
        rotate: bool,
        operations: &O,
    ) -> Result<(), String> {
        crate::config::ensure_private_directory(&self.state_directory)?;
        let destination = self.state_directory.join(TOKEN_FILE);
        inspect_destination(&destination, rotate)?;

        let temporary = temporary_path(&self.state_directory);
        let result = write_private_temporary_file(&temporary, token)
            .and_then(|()| commit_temporary_file(operations, &temporary, &destination, rotate));
        if result.is_err() {
            let _ = operations.remove_file(&temporary);
        }
        result
    }
}

impl TokenStore for FileTokenStore {
    fn persist(&self, token: &str, rotate: bool) -> Result<(), String> {
        self.persist_with_operations(token, rotate, &SystemCommitOperations)
    }
}

impl TokenSource for FileTokenStore {
    fn load(&self) -> Result<String, String> {
        crate::config::ensure_private_directory(&self.state_directory)?;
        let path = self.state_directory.join(TOKEN_FILE);
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    "no Notion webhook verification token is enrolled; run webhook-enroll first"
                        .to_owned()
                } else {
                    format!("cannot safely open webhook token file: {error}")
                }
            })?;
        let metadata = file
            .metadata()
            .map_err(|error| format!("cannot inspect webhook token file: {error}"))?;
        if !metadata.is_file() {
            return Err("webhook token path is not a regular file".to_owned());
        }
        let mode = metadata.mode() & 0o777;
        if mode != 0o600 {
            return Err(format!(
                "webhook token file must have mode 0600, not {mode:04o}"
            ));
        }
        let mut token = String::new();
        file.read_to_string(&mut token)
            .map_err(|error| format!("cannot read webhook token file: {error}"))?;
        if token.is_empty() {
            return Err("webhook token file is empty; rotate the enrolled token".to_owned());
        }
        Ok(token)
    }
}

trait CommitOperations {
    fn hard_link(&self, source: &Path, destination: &Path) -> std::io::Result<()>;
    fn rename(&self, source: &Path, destination: &Path) -> std::io::Result<()>;
    fn remove_file(&self, path: &Path) -> std::io::Result<()>;
    fn sync_directory(&self, path: &Path) -> std::io::Result<()>;
}

struct SystemCommitOperations;

impl CommitOperations for SystemCommitOperations {
    fn hard_link(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        fs::hard_link(source, destination)
    }

    fn rename(&self, source: &Path, destination: &Path) -> std::io::Result<()> {
        fs::rename(source, destination)
    }

    fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        fs::remove_file(path)
    }

    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        fs::File::open(path)?.sync_all()
    }
}

pub fn enroll<R: Read, S: TokenStore>(
    mut input: R,
    store: &S,
    rotate: bool,
) -> Result<String, String> {
    let mut contents = String::new();
    input
        .read_to_string(&mut contents)
        .map_err(|_| "cannot read webhook verification payload from standard input".to_owned())?;
    let payload: VerificationPayload = serde_json::from_str(&contents).map_err(|_| {
        "invalid webhook verification payload: expected JSON with verification_token".to_owned()
    })?;
    if payload.verification_token.trim().is_empty() {
        return Err(
            "invalid webhook verification payload: verification_token must not be empty".to_owned(),
        );
    }

    store.persist(&payload.verification_token, rotate)?;
    Ok(if rotate {
        "Notion webhook verification token rotated".to_owned()
    } else {
        "Notion webhook verification token enrolled".to_owned()
    })
}

fn inspect_destination(path: &Path, rotate: bool) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(format!(
            "refusing unsafe webhook token path {}: symbolic links are not allowed",
            path.display()
        )),
        Ok(metadata) if !metadata.is_file() => Err(format!(
            "refusing unsafe webhook token path {}: expected a regular file",
            path.display()
        )),
        Ok(metadata) if metadata.mode() & 0o777 != 0o600 => Err(format!(
            "webhook token file {} must have mode 0600 before rotation",
            path.display()
        )),
        Ok(_) if !rotate => Err(
            "a Notion webhook verification token is already enrolled; use --rotate to replace it"
                .to_owned(),
        ),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "cannot inspect webhook token file {}: {error}",
            path.display()
        )),
    }
}

fn temporary_path(directory: &Path) -> PathBuf {
    let sequence = NEXT_TEMPORARY_FILE.fetch_add(1, Ordering::Relaxed);
    directory.join(format!(
        ".notion-webhook-verification-token.tmp-{}-{sequence}",
        std::process::id()
    ))
}

fn write_private_temporary_file(temporary: &Path, token: &str) -> Result<(), String> {
    use std::io::Write;

    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(temporary)
        .map_err(|error| format!("cannot create private webhook token file: {error}"))?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|error| format!("cannot secure private webhook token file: {error}"))?;
    file.write_all(token.as_bytes())
        .and_then(|_| file.sync_all())
        .map_err(|error| format!("cannot persist webhook verification token: {error}"))?;
    Ok(())
}

fn commit_temporary_file<O: CommitOperations>(
    operations: &O,
    temporary: &Path,
    destination: &Path,
    rotate: bool,
) -> Result<(), String> {
    if rotate {
        operations
            .rename(temporary, destination)
            .map_err(|error| format!("cannot rotate webhook verification token: {error}"))?;
    } else {
        operations.hard_link(temporary, destination).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                "a Notion webhook verification token is already enrolled; use --rotate to replace it"
                    .to_owned()
            } else {
                format!("cannot enroll webhook verification token: {error}")
            }
        })?;
        let _ = operations.remove_file(temporary);
    }
    operations
        .sync_directory(destination.parent().expect("token path has a parent"))
        .map_err(|error| {
            let action = if rotate { "replaced" } else { "installed" };
            format!(
                "webhook verification token was {action}, but durability confirmation failed: {error}"
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::io;
    use std::os::unix::fs::{PermissionsExt, symlink};

    #[derive(Default)]
    struct FakeTokenStore {
        writes: RefCell<Vec<(String, bool)>>,
    }

    impl TokenStore for FakeTokenStore {
        fn persist(&self, token: &str, rotate: bool) -> Result<(), String> {
            self.writes.borrow_mut().push((token.to_owned(), rotate));
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingOperations {
        calls: RefCell<Vec<&'static str>>,
        unlink_fails: bool,
        sync_fails: bool,
    }

    impl CommitOperations for RecordingOperations {
        fn hard_link(&self, _source: &Path, _destination: &Path) -> io::Result<()> {
            self.calls.borrow_mut().push("hard_link");
            Ok(())
        }

        fn rename(&self, _source: &Path, _destination: &Path) -> io::Result<()> {
            self.calls.borrow_mut().push("rename");
            Ok(())
        }

        fn remove_file(&self, _path: &Path) -> io::Result<()> {
            self.calls.borrow_mut().push("remove_file");
            if self.unlink_fails {
                Err(io::Error::new(io::ErrorKind::PermissionDenied, "injected"))
            } else {
                Ok(())
            }
        }

        fn sync_directory(&self, _path: &Path) -> io::Result<()> {
            self.calls.borrow_mut().push("sync_directory");
            if self.sync_fails {
                Err(io::Error::other("injected"))
            } else {
                Ok(())
            }
        }
    }

    enum Race {
        CreateDestinationSymlink,
        ReplaceDestinationWithSymlink,
    }

    struct RacingOperations {
        race: Race,
        target: PathBuf,
    }

    impl CommitOperations for RacingOperations {
        fn hard_link(&self, source: &Path, destination: &Path) -> io::Result<()> {
            assert!(matches!(self.race, Race::CreateDestinationSymlink));
            symlink(&self.target, destination)?;
            fs::hard_link(source, destination)
        }

        fn rename(&self, source: &Path, destination: &Path) -> io::Result<()> {
            assert!(matches!(self.race, Race::ReplaceDestinationWithSymlink));
            fs::remove_file(destination)?;
            symlink(&self.target, destination)?;
            fs::rename(source, destination)
        }

        fn remove_file(&self, path: &Path) -> io::Result<()> {
            fs::remove_file(path)
        }

        fn sync_directory(&self, path: &Path) -> io::Result<()> {
            fs::File::open(path)?.sync_all()
        }
    }

    fn temporary_directory() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "agent-handover-enrollment-test-{}-{}",
            std::process::id(),
            NEXT_TEMPORARY_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    #[test]
    fn accepts_the_verification_payload_without_contacting_notion() {
        let store = FakeTokenStore::default();
        let message = enroll(
            br#"{"verification_token":"verification-secret"}"#.as_slice(),
            &store,
            false,
        )
        .unwrap();

        assert_eq!(message, "Notion webhook verification token enrolled");
        assert_eq!(
            store.writes.into_inner(),
            vec![("verification-secret".to_owned(), false)]
        );
    }

    #[test]
    fn explicit_rotation_is_forwarded_without_echoing_the_token() {
        let store = FakeTokenStore::default();
        let message = enroll(
            br#"{"verification_token":"replacement-secret"}"#.as_slice(),
            &store,
            true,
        )
        .unwrap();

        assert_eq!(message, "Notion webhook verification token rotated");
        assert!(!message.contains("replacement-secret"));
        assert!(store.writes.borrow()[0].1);
    }

    #[test]
    fn malformed_payload_diagnostics_do_not_echo_input() {
        for input in [
            br#"{"verification_token":"actual-secret","extra":true}"#.as_slice(),
            br#"{"verification_token":42,"secret":"actual-secret"}"#.as_slice(),
            br#"{"verification_token":""}"#.as_slice(),
        ] {
            let error = enroll(input, &FakeTokenStore::default(), false).unwrap_err();
            assert!(error.starts_with("invalid webhook verification payload"));
            assert!(!error.contains("actual-secret"));
        }
    }

    #[test]
    fn first_enrollment_creates_a_private_token_file() {
        let root = temporary_directory();
        let state = root.join("state/agent-handover");
        let store = FileTokenStore::new(state.clone());

        enroll(
            br#"{"verification_token":"first-secret"}"#.as_slice(),
            &store,
            false,
        )
        .unwrap();

        let token_file = state.join(TOKEN_FILE);
        assert_eq!(fs::read_to_string(&token_file).unwrap(), "first-secret");
        assert_eq!(fs::metadata(&token_file).unwrap().mode() & 0o777, 0o600);
        assert_eq!(fs::metadata(&state).unwrap().mode() & 0o777, 0o700);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn repeat_enrollment_refuses_to_overwrite_the_token() {
        let root = temporary_directory();
        let state = root.join("state/agent-handover");
        let store = FileTokenStore::new(state.clone());
        enroll(
            br#"{"verification_token":"first"}"#.as_slice(),
            &store,
            false,
        )
        .unwrap();

        let error = enroll(
            br#"{"verification_token":"second-secret"}"#.as_slice(),
            &store,
            false,
        )
        .unwrap_err();

        assert!(error.contains("already enrolled"));
        assert!(!error.contains("second-secret"));
        assert_eq!(fs::read_to_string(state.join(TOKEN_FILE)).unwrap(), "first");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn explicit_rotation_atomically_replaces_the_token() {
        let root = temporary_directory();
        let state = root.join("state/agent-handover");
        let store = FileTokenStore::new(state.clone());
        enroll(
            br#"{"verification_token":"first"}"#.as_slice(),
            &store,
            false,
        )
        .unwrap();

        enroll(
            br#"{"verification_token":"second"}"#.as_slice(),
            &store,
            true,
        )
        .unwrap();

        let token_file = state.join(TOKEN_FILE);
        assert_eq!(fs::read_to_string(&token_file).unwrap(), "second");
        assert_eq!(fs::metadata(token_file).unwrap().mode() & 0o777, 0o600);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn loads_an_enrolled_private_token_without_exposing_it() {
        let root = temporary_directory();
        let state = root.join("state/agent-handover");
        let store = FileTokenStore::new(state);
        store.persist("enrolled-secret", false).unwrap();

        assert_eq!(store.load().unwrap(), "enrolled-secret");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn loading_refuses_unsafe_or_missing_token_files_with_secret_safe_errors() {
        let root = temporary_directory();
        let state = root.join("state/agent-handover");
        let store = FileTokenStore::new(state.clone());
        assert!(store.load().unwrap_err().contains("webhook-enroll"));

        let token_file = state.join(TOKEN_FILE);
        fs::write(&token_file, "actual-secret").unwrap();
        fs::set_permissions(&token_file, fs::Permissions::from_mode(0o644)).unwrap();
        let error = store.load().unwrap_err();
        assert!(error.contains("mode 0600"));
        assert!(!error.contains("actual-secret"));

        fs::remove_file(&token_file).unwrap();
        let target = root.join("target");
        fs::write(&target, "target-secret").unwrap();
        symlink(&target, &token_file).unwrap();
        let error = store.load().unwrap_err();
        assert!(error.contains("cannot safely open"));
        assert!(!error.contains("target-secret"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn refuses_symbolic_link_token_files_without_changing_the_target() {
        let root = temporary_directory();
        let state = root.join("state/agent-handover");
        crate::config::ensure_private_directory(&state).unwrap();
        let target = root.join("outside");
        fs::write(&target, "unchanged").unwrap();
        symlink(&target, state.join(TOKEN_FILE)).unwrap();
        let store = FileTokenStore::new(state);

        let error = enroll(
            br#"{"verification_token":"actual-secret"}"#.as_slice(),
            &store,
            true,
        )
        .unwrap_err();

        assert!(error.contains("symbolic links are not allowed"));
        assert!(!error.contains("actual-secret"));
        assert_eq!(fs::read_to_string(target).unwrap(), "unchanged");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rotation_refuses_an_existing_file_with_unsafe_permissions() {
        let root = temporary_directory();
        let state = root.join("state/agent-handover");
        crate::config::ensure_private_directory(&state).unwrap();
        let token_file = state.join(TOKEN_FILE);
        fs::write(&token_file, "existing-secret").unwrap();
        fs::set_permissions(&token_file, fs::Permissions::from_mode(0o644)).unwrap();
        let store = FileTokenStore::new(state);

        let error = enroll(
            br#"{"verification_token":"replacement-secret"}"#.as_slice(),
            &store,
            true,
        )
        .unwrap_err();

        assert!(error.contains("must have mode 0600"));
        assert!(!error.contains("existing-secret"));
        assert!(!error.contains("replacement-secret"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cleanup_failure_after_first_enrollment_does_not_report_a_false_failure() {
        let operations = RecordingOperations {
            unlink_fails: true,
            ..RecordingOperations::default()
        };

        let result = commit_temporary_file(
            &operations,
            Path::new("/state/temp"),
            Path::new("/state/token"),
            false,
        );

        assert_eq!(result, Ok(()));
        assert_eq!(
            operations.calls.into_inner(),
            vec!["hard_link", "remove_file", "sync_directory"]
        );
    }

    #[test]
    fn enrollment_sync_failure_reports_that_the_token_was_installed() {
        let operations = RecordingOperations {
            sync_fails: true,
            ..RecordingOperations::default()
        };

        let error = commit_temporary_file(
            &operations,
            Path::new("/state/temp"),
            Path::new("/state/token"),
            false,
        )
        .unwrap_err();

        assert!(error.contains("token was installed"));
        assert!(error.contains("durability confirmation failed"));
        assert_eq!(
            operations.calls.into_inner(),
            vec!["hard_link", "remove_file", "sync_directory"]
        );
    }

    #[test]
    fn rotation_sync_failure_reports_that_the_token_was_replaced() {
        let operations = RecordingOperations {
            sync_fails: true,
            ..RecordingOperations::default()
        };

        let error = commit_temporary_file(
            &operations,
            Path::new("/state/temp"),
            Path::new("/state/token"),
            true,
        )
        .unwrap_err();

        assert!(error.contains("token was replaced"));
        assert!(error.contains("durability confirmation failed"));
        assert_eq!(
            operations.calls.into_inner(),
            vec!["rename", "sync_directory"]
        );
    }

    #[test]
    fn first_enrollment_does_not_clobber_a_destination_created_after_inspection() {
        let root = temporary_directory();
        let state = root.join("state/agent-handover");
        let target = root.join("race-target");
        fs::write(&target, "race-winner").unwrap();
        let operations = RacingOperations {
            race: Race::CreateDestinationSymlink,
            target: target.clone(),
        };
        let store = FileTokenStore::new(state.clone());

        let error = store
            .persist_with_operations("new-secret", false, &operations)
            .unwrap_err();

        assert!(error.contains("already enrolled"));
        assert!(!error.contains("new-secret"));
        assert!(
            fs::symlink_metadata(state.join(TOKEN_FILE))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read_to_string(target).unwrap(), "race-winner");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rotation_replaces_a_symlink_raced_into_the_destination_not_its_target() {
        let root = temporary_directory();
        let state = root.join("state/agent-handover");
        crate::config::ensure_private_directory(&state).unwrap();
        let token_file = state.join(TOKEN_FILE);
        fs::write(&token_file, "existing").unwrap();
        fs::set_permissions(&token_file, fs::Permissions::from_mode(0o600)).unwrap();
        let target = root.join("race-target");
        fs::write(&target, "untouched").unwrap();
        let operations = RacingOperations {
            race: Race::ReplaceDestinationWithSymlink,
            target: target.clone(),
        };
        let store = FileTokenStore::new(state);

        store
            .persist_with_operations("replacement-secret", true, &operations)
            .unwrap();

        assert_eq!(
            fs::read_to_string(&token_file).unwrap(),
            "replacement-secret"
        );
        assert!(
            !fs::symlink_metadata(token_file)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read_to_string(target).unwrap(), "untouched");
        fs::remove_dir_all(root).unwrap();
    }
}
