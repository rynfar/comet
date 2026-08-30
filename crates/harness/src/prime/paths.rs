use std::fs::File;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use super::PrimeDaemonError;

const SOCKET_HASH_LENGTH: usize = 20;
const RUNTIME_CREATE_ATTEMPTS: usize = 4;
// macOS sockaddr_un.sun_path is 104 bytes including the terminator. Keep
// margin for platform details and reject before spawning an opaque failure.
const MAX_UNIX_SOCKET_PATH_BYTES: usize = 103;

#[cfg(unix)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct SocketIdentity {
    device: u64,
    inode: u64,
}

pub(super) struct PrimePaths {
    pub runtime_dir: PathBuf,
    pub socket: PathBuf,
    pub session_dir: PathBuf,
    pub shim: PathBuf,
    pub session_shim: PathBuf,
    _owner_lock: File,
}

impl PrimePaths {
    pub fn prepare(
        state_dir: &Path,
        instance_id: &str,
        socket_root: &Path,
    ) -> Result<Self, PrimeDaemonError> {
        if instance_id.trim().is_empty() {
            return Err(PrimeDaemonError::TransportSecurity {
                reason: "the local instance identity is empty",
            });
        }
        let state_dir = validate_state_root(&absolute(state_dir)?)?;
        let socket_root = validate_socket_root(&absolute(socket_root)?)?;

        let mut hasher = Sha256::new();
        hasher.update(state_dir.as_os_str().as_encoded_bytes());
        hasher.update([0]);
        hasher.update(instance_id.as_bytes());
        let identity = format!("{:x}", hasher.finalize());
        let identity = &identity[..SOCKET_HASH_LENGTH];
        let session_dir = ensure_private_tree(
            &state_dir,
            &["provider-sessions", "prime-agent", identity, "sessions"],
        )?;

        // Hide stable provider hashes inside one owner-only per-user temp root
        // rather than exposing them as dictionaryable names in shared /tmp.
        let user_root = ensure_private_tree(
            &socket_root,
            &[&format!("zeron-prime-user-{}", effective_user_id())],
        )?;
        // The transport path is stable for crash recovery. A kernel-held lock
        // (not a PID marker) serializes Comet owners and is released on crash.
        let transport_dir = ensure_private_tree(&user_root, &[&format!("identity-{identity}")])?;
        let owner_lock = lock_owner(&transport_dir.join("owner.lock"))?;
        let socket = transport_dir.join("daemon.sock");
        if socket.as_os_str().as_encoded_bytes().len() > MAX_UNIX_SOCKET_PATH_BYTES {
            return Err(PrimeDaemonError::TransportSecurity {
                reason: "the private socket path is too long",
            });
        }

        // The SDK bootstrap script is per launch. It is never used as daemon
        // identity and can be removed on cancellation without touching the
        // stable socket or persistent sessions.
        let runtime_dir = create_runtime_dir(&transport_dir)?;
        Ok(Self {
            shim: runtime_dir.join("bootstrap-bridge.mjs"),
            session_shim: runtime_dir.join("session-host.mjs"),
            runtime_dir,
            socket,
            session_dir,
            _owner_lock: owner_lock,
        })
    }

    pub fn cleanup(&self) {
        let _ = std::fs::remove_dir_all(&self.runtime_dir);
    }

    #[cfg(unix)]
    pub fn socket_identity(&self) -> Result<Option<SocketIdentity>, PrimeDaemonError> {
        read_socket_identity(&self.socket)
    }

    /// Remove only the same verified stale Unix socket that a bounded connect
    /// probe found refused. The inode comparison closes replacement races.
    #[cfg(unix)]
    pub fn remove_stale_socket(&self, expected: SocketIdentity) -> Result<(), PrimeDaemonError> {
        match read_socket_identity(&self.socket)? {
            Some(current) if current == expected => std::fs::remove_file(&self.socket)
                .map_err(|error| PrimeDaemonError::io("stale socket removal", &error)),
            None => Ok(()),
            Some(_) => Err(PrimeDaemonError::TransportSecurity {
                reason: "the private socket identity changed during cleanup",
            }),
        }
    }
}

impl Drop for PrimePaths {
    fn drop(&mut self) {
        // Startup futures can be cancelled after allocation but before a
        // PrimeDaemon is returned. The unique runtime directory must still be
        // removed; stable transport identity and persistent sessions remain.
        self.cleanup();
    }
}

fn absolute(path: &Path) -> Result<PathBuf, PrimeDaemonError> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .map_err(|error| PrimeDaemonError::io("path resolution", &error))
}

#[cfg(unix)]
fn read_socket_identity(path: &Path) -> Result<Option<SocketIdentity>, PrimeDaemonError> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            if metadata.uid() != unsafe { libc::geteuid() }
                || metadata.permissions().mode() & 0o077 != 0
            {
                return Err(PrimeDaemonError::TransportSecurity {
                    reason: "the private socket owner or mode is unsafe",
                });
            }
            Ok(Some(SocketIdentity {
                device: metadata.dev(),
                inode: metadata.ino(),
            }))
        }
        Ok(_) => Err(PrimeDaemonError::TransportSecurity {
            reason: "the private socket path is not a Unix socket",
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(PrimeDaemonError::io("socket validation", &error)),
    }
}

fn validate_state_root(path: &Path) -> Result<PathBuf, PrimeDaemonError> {
    reject_symlink_components(path)?;
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| PrimeDaemonError::io("state root validation", &error))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(PrimeDaemonError::TransportSecurity {
            reason: "the state root is not a real directory",
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.uid() != unsafe { libc::geteuid() } {
            return Err(PrimeDaemonError::TransportSecurity {
                reason: "the state root is not owned by the current user",
            });
        }
    }
    std::fs::canonicalize(path)
        .map_err(|error| PrimeDaemonError::io("state root validation", &error))
}

fn validate_socket_root(path: &Path) -> Result<PathBuf, PrimeDaemonError> {
    reject_symlink_components(path)?;
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| PrimeDaemonError::io("socket root validation", &error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(PrimeDaemonError::TransportSecurity {
            reason: "the socket root is not a real directory",
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let euid = unsafe { libc::geteuid() };
        let mode = metadata.permissions().mode();
        let owned = metadata.uid() == euid;
        let protected_shared_root = metadata.uid() == 0 && mode & 0o1000 != 0;
        if !owned && !protected_shared_root {
            return Err(PrimeDaemonError::TransportSecurity {
                reason: "the socket root has an unsafe owner or mode",
            });
        }
    }
    std::fs::canonicalize(path)
        .map_err(|error| PrimeDaemonError::io("socket root validation", &error))
}

fn reject_symlink_components(path: &Path) -> Result<(), PrimeDaemonError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        if current.parent().is_none() {
            continue;
        }
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(PrimeDaemonError::TransportSecurity {
                    reason: "a private root contains a symlink component",
                });
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(PrimeDaemonError::TransportSecurity {
                    reason: "a private root component is missing",
                });
            }
            Err(error) => return Err(PrimeDaemonError::io("private root validation", &error)),
        }
    }
    Ok(())
}

fn ensure_private_tree(root: &Path, components: &[&str]) -> Result<PathBuf, PrimeDaemonError> {
    let mut current = root.to_path_buf();
    for component in components {
        current.push(component);
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(PrimeDaemonError::TransportSecurity {
                    reason: "a private directory component is a symlink",
                });
            }
            Ok(_) => set_owner_only(&current)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut builder = std::fs::DirBuilder::new();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::DirBuilderExt;
                    builder.mode(0o700);
                }
                match builder.create(&current) {
                    Ok(()) => {}
                    // Another identity can race us while creating a shared
                    // parent on first launch. Treat that only as a cue to run
                    // the same no-symlink/type/owner/mode validation below.
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(error) => {
                        return Err(PrimeDaemonError::io("private directory creation", &error));
                    }
                }
                set_owner_only(&current)?;
            }
            Err(error) => {
                return Err(PrimeDaemonError::io("private directory validation", &error));
            }
        }
    }
    Ok(current)
}

#[cfg(unix)]
fn effective_user_id() -> u32 {
    unsafe { libc::geteuid() }
}

#[cfg(not(unix))]
fn effective_user_id() -> u32 {
    0
}

fn create_runtime_dir(root: &Path) -> Result<PathBuf, PrimeDaemonError> {
    for _ in 0..RUNTIME_CREATE_ATTEMPTS {
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        let candidate = root.join(format!("run-{}", &nonce[..12]));
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        match builder.create(&candidate) {
            Ok(()) => {
                set_owner_only(&candidate)?;
                return Ok(candidate);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(PrimeDaemonError::io("runtime directory creation", &error)),
        }
    }
    Err(PrimeDaemonError::TransportSecurity {
        reason: "a unique runtime directory could not be allocated",
    })
}

fn set_owner_only(path: &Path) -> Result<(), PrimeDaemonError> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| PrimeDaemonError::io("private directory validation", &error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(PrimeDaemonError::TransportSecurity {
            reason: "a private path is not a real directory",
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.uid() != unsafe { libc::geteuid() } {
            return Err(PrimeDaemonError::TransportSecurity {
                reason: "a private directory is not owned by the current user",
            });
        }
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|error| PrimeDaemonError::io("private directory permissions", &error))?;
        let mode = std::fs::symlink_metadata(path)
            .map_err(|error| PrimeDaemonError::io("private directory validation", &error))?
            .permissions()
            .mode();
        if mode & 0o077 != 0 {
            return Err(PrimeDaemonError::TransportSecurity {
                reason: "private directory permissions are too broad",
            });
        }
    }
    Ok(())
}

#[cfg(unix)]
fn lock_owner(path: &Path) -> Result<File, PrimeDaemonError> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| PrimeDaemonError::io("owner lock open", &error))?;
    let metadata = file
        .metadata()
        .map_err(|error| PrimeDaemonError::io("owner lock validation", &error))?;
    if !metadata.is_file() || metadata.uid() != unsafe { libc::geteuid() } {
        return Err(PrimeDaemonError::TransportSecurity {
            reason: "the owner lock is not a private regular file",
        });
    }
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .map_err(|error| PrimeDaemonError::io("owner lock permissions", &error))?;
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::WouldBlock {
            return Err(PrimeDaemonError::TransportSecurity {
                reason: "another Comet process owns this Prime daemon identity",
            });
        }
        return Err(PrimeDaemonError::io("owner lock acquisition", &error));
    }
    Ok(file)
}

#[cfg(not(unix))]
fn lock_owner(path: &Path) -> Result<File, PrimeDaemonError> {
    File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
        .map_err(|error| PrimeDaemonError::io("owner lock open", &error))
}

pub(super) fn write_shim(path: &Path, contents: &str) -> Result<(), PrimeDaemonError> {
    std::fs::write(path, contents)
        .map_err(|error| PrimeDaemonError::io("bridge materialization", &error))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|error| PrimeDaemonError::io("bridge permissions", &error))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(unix)]
    fn paths_are_private_stable_and_exclusively_owned() {
        use std::os::unix::fs::PermissionsExt;
        let state = tempfile::tempdir().unwrap();
        let sockets = tempfile::tempdir_in("/tmp").unwrap();
        let state_path = std::fs::canonicalize(state.path()).unwrap();
        let sockets_path = std::fs::canonicalize(sockets.path()).unwrap();
        let first = PrimePaths::prepare(&state_path, "provider-1", &sockets_path).unwrap();
        let first_socket = first.socket.clone();
        let first_session = first.session_dir.clone();
        let first_runtime = first.runtime_dir.clone();
        assert_eq!(
            first.session_shim.parent(),
            Some(first.runtime_dir.as_path())
        );
        assert_eq!(
            first
                .session_shim
                .file_name()
                .and_then(|name| name.to_str()),
            Some("session-host.mjs")
        );
        let error = PrimePaths::prepare(&state_path, "provider-1", &sockets_path)
            .err()
            .expect("path is rejected");
        assert!(matches!(error, PrimeDaemonError::TransportSecurity { .. }));
        drop(first);

        let second = PrimePaths::prepare(&state_path, "provider-1", &sockets_path).unwrap();
        assert_eq!(first_socket, second.socket);
        assert_eq!(first_session, second.session_dir);
        assert_ne!(first_runtime, second.runtime_dir);
        for path in [
            second.socket.parent().unwrap(),
            &second.runtime_dir,
            &second.session_dir,
        ] {
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
        let second_runtime = second.runtime_dir.clone();
        let second_session_shim = second.session_shim.clone();
        second.cleanup();
        assert!(!second_runtime.exists());
        assert!(!second_session_shim.exists());
    }

    #[test]
    #[cfg(unix)]
    fn distinct_identities_can_create_shared_private_parents_concurrently() {
        let state = tempfile::tempdir().unwrap();
        let sockets = tempfile::tempdir_in("/tmp").unwrap();
        let state = std::fs::canonicalize(state.path()).unwrap();
        let sockets = std::fs::canonicalize(sockets.path()).unwrap();
        let worker_count = 24;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(worker_count));
        let workers = (0..worker_count)
            .map(|index| {
                let state = state.clone();
                let sockets = sockets.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let paths = PrimePaths::prepare(
                        &state,
                        &format!("concurrent-provider-{index}"),
                        &sockets,
                    )?;
                    paths.cleanup();
                    Ok::<_, PrimeDaemonError>(())
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker
                .join()
                .expect("path preparation worker did not panic")
                .expect("distinct identity prepared its private tree");
        }
    }

    #[test]
    #[cfg(unix)]
    fn preexisting_session_symlink_is_rejected() {
        use std::os::unix::fs::symlink;
        let state = tempfile::tempdir().unwrap();
        let sockets = tempfile::tempdir_in("/tmp").unwrap();
        let state_path = std::fs::canonicalize(state.path()).unwrap();
        let sockets_path = std::fs::canonicalize(sockets.path()).unwrap();
        let target = tempfile::tempdir().unwrap();
        let first = PrimePaths::prepare(&state_path, "provider-1", &sockets_path).unwrap();
        let session_dir = first.session_dir.clone();
        drop(first);
        std::fs::remove_dir(&session_dir).unwrap();
        symlink(target.path(), &session_dir).unwrap();
        let error = PrimePaths::prepare(&state_path, "provider-1", &sockets_path)
            .err()
            .expect("path is rejected");
        assert!(matches!(error, PrimeDaemonError::TransportSecurity { .. }));
    }

    #[test]
    #[cfg(unix)]
    fn state_root_symlink_is_rejected() {
        use std::os::unix::fs::symlink;
        let parent = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        let sockets = tempfile::tempdir_in("/tmp").unwrap();
        let parent = std::fs::canonicalize(parent.path()).unwrap();
        let target = std::fs::canonicalize(target.path()).unwrap();
        let sockets = std::fs::canonicalize(sockets.path()).unwrap();
        let link = parent.join("state-link");
        symlink(target, &link).unwrap();
        let error = PrimePaths::prepare(&link, "provider-1", &sockets)
            .err()
            .expect("state symlink is rejected");
        assert!(matches!(error, PrimeDaemonError::TransportSecurity { .. }));
    }

    #[test]
    #[cfg(unix)]
    fn intermediate_session_symlink_is_rejected() {
        use std::os::unix::fs::symlink;
        let state = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        let sockets = tempfile::tempdir_in("/tmp").unwrap();
        let state = std::fs::canonicalize(state.path()).unwrap();
        let target = std::fs::canonicalize(target.path()).unwrap();
        let sockets = std::fs::canonicalize(sockets.path()).unwrap();
        symlink(&target, state.join("provider-sessions")).unwrap();
        let error = PrimePaths::prepare(&state, "provider-1", &sockets)
            .err()
            .expect("intermediate symlink is rejected");
        assert!(matches!(error, PrimeDaemonError::TransportSecurity { .. }));
        assert_eq!(std::fs::read_dir(target).unwrap().count(), 0);
    }

    #[test]
    #[cfg(unix)]
    fn socket_root_symlink_is_rejected() {
        use std::os::unix::fs::symlink;
        let state = tempfile::tempdir().unwrap();
        let sockets = tempfile::tempdir_in("/tmp").unwrap();
        let state_path = std::fs::canonicalize(state.path()).unwrap();
        let sockets_path = std::fs::canonicalize(sockets.path()).unwrap();
        let link = sockets_path.join("link");
        symlink(&sockets_path, &link).unwrap();
        let error = PrimePaths::prepare(&state_path, "provider-1", &link)
            .err()
            .expect("path is rejected");
        assert!(matches!(error, PrimeDaemonError::TransportSecurity { .. }));
    }
}
