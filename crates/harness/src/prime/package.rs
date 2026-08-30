use std::io::Read;
use std::path::{Path, PathBuf};

use serde_json::Value;

use super::PrimeDaemonError;

const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;

#[derive(Clone)]
pub(super) struct PrimePackage {
    /// Canonical package bin proven against `package.json`.
    pub executable: PathBuf,
    pub public_entry: PathBuf,
    pub version: String,
    /// Canonical Node executable, preferring the configured bin's sibling.
    pub node: PathBuf,
}

pub(super) fn resolve(configured: &Path) -> Result<PrimePackage, PrimeDaemonError> {
    if !configured.as_os_str().is_empty() {
        return resolve_candidate(resolve_explicit(configured)?);
    }
    if let Some(override_path) =
        std::env::var_os("PRIME_AGENT_EXECUTABLE").filter(|value| !value.is_empty())
    {
        return resolve_candidate(resolve_explicit(Path::new(&override_path))?);
    }
    resolve_discovered(discovery_candidates("prime-agent"))
}

fn resolve_discovered(candidates: Vec<PathBuf>) -> Result<PrimePackage, PrimeDaemonError> {
    let mut saw_candidate = false;
    for candidate in candidates {
        if !candidate.is_file() {
            continue;
        }
        saw_candidate = true;
        if let Ok(package) = resolve_candidate(candidate) {
            return Ok(package);
        }
    }
    if saw_candidate {
        Err(PrimeDaemonError::IncompatiblePackage {
            reason: "no discovered executable is a declared prime-agent package bin",
        })
    } else {
        Err(PrimeDaemonError::NotInstalled {
            reason: "prime-agent was not found",
        })
    }
}

fn resolve_explicit(configured: &Path) -> Result<PathBuf, PrimeDaemonError> {
    if configured.is_absolute() || configured.components().count() > 1 {
        return configured
            .is_file()
            .then(|| configured.to_path_buf())
            .ok_or(PrimeDaemonError::NotInstalled {
                reason: "the configured executable was not found",
            });
    }
    let Some(name) = configured.to_str() else {
        return Err(PrimeDaemonError::NotInstalled {
            reason: "the configured executable name is invalid",
        });
    };
    discovery_candidates(name)
        .into_iter()
        .find(|candidate| candidate.is_file())
        .ok_or(PrimeDaemonError::NotInstalled {
            reason: "the configured executable was not found",
        })
}

fn resolve_candidate(executable: PathBuf) -> Result<PrimePackage, PrimeDaemonError> {
    let canonical_cli = std::fs::canonicalize(&executable)
        .map_err(|error| PrimeDaemonError::io("executable resolution", &error))?;
    if !canonical_cli.is_file() {
        return Err(PrimeDaemonError::NotInstalled {
            reason: "the configured executable is not a file",
        });
    }

    let (package_root, manifest) = find_package(&canonical_cli)?;
    let version = manifest
        .get("version")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(PrimeDaemonError::IncompatiblePackage {
            reason: "the package version is missing",
        })?
        .to_owned();

    let declared_bin = declared_bin(&manifest).ok_or(PrimeDaemonError::IncompatiblePackage {
        reason: "the prime-agent bin declaration is missing",
    })?;
    let canonical_declared_bin = std::fs::canonicalize(package_root.join(declared_bin))
        .map_err(|error| PrimeDaemonError::io("package bin validation", &error))?;
    if canonical_declared_bin != canonical_cli {
        return Err(PrimeDaemonError::IncompatiblePackage {
            reason: "the configured executable is not the package bin",
        });
    }

    let export = public_root_export(&manifest).ok_or(PrimeDaemonError::IncompatiblePackage {
        reason: "the public root ESM export is missing",
    })?;
    if !export.starts_with("./") {
        return Err(PrimeDaemonError::IncompatiblePackage {
            reason: "the public root export is unsafe",
        });
    }
    let public_entry = std::fs::canonicalize(package_root.join(export))
        .map_err(|error| PrimeDaemonError::io("public export validation", &error))?;
    if !public_entry.is_file() || !is_inside(&package_root, &public_entry) {
        return Err(PrimeDaemonError::IncompatiblePackage {
            reason: "the public root export is unsafe",
        });
    }

    let node = resolve_node(&executable)?;
    Ok(PrimePackage {
        executable: canonical_cli,
        public_entry,
        version,
        node,
    })
}

fn find_package(canonical_cli: &Path) -> Result<(PathBuf, Value), PrimeDaemonError> {
    let mut directory = canonical_cli.parent();
    while let Some(candidate) = directory {
        let manifest_path = candidate.join("package.json");
        if let Some(manifest) = read_bounded_manifest(&manifest_path)?
            && manifest.get("name").and_then(Value::as_str) == Some("prime-agent")
        {
            let canonical_root = std::fs::canonicalize(candidate)
                .map_err(|error| PrimeDaemonError::io("package resolution", &error))?;
            return Ok((canonical_root, manifest));
        }
        directory = candidate.parent();
    }
    Err(PrimeDaemonError::IncompatiblePackage {
        reason: "no containing prime-agent package was found",
    })
}

fn read_bounded_manifest(path: &Path) -> Result<Option<Value>, PrimeDaemonError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(PrimeDaemonError::io("package manifest validation", &error)),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Ok(None);
    }
    if metadata.len() > MAX_MANIFEST_BYTES {
        return Err(PrimeDaemonError::IncompatiblePackage {
            reason: "a package manifest exceeds the size bound",
        });
    }
    let file = std::fs::File::open(path)
        .map_err(|error| PrimeDaemonError::io("package manifest read", &error))?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_MANIFEST_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| PrimeDaemonError::io("package manifest read", &error))?;
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(PrimeDaemonError::IncompatiblePackage {
            reason: "a package manifest exceeds the size bound",
        });
    }
    Ok(serde_json::from_slice(&bytes).ok())
}

fn declared_bin(manifest: &Value) -> Option<&str> {
    match manifest.get("bin")? {
        Value::String(value) => Some(value),
        Value::Object(values) => values.get("prime-agent")?.as_str(),
        _ => None,
    }
}

fn public_root_export(manifest: &Value) -> Option<&str> {
    let root = match manifest.get("exports")? {
        Value::String(value) => return Some(value),
        Value::Object(values) => values.get(".")?,
        _ => return None,
    };
    match root {
        Value::String(value) => Some(value),
        Value::Object(conditions) => conditions.get("import")?.as_str(),
        _ => None,
    }
}

fn is_inside(root: &Path, candidate: &Path) -> bool {
    candidate.strip_prefix(root).is_ok()
}

fn resolve_node(executable: &Path) -> Result<PathBuf, PrimeDaemonError> {
    let mut directories = executable
        .parent()
        .map(Path::to_path_buf)
        .into_iter()
        .collect::<Vec<_>>();
    directories.extend(crate::node_version_manager_bins());
    if let Some(path) = std::env::var_os("PATH") {
        directories.extend(std::env::split_paths(&path));
    }
    if let Some(path) = crate::shell_env::login_shell_path() {
        directories.extend(std::env::split_paths(&path));
    }
    let mut seen = std::collections::HashSet::new();
    for directory in directories {
        if !seen.insert(directory.clone()) {
            continue;
        }
        let candidate = directory.join(node_executable_name());
        if candidate.is_file() {
            return std::fs::canonicalize(candidate)
                .map_err(|error| PrimeDaemonError::io("Node.js resolution", &error));
        }
    }
    Err(PrimeDaemonError::NotInstalled {
        reason: "Node.js is required by Prime Agent",
    })
}

fn discovery_candidates(name: &str) -> Vec<PathBuf> {
    let mut directories = Vec::new();
    if let Some(path) = std::env::var_os("PATH") {
        directories.extend(std::env::split_paths(&path));
    }
    if let Some(path) = crate::shell_env::login_shell_path() {
        directories.extend(std::env::split_paths(&path));
    }
    directories.extend(crate::node_version_manager_bins());
    let mut seen = std::collections::HashSet::new();
    directories
        .into_iter()
        .filter(|directory| seen.insert(directory.clone()))
        .map(|directory| directory.join(name))
        .collect()
}

#[cfg(windows)]
fn node_executable_name() -> &'static str {
    "node.exe"
}

#[cfg(not(windows))]
fn node_executable_name() -> &'static str {
    "node"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_public_root_export_is_accepted() {
        let string: Value = serde_json::json!({"exports":"./dist/index.js"});
        assert_eq!(public_root_export(&string), Some("./dist/index.js"));
        let conditional: Value = serde_json::json!({
            "exports": {".": {"types":"./dist/index.d.ts", "import":"./dist/index.js"}}
        });
        assert_eq!(public_root_export(&conditional), Some("./dist/index.js"));
        let private_only: Value = serde_json::json!({
            "main":"./dist/index.js",
            "exports":{"./private":"./dist/private.js"}
        });
        assert_eq!(public_root_export(&private_only), None);
    }

    #[test]
    #[cfg(unix)]
    fn discovery_skips_a_wrapper_before_a_valid_package() {
        let temp = tempfile::tempdir().unwrap();
        let wrapper = temp.path().join("wrapper");
        std::fs::write(&wrapper, "not a package bin").unwrap();
        let package = temp.path().join("prime-agent");
        let bin = package.join("dist/cli.js");
        let entry = package.join("dist/index.js");
        std::fs::create_dir_all(bin.parent().unwrap()).unwrap();
        std::fs::write(&bin, "cli").unwrap();
        std::fs::write(&entry, "export const VERSION='test';").unwrap();
        std::fs::write(
            package.join("package.json"),
            r#"{"name":"prime-agent","version":"test","bin":{"prime-agent":"./dist/cli.js"},"exports":{".":{"import":"./dist/index.js"}}}"#,
        )
        .unwrap();
        let node_dir = temp.path().join("node-bin");
        std::fs::create_dir(&node_dir).unwrap();
        std::fs::write(node_dir.join(node_executable_name()), "node").unwrap();
        let package_link = node_dir.join("prime-agent");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&bin, &package_link).unwrap();
        #[cfg(windows)]
        std::fs::copy(&bin, &package_link).unwrap();

        let resolved = resolve_discovered(vec![wrapper, package_link]).unwrap();
        assert_eq!(resolved.executable, std::fs::canonicalize(bin).unwrap());
        assert_eq!(
            resolved.node,
            std::fs::canonicalize(node_dir.join(node_executable_name())).unwrap()
        );
    }
}
