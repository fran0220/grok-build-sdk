use std::path::{Path, PathBuf};
use std::process::Command;

fn git(root: &Path, arguments: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(arguments)
        .output()
        .ok()?;
    output.status.success().then(|| {
        String::from_utf8(output.stdout)
            .expect("git provenance is UTF-8")
            .trim()
            .to_owned()
    })
}

fn full_commit(value: String) -> String {
    assert!(
        value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "Origin Grok fork commit must be a full Git object id"
    );
    value
}

fn env_bool(name: &str) -> Option<bool> {
    std::env::var(name).ok().map(|value| match value.as_str() {
        "1" | "true" => true,
        "0" | "false" => false,
        _ => panic!("{name} must be one of: 0, 1, false, true"),
    })
}

fn main() {
    let manifest = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let root = manifest
        .ancestors()
        .nth(2)
        .expect("origin-grok-runtime remains in the fork workspace");
    let git_commit = git(root, &["rev-parse", "HEAD"]);
    let commit = full_commit(
        std::env::var("ORIGIN_GROK_BUILD_COMMIT")
            .ok()
            .or(git_commit.clone())
            .expect(
                "source archives must set ORIGIN_GROK_BUILD_COMMIT to the exact Origin fork commit",
            ),
    );
    if let Some(git_commit) = git_commit {
        assert_eq!(
            commit,
            full_commit(git_commit),
            "ORIGIN_GROK_BUILD_COMMIT does not match the checked-out Origin fork"
        );
    }
    let git_dirty = git(root, &["status", "--porcelain", "--untracked-files=all"])
        .map(|status| !status.is_empty());
    let dirty = env_bool("ORIGIN_GROK_BUILD_DIRTY")
        .or(git_dirty)
        .expect("source archives must set ORIGIN_GROK_BUILD_DIRTY to true or false");
    println!("cargo:rustc-env=ORIGIN_GROK_BUILD_COMMIT={commit}");
    println!("cargo:rustc-env=ORIGIN_GROK_BUILD_DIRTY={dirty}");
    println!(
        "cargo:rerun-if-changed={}",
        root.join(".git/HEAD").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        root.join(".git/index").display()
    );
    println!("cargo:rerun-if-env-changed=ORIGIN_GROK_BUILD_COMMIT");
    println!("cargo:rerun-if-env-changed=ORIGIN_GROK_BUILD_DIRTY");
}
