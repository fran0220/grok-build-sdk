// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

use std::{path::PathBuf, process::Command};

#[test]
fn rust_source_files_stay_within_ownership_limits() {
    let package_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let script = package_root.join("scripts/check-source-layout.sh");
    let output = Command::new(&script)
        .output()
        .unwrap_or_else(|error| panic!("failed to run {}: {error}", script.display()));

    assert!(
        output.status.success(),
        "source layout check failed ({}):\n{}{}",
        script.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
