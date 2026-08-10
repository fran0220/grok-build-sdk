use std::{
    fs,
    path::{Path, PathBuf},
};

const LIB_RS_LINE_LIMIT: usize = 300;
const RUST_FILE_LINE_LIMIT: usize = 2_000;

#[test]
fn rust_source_files_stay_within_ownership_limits() {
    let package_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();

    let build_script = package_root.join("build.rs");
    if build_script.is_file() {
        files.push(build_script);
    }
    for source_root in ["src", "tests", "examples", "build"] {
        collect_rust_files(&package_root.join(source_root), &mut files);
    }
    files.sort();

    let mut violations = Vec::new();
    for path in files {
        let relative = path
            .strip_prefix(&package_root)
            .expect("collected source is inside the package root");
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", relative.display()));
        let line_count = source.lines().count();
        let limit = if relative == Path::new("src/lib.rs") {
            LIB_RS_LINE_LIMIT
        } else {
            RUST_FILE_LINE_LIMIT
        };
        if line_count > limit {
            violations.push((relative.to_path_buf(), line_count, limit));
        }
    }

    if !violations.is_empty() {
        let mut message = String::from("Rust source layout violations:\n");
        for (path, line_count, limit) in violations {
            message.push_str(&format!(
                "- {}: {line_count} lines (limit {limit})\n",
                path.display()
            ));
        }
        message.push_str(
            "Split each violating file by responsibility rather than raising the line limit.",
        );
        panic!("{message}");
    }
}

fn collect_rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
    if !directory.is_dir() {
        return;
    }

    let mut entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()));
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .unwrap_or_else(|error| panic!("failed to inspect {}: {error}", path.display()));
        if file_type.is_dir() {
            collect_rust_files(&path, files);
        } else if file_type.is_file() && path.extension().is_some_and(|extension| extension == "rs")
        {
            files.push(path);
        }
    }
}
