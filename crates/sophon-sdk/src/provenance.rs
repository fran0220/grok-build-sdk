// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.
use crate::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceProvenance {
    pub upstream_release: &'static str,
    pub fork_commit: &'static str,
    pub upstream_source_rev: &'static str,
    pub facade_version: &'static str,
    pub dirty: bool,
}
pub fn source_provenance() -> SourceProvenance {
    SourceProvenance {
        upstream_release: "1.0.1",
        fork_commit: env!("SOPHON_SDK_COMMIT"),
        upstream_source_rev: include_str!("../../../SOURCE_REV").trim(),
        facade_version: env!("CARGO_PKG_VERSION"),
        dirty: match env!("SOPHON_SDK_DIRTY") {
            "true" => true,
            "false" => false,
            _ => panic!("build script emitted an invalid dirty marker"),
        },
    }
}

pub fn prompt_digest(text: &str) -> String {
    xai_grok_shell::origin_runtime::prompt_digest(text)
}
pub fn prompt_digest_content(prompt: &Prompt) -> Result<String, Error> {
    use sha2::Digest as _;
    let mut value = serde_json::to_value(prompt).map_err(|e| Error::Operation(e.to_string()))?;
    canonicalize_json(&mut value);
    let canonical = serde_json::to_vec(&value).map_err(|e| Error::Operation(e.to_string()))?;
    let mut digest = sha2::Sha256::new();
    // Compatibility identifier: persisted ledgers created before the public
    // crate rename must keep the same digest and rewind identity.
    digest.update(b"origin-grok-runtime.prompt.v2\0");
    digest.update(canonical);
    Ok(format!("sha256-v2:{:x}", digest.finalize()))
}

pub(crate) fn canonicalize_json(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                canonicalize_json(value);
            }
        }
        serde_json::Value::Object(object) => {
            for value in object.values_mut() {
                canonicalize_json(value);
            }
            let mut entries = std::mem::take(object).into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            object.extend(entries);
        }
        _ => {}
    }
}
