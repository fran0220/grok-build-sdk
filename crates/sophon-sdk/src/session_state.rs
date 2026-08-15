// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! Current-only, content-addressed Session log storage.
//!
//! There is deliberately no GC operation in this release. Implementations may eventually
//! collect objects unreachable from every live manifest, but only under an operator-defined
//! backup, replication, and retention policy.

mod codec;
mod conformance;
mod contracts;
mod local;

pub use conformance::*;
pub use contracts::*;
pub use local::LocalSessionStateStore;

#[cfg(test)]
mod tests;
