//! Durable, host-independent Run state, reducer, and local providers.
//!
//! The reducer is the sole Run authority. Hosts may execute only effects that
//! were committed to the outbox and claimed with an attempt-scoped token.
//! Session turns deliberately reference the SDK's existing Turn ledger rather
//! than introducing another conversation or settlement ledger here.

mod artifact;
mod controller;
mod model;
mod providers;
mod store;

pub use artifact::{ArtifactMetadata, ArtifactStore, LocalArtifactStore};
pub use controller::RunController;
pub use model::*;
pub use providers::*;
pub use store::{LocalRunStore, RunStore, StoreCommit, StoreCommitResult};

#[cfg(test)]
mod tests;
