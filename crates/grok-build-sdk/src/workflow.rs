// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! A bounded workflow driver that adds no state store.
//!
//! A workflow is a bounded sequence of steps a Host wants to run unattended and
//! resume after a crash. The temptation is to give it a store of its own. This
//! module deliberately does not, because every piece of a workflow's state
//! already has exactly one owner:
//!
//! | State | Owner | Mechanism |
//! |---|---|---|
//! | Which workflow is due, and who may run it | [`crate::ActivationCoordinator`] | `claim_due`, [`ActivationFencingToken`] |
//! | Whether the outcome was recorded | [`crate::ActivationCoordinator`] | `ActivationSettlement::AlreadySettled` |
//! | Step sequence and current position | [`crate::run`] | `IterationId`, `BeginIteration` / `FinishIteration` |
//! | One step's declared intent | [`crate::run`] | `PrepareOperation` + `EffectSpec` |
//! | One step's exclusive right to execute | [`crate::run`] | `ClaimEffect` + [`ActivationFence`] |
//! | One step's outcome | [`crate::run`] | `EffectReceipt`, `acknowledge_effect` |
//! | Unknown-outcome recovery | [`crate::run`] | `ReconcileEffect`, `ReconcileDecision` |
//! | Step inputs and outputs | [`crate::ArtifactVault`] | [`ArtifactRef`], digest-verified |
//! | Resource consumption | [`crate::run`] | `ResourceVector`, `EffectUsage` |
//! | Kernel session state | [`crate::KernelRuntime`] | evidence only, never consulted for truth |
//!
//! There is no row without an owner, which is the whole argument for adding no
//! store. What this module contributes is the two things that genuinely did not
//! exist: the declared ceilings a workflow is held to, and the answer to *may
//! it take another step* — computed from the Run, never from a counter of the
//! driver's own.
//!
//! [`ActivationFencingToken`]: crate::ActivationFencingToken

use crate::run::{
    ActivationFence, ClaimEffect, IterationId, OperationId, OperationState, ResourceDimension,
    ResourceVector, RunEnvelope, RunError, RunId,
};
use xai_agent_lifecycle::run::RunRecord;

/// Largest step ceiling one workflow may declare.
pub const MAX_WORKFLOW_STEPS: u32 = 10_000;
/// Largest wall ceiling one workflow may declare: twenty-four hours.
pub const MAX_WORKFLOW_WALL_MS: u64 = 24 * 60 * 60 * 1000;

fn refuse(message: impl Into<String>) -> RunError {
    RunError::Validation(message.into())
}

/// Declared ceilings one workflow run is held to.
///
/// Validated before the first step is prepared, so a workflow that cannot
/// terminate never starts. Every ceiling is finite and non-zero: an unbounded
/// dimension here would be an unattended loop with nothing to stop it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowCeilings {
    max_steps: u32,
    max_wall_ms: u64,
    max_consecutive_failures: u32,
    budget: ResourceVector,
}

impl WorkflowCeilings {
    pub fn new(
        max_steps: u32,
        max_wall_ms: u64,
        max_consecutive_failures: u32,
        budget: ResourceVector,
    ) -> Result<Self, RunError> {
        let ceilings = Self {
            max_steps,
            max_wall_ms,
            max_consecutive_failures,
            budget,
        };
        ceilings.validate()?;
        Ok(ceilings)
    }

    pub fn max_steps(&self) -> u32 {
        self.max_steps
    }

    pub fn max_wall_ms(&self) -> u64 {
        self.max_wall_ms
    }

    pub fn max_consecutive_failures(&self) -> u32 {
        self.max_consecutive_failures
    }

    pub fn budget(&self) -> &ResourceVector {
        &self.budget
    }

    /// Re-checks declared ceilings. A zero anywhere is refused rather than read
    /// as *no limit*, which is the reading that turns a bounded workflow into
    /// an unattended one.
    pub fn validate(&self) -> Result<(), RunError> {
        if self.max_steps == 0 || self.max_steps > MAX_WORKFLOW_STEPS {
            return Err(refuse(format!(
                "a workflow declares between 1 and {MAX_WORKFLOW_STEPS} steps"
            )));
        }
        if self.max_wall_ms == 0 || self.max_wall_ms > MAX_WORKFLOW_WALL_MS {
            return Err(refuse(format!(
                "a workflow declares between 1 and {MAX_WORKFLOW_WALL_MS} milliseconds of wall time"
            )));
        }
        if self.max_consecutive_failures == 0 {
            return Err(refuse(
                "a workflow that tolerates no failure count cannot stop on failures",
            ));
        }
        if self.budget.is_zero() {
            return Err(refuse("a workflow declares a non-zero resource budget"));
        }
        Ok(())
    }
}

/// What one step does. It is an existing intent plus its share of the ceilings;
/// it is not a new durable object.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkflowAction {
    Program(Box<crate::ProgramIntent>),
    /// A fragment submitted to a live kernel session. The source travels as an
    /// artifact rather than inline for the same reason a program's context
    /// does: it is content, it is already addressed, and a Run envelope is not
    /// where content belongs.
    KernelSubmit {
        session: crate::KernelSessionId,
        submission: crate::run::ArtifactRef,
    },
}

/// One step's declaration.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowStepIntent {
    pub run_id: RunId,
    pub operation_id: OperationId,
    pub iteration_id: IterationId,
    pub action: WorkflowAction,
    pub reservation: ResourceVector,
}

impl WorkflowStepIntent {
    pub fn validate(&self) -> Result<(), RunError> {
        if self.reservation.is_zero() {
            return Err(refuse("a workflow step reserves nothing"));
        }
        match &self.action {
            WorkflowAction::Program(intent) => intent.validate(),
            WorkflowAction::KernelSubmit { submission, .. } => submission.validate(),
        }
    }
}

/// Why a workflow stopped.
///
/// Every ceiling has its own name. There is no variant meaning *ran out of
/// something*, because a Host that has to tell a person why an unattended
/// sequence stopped cannot do it from a word like that.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkflowDisposition {
    Completed,
    StepCeiling,
    WallCeiling,
    BudgetCeiling { dimension: ResourceDimension },
    ConsecutiveFailureCeiling,
    Cancelled,
    Interrupted,
}

/// Whether a workflow may take another step, computed from the Run alone.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkflowAdmission {
    /// The step may be prepared. `index` is the Run's own iteration count, not
    /// a number the driver kept.
    Step { index: u64 },
    /// The workflow stops, for exactly this reason.
    Stop(WorkflowDisposition),
}

/// The bounded driver: declared ceilings plus the fence that makes a superseded
/// driver harmless.
///
/// It holds no counter, no cursor and no store. Constructing one validates the
/// ceilings, which is what "a workflow that cannot terminate never starts"
/// means in practice: there is no way to reach [`Self::admit`] with ceilings
/// that were never checked.
#[derive(Clone, Debug)]
pub struct WorkflowDriver {
    ceilings: WorkflowCeilings,
    fence: ActivationFence,
}

impl WorkflowDriver {
    /// The fence comes from the activation grant that made this driver the one
    /// entitled to run the workflow. It is not optional: a driver with no fence
    /// is a driver that cannot be superseded.
    pub fn new(ceilings: WorkflowCeilings, fence: ActivationFence) -> Result<Self, RunError> {
        ceilings.validate()?;
        Ok(Self { ceilings, fence })
    }

    pub fn ceilings(&self) -> &WorkflowCeilings {
        &self.ceilings
    }

    pub fn fence(&self) -> &ActivationFence {
        &self.fence
    }

    /// The claim for one step, always carrying this driver's fence.
    ///
    /// Every claim goes through here, which is what makes the fence structural
    /// rather than remembered: there is no shape in this module that produces a
    /// claim without one, so a superseded driver's step is refused by the
    /// reducer before it can reach `acknowledge_effect`.
    pub fn claim(&self, step: &WorkflowStepIntent) -> Result<ClaimEffect, RunError> {
        step.validate()?;
        if !step.reservation.within(&self.ceilings.budget) {
            return Err(refuse(
                "a workflow step reserves more than the workflow's whole budget",
            ));
        }
        Ok(ClaimEffect::new(step.operation_id.clone())
            .reservation(step.reservation.clone())
            .activation(self.fence.clone()))
    }

    /// Whether the workflow may take another step, and if not, why it stopped.
    ///
    /// Every input is read from the Run: the step index is the Run's iteration
    /// count, the elapsed wall time is the caller's declared instant minus the
    /// Run's recorded start, the spend is the Run's accumulated usage, and the
    /// failure streak is the tail of the Run's own operations. Nothing here
    /// consults a clock and nothing here remembers.
    pub fn admit(&self, snapshot: &RunEnvelope, now_ms: u64) -> WorkflowAdmission {
        let run = &snapshot.run;
        let taken = run.next_iteration_id.saturating_sub(1);
        if taken >= u64::from(self.ceilings.max_steps) {
            return WorkflowAdmission::Stop(WorkflowDisposition::StepCeiling);
        }
        if now_ms.saturating_sub(run.created_at_ms) >= self.ceilings.max_wall_ms {
            return WorkflowAdmission::Stop(WorkflowDisposition::WallCeiling);
        }
        if let Some(dimension) = exceeded(&run.usage, &self.ceilings.budget) {
            return WorkflowAdmission::Stop(WorkflowDisposition::BudgetCeiling { dimension });
        }
        if consecutive_failures(run) >= u64::from(self.ceilings.max_consecutive_failures) {
            return WorkflowAdmission::Stop(WorkflowDisposition::ConsecutiveFailureCeiling);
        }
        WorkflowAdmission::Step { index: taken }
    }
}

/// The first budget dimension the Run has spent past, in declaration order, so
/// a settlement can name the ceiling rather than describe it.
fn exceeded(usage: &ResourceVector, budget: &ResourceVector) -> Option<ResourceDimension> {
    let dimensions = [
        (
            ResourceDimension::Iterations,
            usage.iterations,
            budget.iterations,
        ),
        (
            ResourceDimension::AgentCalls,
            usage.agent_calls,
            budget.agent_calls,
        ),
        (
            ResourceDimension::AgentConcurrency,
            usage.agent_concurrency,
            budget.agent_concurrency,
        ),
        (
            ResourceDimension::ActiveMs,
            usage.active_ms,
            budget.active_ms,
        ),
        (ResourceDimension::WallMs, usage.wall_ms, budget.wall_ms),
        (ResourceDimension::Tokens, usage.tokens, budget.tokens),
        (
            ResourceDimension::CostMicros,
            usage.cost_micros,
            budget.cost_micros,
        ),
        (
            ResourceDimension::ArtifactBytes,
            usage.artifact_bytes,
            budget.artifact_bytes,
        ),
    ];
    dimensions
        .into_iter()
        .find(|(_, spent, limit)| spent >= limit && *limit > 0)
        .map(|(dimension, _, _)| dimension)
}

/// How many of the Run's most recent iterations settled without success.
///
/// An iteration counts as failed when every operation it prepared ended
/// unsuccessfully; one acknowledged or reconciled operation ends the streak,
/// because the workflow demonstrably made progress in that step.
fn consecutive_failures(run: &RunRecord) -> u64 {
    let mut iterations: Vec<IterationId> = run
        .operations
        .values()
        .map(|operation| operation.iteration_id)
        .collect();
    iterations.sort_unstable();
    iterations.dedup();
    let mut streak = 0;
    for iteration in iterations.into_iter().rev() {
        let mut settled = false;
        let mut progressed = false;
        for operation in run
            .operations
            .values()
            .filter(|operation| operation.iteration_id == iteration)
        {
            match operation.state {
                OperationState::Acknowledged | OperationState::Reconciled => {
                    settled = true;
                    progressed = true;
                }
                OperationState::FailedRetryable
                | OperationState::Uncertain
                | OperationState::Abandoned => settled = true,
                // A step still in flight is not yet evidence of anything.
                OperationState::Prepared | OperationState::Dispatching => {}
                _ => {}
            }
        }
        if !settled {
            continue;
        }
        if progressed {
            break;
        }
        streak += 1;
    }
    streak
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget() -> ResourceVector {
        ResourceVector::default()
            .iterations(8)
            .agent_calls(8)
            .tokens(1_000)
    }

    fn ceilings() -> WorkflowCeilings {
        WorkflowCeilings::new(4, 60_000, 2, budget()).expect("declared ceilings are valid")
    }

    /// A fence is minted by the reducer when an activation is claimed, never by
    /// a caller, so a test that needs one decodes it rather than building it.
    fn fence(token: &str) -> ActivationFence {
        serde_json::from_value(serde_json::json!({
            "worker_id": "worker-1",
            "epoch": 1,
            "token": token,
        }))
        .expect("a fence decodes")
    }

    /// A workflow that cannot terminate never starts: the ceilings are checked
    /// before anything is prepared, and a zero is refused rather than read as
    /// "no limit".
    #[test]
    fn ceilings_that_cannot_terminate_are_refused() {
        assert!(WorkflowCeilings::new(0, 60_000, 1, budget()).is_err());
        assert!(WorkflowCeilings::new(1, 0, 1, budget()).is_err());
        assert!(WorkflowCeilings::new(1, 60_000, 0, budget()).is_err());
        assert!(WorkflowCeilings::new(1, 60_000, 1, ResourceVector::default()).is_err());
        assert!(WorkflowCeilings::new(MAX_WORKFLOW_STEPS + 1, 60_000, 1, budget()).is_err());
        assert!(WorkflowCeilings::new(1, MAX_WORKFLOW_WALL_MS + 1, 1, budget()).is_err());
        assert!(WorkflowDriver::new(ceilings(), fence("token-1")).is_ok());
    }

    /// Every claim this driver mints carries its fence. That is what makes a
    /// superseded driver harmless: the reducer refuses a stale fence, and there
    /// is no shape here that produces a claim without one.
    #[test]
    fn every_claim_carries_the_drivers_fence() {
        let driver = WorkflowDriver::new(ceilings(), fence("token-1")).expect("the driver starts");
        let step = WorkflowStepIntent {
            run_id: RunId::new("run-1").expect("a valid run id"),
            operation_id: OperationId::new("op-1").expect("a valid operation id"),
            iteration_id: IterationId::new(1),
            action: WorkflowAction::KernelSubmit {
                session: crate::KernelSessionId::new("run-1.kernel").expect("a valid session id"),
                submission: crate::run::ArtifactRef::new(
                    "a".repeat(64),
                    "text/plain",
                    16,
                    "kernel_fragment",
                    "run-1",
                ),
            },
            reservation: ResourceVector::default().iterations(1).tokens(10),
        };
        let claim = driver.claim(&step).expect("a claimed step");
        assert_eq!(claim.activation.as_ref(), Some(driver.fence()));

        let superseded = WorkflowDriver::new(ceilings(), fence("token-2"))
            .expect("the superseded driver starts");
        assert_ne!(
            superseded.claim(&step).expect("a claimed step").activation,
            claim.activation,
            "two drivers minted claims a reducer cannot tell apart",
        );
    }

    /// A step may not reserve more than the whole workflow is allowed to spend,
    /// and a step that reserves nothing is not a step.
    #[test]
    fn a_step_reservation_is_bounded_by_the_workflow_budget() {
        let driver = WorkflowDriver::new(ceilings(), fence("token-1")).expect("the driver starts");
        let mut step = WorkflowStepIntent {
            run_id: RunId::new("run-1").expect("a valid run id"),
            operation_id: OperationId::new("op-1").expect("a valid operation id"),
            iteration_id: IterationId::new(1),
            action: WorkflowAction::KernelSubmit {
                session: crate::KernelSessionId::new("run-1.kernel").expect("a valid session id"),
                submission: crate::run::ArtifactRef::new(
                    "a".repeat(64),
                    "text/plain",
                    16,
                    "kernel_fragment",
                    "run-1",
                ),
            },
            reservation: ResourceVector::default().tokens(10_000),
        };
        assert!(driver.claim(&step).is_err());
        step.reservation = ResourceVector::default();
        assert!(driver.claim(&step).is_err());
    }

    /// A ceiling settlement names the dimension that stopped the workflow.
    /// "Ran out of something" is not an answer a Host can give a person.
    #[test]
    fn an_exceeded_budget_names_its_own_dimension() {
        let spent = ResourceVector::default().iterations(2).tokens(1_000);
        assert_eq!(exceeded(&spent, &budget()), Some(ResourceDimension::Tokens));
        assert_eq!(
            exceeded(&ResourceVector::default().iterations(8), &budget()),
            Some(ResourceDimension::Iterations)
        );
        assert_eq!(
            exceeded(&ResourceVector::default().tokens(1), &budget()),
            None
        );
    }
}
