//! The crate's own host tooling
//! (`docs/uvrr-host-compliance.md`).
//!
//! The sans-I/O core proper reads no clock and opens no socket, and the
//! modules here are stated so the boundary is explicit. `harness` is the
//! reference host the Definition of Done runs on; `corpus` is the case
//! record and executor behind the `serde` feature, the same opt-in the
//! reference runner takes.
//!
//! The zero-dependency property a consumer buys is untouched: every
//! Cargo dependency stays optional and no feature name enters the
//! default set.

#[cfg(feature = "serde")]
pub mod corpus;
pub mod harness;

#[cfg(feature = "serde")]
pub use corpus::{
    Case, Executor, Expect, Op, SystemOp, assert_expectation, identity, pair_of, wire_hex,
};
pub use harness::{Harness, StepOutcome, mint_id, mint_pair};
