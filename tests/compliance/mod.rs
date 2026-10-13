//! The compliance suite's executor, re-exported from the library: the
//! one copy lives at `src/conformance/corpus.rs` behind the `serde`
//! feature, and this module keeps the compliance runner, which includes
//! it by path, compiling unchanged.
//!
//! # The committed compliance artefacts are generated
//!
//! The compliance corpus is generated from the reference behaviour, and
//! one gate says so by rebuilding it in memory and comparing:
//!
//! - the corpus, `tests/compliance/corpus/*.json`, gated by
//!   `the_regenerated_corpus_equals_the_committed_corpus` in
//!   `tests/compliance.rs`.
//!
//! Regenerate and commit:
//!
//! ```text
//! make corpus-export   # cargo test --all-features --test compliance -- --ignored export_corpus
//! ```
//!
//! CI carries the same discipline as a named step of its own on every
//! pull request, `the generated compliance artefacts are current`, which
//! runs the exporter and fails the pull request when the committed tree
//! differs from the regenerated one. The pre-push hook's all-features
//! lane runs the sync gate, so a push with a stale corpus is a push the
//! hook refuses.
//!
//! The exporter overwrites without guard. A hand edit inside
//! `tests/compliance/corpus/` is destroyed by the next export with
//! nothing said about it: the bytes are a function of the reference
//! behaviour, so the reference is what an edit belongs in.

#[allow(unused_imports)]
pub use uvrr::conformance::corpus::*;
