//! The state seam: the lock table's persistence contract, one trait.
//!
//! The law, and it is the whole of this module:
//!
//! - **FLUSH is EAGER**: on the shutdown path the whole lock-table state
//!   is written in the foreground before the stop completes. A stop that
//!   cannot flush is a FAILED stop: it surfaces the failure, it never
//!   silently skips.
//! - **LOAD is LAZY**: the regular path never loads eagerly at boot. The
//!   table starts empty and state materialises only on demand (the
//!   cold-start fallback).
//! - **A CRASHED boot (the fence's verdict) DISTRUSTS the state file**:
//!   load returns nothing and the node rebuilds from the replica stream.
//!   Only a clean-stop file is loadable.
//!
//! Two words, two different disciplines, one trait ([`StateStore`]). The
//! shutdown path calls [`StateStore::flush`] in the foreground and treats
//! an `Err` as a failed stop; the regular path calls [`StateStore::load`]
//! exactly once, lazily, behind the fence verdict's guard, never at boot.
//!
//! The in-tree implementation is [`FileStateStore`]: one file, written
//! through the [`Disk`] seam (the demo has no `std::fs` call of its own),
//! staged temp-file-then-rename so a torn write can never replace a good
//! file, and framed as
//!
//! ```text
//! version byte | payload length (u32 BE) | payload | checksum (u32 BE)
//! ```
//!
//! A format-version byte so an old reader refuses a new file, a length
//! prefix so a truncated tail is a refusal rather than a half-decoded
//! table, and a trailing CRC over everything before it so a rotted byte
//! anywhere is a refusal. The checksum is this module's own CRC-32 IEEE;
//! no dependency was added for it.
//!
//! [`load`][StateStore::load] is deliberately asymmetric with `flush`: a
//! file that is absent, of the wrong version, short, long,
//! checksum-rotten or undecodable is `Ok(None)`, never an `Err`. State is
//! a fallback, and a fallback that blocks the boot has turned a cold
//! start into an outage.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::demo::disk::{Disk, std_disk, write_atomically};
use crate::demo::locks::StateSnapshot;

/// The on-disk format's version byte. Bump it when the payload's schema
/// changes; a reader that does not recognise the byte refuses the file.
/// There is no conversion and no fallback: this crate is alpha and
/// unreleased, and old files are deleted.
const FORMAT_VERSION: u8 = 1;

/// The lock table's persistence contract: flush the whole table on the
/// shutdown path, load it back on the regular path.
///
/// The law, restated on the trait itself so every implementation inherits
/// it:
///
/// - **FLUSH is EAGER**: on the shutdown path the whole lock-table state
///   is written in the foreground before the stop completes. A stop that
///   cannot flush is a FAILED stop: it surfaces the failure, it never
///   silently skips.
/// - **LOAD is LAZY**: the regular path never loads eagerly at boot. The
///   table starts empty and state materialises only on demand.
/// - **A CRASHED boot DISTRUSTS the state file**: load returns nothing
///   and the node rebuilds from the replica stream. Only a clean-stop
///   file is loadable.
///
/// `flush` takes `&mut self` because a store may hold a staging
/// position; `load` takes `&self` because loading must never mutate the
/// store: the snapshot it hands back is the caller's to apply or drop.
pub trait StateStore: Send + Sync {
    /// Write the whole lock-table state, durably, before returning. The
    /// eager half of the law: the caller is the shutdown path, in the
    /// foreground, between the stop's first fence write and the final
    /// one. An `Err` fails the stop; the caller surfaces it and never
    /// skips it.
    fn flush(&mut self, snap: &StateSnapshot) -> io::Result<()>;

    /// Read the state back, or report that there is none to read.
    ///
    /// `Ok(None)` is every "there is nothing trustworthy here" answer:
    /// no file, a version this build does not know, a length that does
    /// not match the bytes present, a failed checksum, or a payload that
    /// does not decode. Never an `Err` for a content problem: a fallback
    /// that blocks the boot is an outage, and this is a fallback. The
    /// crashed-boot guard lives at the call site, never here: a store
    /// that is handed a load has already been cleared for it.
    fn load(&self) -> io::Result<Option<StateSnapshot>>;
}

/// The in-tree store: one file, written through the disk seam.
///
/// A mounted engine replaces this whole struct; nothing above the trait
/// knows which one the node holds. The file is the node's state path with
/// a `.state` suffix.
pub struct FileStateStore {
    disk: Arc<dyn Disk>,
    path: PathBuf,
}

impl FileStateStore {
    /// The store over the demo's own disk: the local filesystem behind
    /// the seam, one path, no policy beyond the framing below.
    pub fn new(path: &Path) -> Self {
        Self::new_on(std_disk(), path)
    }

    /// The store over a caller-supplied disk, the seam's own injection
    /// point, and the reason the disk is a parameter rather than a
    /// hard-wired `std_disk()` call inside `flush`.
    pub fn new_on(disk: Arc<dyn Disk>, path: &Path) -> Self {
        Self {
            disk,
            path: state_file_path(path),
        }
    }

    /// The file this store reads and writes: the node's state path with
    /// `.state` appended. Public because the file's name is part of the
    /// contract: an embedder's own store is named the same way. The bytes
    /// inside it are this struct's business.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The framed bytes for a snapshot: version, length, payload,
    /// checksum. One function so the writer and the reader cannot drift.
    fn frame(snap: &StateSnapshot) -> io::Result<Vec<u8>> {
        let payload = serde_json::to_vec(snap)?;
        let mut framed = Vec::with_capacity(5 + payload.len() + 4);
        framed.push(FORMAT_VERSION);
        framed.extend_from_slice(
            &u32::try_from(payload.len())
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "state snapshot is too large to frame",
                    )
                })?
                .to_be_bytes(),
        );
        framed.extend_from_slice(&payload);
        let checksum = crc32_ieee(&framed);
        framed.extend_from_slice(&checksum.to_be_bytes());
        Ok(framed)
    }

    /// The snapshot a framed file carries, or `None` when the frame is
    /// not one this build trusts. Every refusal is a content refusal:
    /// wrong version, a length that does not match the bytes present, a
    /// short file, a rotted checksum, or a payload that does not decode.
    fn unframe(bytes: &[u8]) -> Option<StateSnapshot> {
        const HEADER: usize = 1 + 4;
        const CHECKSUM: usize = 4;
        if bytes.len() < HEADER + CHECKSUM || bytes[0] != FORMAT_VERSION {
            return None;
        }
        let length = u32::from_be_bytes(bytes[1..HEADER].try_into().ok()?) as usize;
        let body_end = HEADER.checked_add(length)?;
        // The length prefix is load-bearing: a torn tail is a file whose
        // length disagrees with its own header, and that is a refusal
        // rather than a half-decoded table.
        if bytes.len() != body_end + CHECKSUM {
            return None;
        }
        let stored = u32::from_be_bytes(bytes[body_end..].try_into().ok()?);
        if crc32_ieee(&bytes[..body_end]) != stored {
            return None;
        }
        serde_json::from_slice(&bytes[HEADER..body_end]).ok()
    }
}

impl StateStore for FileStateStore {
    fn flush(&mut self, snap: &StateSnapshot) -> io::Result<()> {
        write_atomically(&*self.disk, &self.path, &Self::frame(snap)?)
    }

    fn load(&self) -> io::Result<Option<StateSnapshot>> {
        // An absent file is the ordinary cold-start answer, not a
        // failure: `exists` is the seam's never-failing stat and every
        // other content problem lands on `unframe`'s `None`.
        if !self.disk.exists(&self.path) {
            return Ok(None);
        }
        Ok(Self::unframe(&self.disk.read(&self.path)?))
    }
}

/// The state file's path for a node state path: the path's own name with
/// `.state` appended.
#[must_use]
fn state_file_path(state: &Path) -> PathBuf {
    let mut os = state.as_os_str().to_os_string();
    os.push(".state");
    PathBuf::from(os)
}

/// THE LAZY LOAD, with its guard, as one function: the seam's half of the
/// law, kept beside the trait so the guard is stated once and can be
/// driven without a node.
///
/// The guard has two clauses and both are refusals:
///
/// - **A CRASHED boot distrusts the file.** `clean_boot` is the fence's
///   verdict: only a clean stop, the one whose final fence write landed
///   over the completed flush, left a file worth reading. A crashed boot
///   reads nothing and rebuilds from the replica stream, even when a
///   perfectly readable file is sitting there.
/// - **A populated table is never overwritten.** `table_empty` says the
///   caller has established the table holds nothing; anything in it came
///   from the regular path or an earlier load, and a load now would
///   overwrite live state.
///
/// The one-shot clause, at most one load per node life, belongs to the
/// caller, because it is per-life state and this function is not.
///
/// Every refusal is `Ok(None)`: the same answer a missing file gives.
/// Nothing here can fail a boot.
pub fn lazy_load(
    store: &dyn StateStore,
    clean_boot: bool,
    table_empty: bool,
) -> io::Result<Option<StateSnapshot>> {
    if !clean_boot {
        return Ok(None);
    }
    if !table_empty {
        return Ok(None);
    }
    store.load()
}

/// The store's own CRC-32 IEEE, bitwise, most significant bit first, the
/// reflected polynomial `0xEDB88320`. No dependency was added for it.
#[must_use]
pub(crate) fn crc32_ieee(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFF_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}
