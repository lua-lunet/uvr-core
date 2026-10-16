//! The demo's state store: the lock table's persistence, the least
//! environment that satisfies the contract.
//!
//! The contract ([`StateStore`]) lives in the core locks module
//! (`crate::locks`); this file is the demo's own implementation:
//! [`FileStateStore`], one file, written through the demo's [`Disk`] seam
//! (the demo has no `std::fs` call of its own), staged
//! temp-file-then-rename so a torn write can never replace a good file,
//! and framed as
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

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::demo::disk::{Disk, std_disk, write_atomically};
use crate::locks::{StateSnapshot, StateStore};

/// The on-disk format's version byte. Bump it when the payload's schema
/// changes; a reader that does not recognise the byte refuses the file.
/// There is no conversion and no fallback: this crate is alpha and
/// unreleased, and old files are deleted.
///
/// Byte two carries the lock table's generation watermarks beside its
/// records. Byte one did not, and a table that read a byte-one file as
/// carrying no watermark would hand out a generation twice.
const FORMAT_VERSION: u8 = 2;

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
