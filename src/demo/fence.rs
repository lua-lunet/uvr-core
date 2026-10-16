//! The demo's boot fence: the single small file, the least environment
//! that satisfies the contract.
//!
//! The contract ([`BootFence`], with its [`FenceWord`]) lives in the core
//! locks module (`crate::locks`); this file is the demo's own
//! implementation: [`FileBootFence`], one plain file over the demo's
//! [`Disk`] seam, one forced write per transition, read back at boot, the
//! disk trusted. No quorum of copies, no hash chain, no multiwrite
//! superblock, no snapshotting; an industrial host mounts its superblock
//! behind the same trait and changes nothing above it.
//!
//! The demo performs no graceful stop, so its fences read the anchor
//! forever: every boot after the first classifies the previous life as
//! crashed, which is the truthful verdict for this scaffold, whose stop
//! path exists for the seam's sake and for the industrial host's.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::demo::disk::{Disk, std_disk, write_atomically};
use crate::locks::{BootFence, FenceWord};

/// The demo's own fence: one plain file, over the disk seam.
///
/// The file is exactly the word, one line. The write is the demo's one
/// atomic durable write: temp file, fsync, rename, parent fsync. The read
/// trusts the disk: a word the fence wrote is the verdict; absent is the
/// first life; anything else is the unproven word.
pub struct FileBootFence {
    disk: Arc<dyn Disk>,
    path: PathBuf,
}

impl FileBootFence {
    /// The fence at `path`, over the demo's own disk.
    pub fn new(path: &Path) -> Self {
        Self::new_on(std_disk(), path)
    }

    /// The fence at `path`, over a caller-supplied disk: the seam's own
    /// injection point, and how an in-memory disk backs a test.
    pub fn new_on(disk: Arc<dyn Disk>, path: &Path) -> Self {
        Self {
            disk,
            path: path.to_path_buf(),
        }
    }

    /// The file this fence reads and writes.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl BootFence for FileBootFence {
    fn write(&mut self, word: FenceWord) -> io::Result<()> {
        let mut line = Vec::with_capacity(word.as_str().len() + 1);
        line.extend_from_slice(word.as_str().as_bytes());
        line.push(b'\n');
        write_atomically(&*self.disk, &self.path, &line)
    }

    fn read(&self) -> io::Result<Option<FenceWord>> {
        if !self.disk.exists(&self.path) {
            return Ok(None);
        }
        let bytes = self.disk.read(&self.path)?;
        let word = core::str::from_utf8(&bytes)
            .ok()
            .and_then(|text| FenceWord::of_str(text.trim()))
            .unwrap_or(FenceWord::Stopping);
        Ok(Some(word))
    }
}
