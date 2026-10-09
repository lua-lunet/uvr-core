//! The boot fence: the single small file that says how the last life
//! ended.
//!
//! The contract is exactly one file, one status word, one write per
//! transition, forced, renamed, and a boot that trusts what it reads
//! back. No quorum of copies, no hash chain, no multiwrite superblock,
//! no snapshotting. The demo's implementation is the trivial plain file;
//! an industrial host mounts its superblock behind the same trait and
//! changes nothing above it.
//!
//! The three words, in the order a life writes them:
//!
//! - [`FenceWord::Stopping`], the anchor written at every boot: this
//!   life's writes are unproven. A crash never advances the word, so a
//!   crash reads the anchor and nothing more.
//! - [`FenceWord::Stopped`], the halt has begun. It vouches for nothing.
//! - [`FenceWord::Flushed`], the state flush completed and was forced
//!   over it. This is the clean word, the drain's proof: only it makes
//!   the state file loadable, and only it can make a boot clean.
//!
//! The demo performs no graceful stop, so its fences read the anchor
//! forever: every boot after the first classifies the previous life as
//! crashed, which is the truthful verdict for this scaffold, whose stop
//! path exists for the seam's sake and for the industrial host's.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::demo::disk::{Disk, std_disk, write_atomically};

/// The fence's status word, the one value the single file carries.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FenceWord {
    /// The boot anchor: this life's writes are unproven.
    Stopping,
    /// The halt has begun; the flush is unproven under it.
    Stopped,
    /// The flush was forced over the halt; the clean word.
    Flushed,
}

impl FenceWord {
    /// The word as the file carries it: one word, one line.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            FenceWord::Stopping => "stopping",
            FenceWord::Stopped => "stopped",
            FenceWord::Flushed => "flushed",
        }
    }

    /// The word a line reads as, or none for anything the fence did not
    /// write.
    #[must_use]
    pub fn of_str(word: &str) -> Option<Self> {
        match word {
            "stopping" => Some(FenceWord::Stopping),
            "stopped" => Some(FenceWord::Stopped),
            "flushed" => Some(FenceWord::Flushed),
            _ => None,
        }
    }
}

/// The boot fence's contract: write the status word, one forced write;
/// read it back at boot; trust the disk.
///
/// `write` takes `&mut self` because a write is a transition of the
/// fence's own state; `read` takes `&self` because reading is the boot's
/// verdict and must never mutate the fence.
pub trait BootFence: Send + Sync {
    /// One forced write of `word`: staged temp file, fsync, rename,
    /// parent fsync. The fence is one small file and the write is one
    /// transition; there is nothing to batch and nothing to amortise.
    ///
    /// # Errors
    ///
    /// The disk refused. A stop that cannot advance its fence word is a
    /// failed stop, and a boot that cannot write its anchor refuses to
    /// start; the caller surfaces the error in both cases.
    fn write(&mut self, word: FenceWord) -> io::Result<()>;

    /// The word at boot, or `None` when no fence was ever written (the
    /// first life). A file the fence did not write reads as
    /// [`FenceWord::Stopping`], the unproven word: the fence classifies
    /// the past conservatively and never invents the clean word.
    ///
    /// # Errors
    ///
    /// The read itself failed; only a disk error is an `Err`. Content
    /// problems are classifications, never errors.
    fn read(&self) -> io::Result<Option<FenceWord>>;
}

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
