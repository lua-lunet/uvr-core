//! The disk seam: every byte the demo reads or writes, named as one
//! contract.
//!
//! Nothing in the demo names `std::fs` directly. Every store, every fence,
//! every durable write names the operation it wants and hands it a
//! [`Disk`]. The demo's own implementation is [`StdDisk`], a one-to-one
//! delegation to the local filesystem; an industrial host mounts its
//! storage engine behind the same trait and changes nothing above it. The
//! crate compiles identically against either, and nothing above the seam
//! knows which one it is holding.
//!
//! The seam is dynamic, never generic: every holder is an `Arc<dyn Disk>`.
//! Durability is two distinct operations and never one:
//! [`DiskFile::sync_all`] is an fsync, the only one that survives power
//! loss, and the fence and the state store both depend on it.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;

/// One open file, as the seam hands it back.
///
/// The demo's open pattern is one shape: create new, write the whole
/// buffer, fsync, rename. The handle carries exactly the operations that
/// pattern names, so an embedder's engine can answer them the same way it
/// answers [`Disk::create_new_write`].
pub struct DiskFile {
    file: File,
}

impl DiskFile {
    /// Append every byte, or fail.
    pub fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all(bytes)
    }

    /// fsync: the bytes are on the medium. The one operation whose loss
    /// window the fence and the state store are stated against.
    pub fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }
}

/// Every byte of disk access the demo performs.
///
/// The contract is the demo's own usage and nothing wider: each method is
/// one operation a live call site actually performs. An implementation is
/// free to serve any of them from an engine rather than a filesystem, and
/// must report failure the way the underlying operation does.
pub trait Disk: Send + Sync {
    /// Create `path` and every missing ancestor. Succeeds when the
    /// directory already exists.
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;

    /// Rename (or move) `from` onto `to`, replacing `to` if it exists.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Unlink `path`.
    fn remove_file(&self, path: &Path) -> io::Result<()>;

    /// Read a whole file into memory.
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;

    /// Whether `path` exists. Never fails: a stat that cannot answer is
    /// reported as absent, exactly as the `Path::exists` it replaces does.
    fn exists(&self, path: &Path) -> bool;

    /// fsync `path` itself, opening it for that purpose. The POSIX
    /// crash-consistency idiom: after a rename, fsync the containing
    /// directory so the new directory entry is persisted, not just the
    /// file's data.
    fn sync_path(&self, path: &Path) -> io::Result<()>;

    /// Create a new file for writing and fail if it already exists. Every
    /// atomic durable write in the demo stages through this shape: temp
    /// file, fsync, rename.
    fn create_new_write(&self, path: &Path) -> io::Result<DiskFile>;
}

/// The demo's own implementation: the local filesystem, one method per
/// operation, no policy and no caching.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StdDisk;

impl StdDisk {
    fn wrap(file: File) -> DiskFile {
        DiskFile { file }
    }
}

impl Disk for StdDisk {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        fs::read(path)
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn sync_path(&self, path: &Path) -> io::Result<()> {
        Self::wrap(File::open(path)?).sync_all()
    }

    fn create_new_write(&self, path: &Path) -> io::Result<DiskFile> {
        Ok(Self::wrap(
            OpenOptions::new().write(true).create_new(true).open(path)?,
        ))
    }
}

/// The default disk: the demo's own, behind the seam's own indirection.
///
/// Every constructor that does not take a disk of its own builds one of
/// these, so the default path is the same dynamic dispatch as a mounted
/// engine, never a direct `std::fs` call.
pub fn std_disk() -> Arc<dyn Disk> {
    Arc::new(StdDisk)
}

/// The staged durable write shared by the fence and the state store: temp
/// file, fsync, rename, parent fsync, over the disk seam. A failure
/// removes the temp file and reports; a torn write never replaces a good
/// file.
pub(crate) fn write_atomically(disk: &dyn Disk, path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let base = path.file_name().unwrap_or_default();
    let mut temporary = parent.to_path_buf();
    temporary.push(format!(
        ".{}-tmp-{}-{}",
        base.to_string_lossy(),
        std::process::id(),
        TEMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
    ));
    let result = (|| {
        let mut file = disk.create_new_write(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        disk.rename(&temporary, path)?;
        disk.sync_path(parent)
    })();
    if result.is_err() {
        let _ = disk.remove_file(&temporary);
    }
    result
}

/// The staging name's counter: the process id plus one draw per write is
/// already unique within a process, and the counter keeps two draws in
/// the same nanosecond apart.
static TEMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
