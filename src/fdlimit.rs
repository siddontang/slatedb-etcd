//! The open file limit (`RLIMIT_NOFILE`).
//!
//! The server holds descriptors for client and peer sockets, Raft log files, SlateDB files during
//! flush and compaction, and cached SST reads. Many Linux hosts start processes with a soft limit
//! of 1024, which a loaded server exceeds. The Go runtime raises the soft limit to the hard limit
//! at startup, so etcd does not see this limit. [`raise`] does the same for this server.

/// Soft limits below this value can still run out under load.
pub const LOW: u64 = 65536;

/// The open file limit before and after [`raise`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limit {
    /// The soft limit before the change.
    pub before: u64,
    /// The soft limit now in effect.
    pub soft: u64,
    /// The hard limit. `u64::MAX` means no limit.
    pub hard: u64,
}

/// Raise the soft open file limit to the hard limit.
///
/// macOS rejects a soft limit above `OPEN_MAX`, so the soft limit stops there, as in Go. If the
/// kernel rejects the change, the old limit stays in effect and the result shows it.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn raise() -> std::io::Result<Limit> {
    let mut limit = get()?;
    let before = limit.rlim_cur;
    // `OPEN_MAX` in <sys/syslimits.h>.
    #[cfg(target_os = "macos")]
    let target = limit.rlim_max.min(10240);
    #[cfg(not(target_os = "macos"))]
    let target = limit.rlim_max;
    if target > limit.rlim_cur {
        let wanted = libc::rlimit {
            rlim_cur: target,
            rlim_max: limit.rlim_max,
        };
        // SAFETY: `wanted` is a valid rlimit that lives for the call.
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &wanted) } == 0 {
            limit.rlim_cur = target;
        }
    }
    Ok(Limit {
        before: to_u64(before),
        soft: to_u64(limit.rlim_cur),
        hard: to_u64(limit.rlim_max),
    })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn raise() -> std::io::Result<Limit> {
    Err(std::io::ErrorKind::Unsupported.into())
}

/// The soft open file limit now in effect, or `None` when it is unknown.
pub fn soft() -> Option<u64> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        get().ok().map(|limit| to_u64(limit.rlim_cur))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

/// True when `error` means that the process or the system has no free file descriptor.
pub fn exhausted(error: &std::io::Error) -> bool {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        matches!(
            error.raw_os_error(),
            Some(libc::EMFILE) | Some(libc::ENFILE)
        )
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = error;
        false
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn get() -> std::io::Result<libc::rlimit> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid out pointer for the call.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(limit)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn to_u64(value: libc::rlim_t) -> u64 {
    if value == libc::RLIM_INFINITY {
        u64::MAX
    } else {
        value
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;

    #[test]
    fn raise_reaches_the_hard_limit() {
        let limit = raise().unwrap();
        assert!(limit.soft >= limit.before);
        assert_eq!(soft(), Some(limit.soft));
        #[cfg(target_os = "linux")]
        assert_eq!(limit.soft, limit.hard);
        // A second call changes nothing.
        let again = raise().unwrap();
        assert_eq!((again.before, again.soft), (limit.soft, limit.soft));
    }

    #[test]
    fn exhausted_matches_emfile_and_enfile() {
        assert!(exhausted(&std::io::Error::from_raw_os_error(libc::EMFILE)));
        assert!(exhausted(&std::io::Error::from_raw_os_error(libc::ENFILE)));
        assert!(!exhausted(&std::io::Error::from_raw_os_error(libc::ENOENT)));
    }
}
