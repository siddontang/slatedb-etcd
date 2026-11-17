//! Process crash hooks for dedicated fault-test builds.

#[cfg(not(feature = "fault-injection"))]
#[inline]
pub(crate) fn hit(_name: &str) {}

#[cfg(feature = "fault-injection")]
pub(crate) fn hit(name: &str) {
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::OnceLock;

    static FILE: OnceLock<Option<PathBuf>> = OnceLock::new();
    let Some(path) =
        FILE.get_or_init(|| std::env::var_os("SLATEDB_FAILPOINT_FILE").map(PathBuf::from))
    else {
        return;
    };
    let Ok(selected) = std::fs::read_to_string(path) else {
        return;
    };
    if selected.trim() != name {
        return;
    }
    let reached = path.with_extension("reached");
    let mut file = std::fs::File::create(reached).expect("write failpoint witness");
    file.write_all(name.as_bytes())
        .expect("write failpoint name");
    file.sync_all().expect("sync failpoint witness");
    // Exit without destructors. The witness lets tests reject an unrelated process failure.
    std::process::exit(86);
}
