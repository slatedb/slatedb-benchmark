use slatedb::manifest::SsTableView;
use slatedb::VersionedManifest;
use std::collections::HashSet;

/// Returns the compressed on-disk size of the physical SSTs in the live LSM.
///
/// A shallow clone's manifest contains views for both inherited and locally
/// owned SSTs. Measuring the manifest therefore follows the current database
/// state without charging the database for obsolete SSTs retained by older
/// checkpoints. Physical SST IDs are deduplicated because projected views can
/// reference the same SST more than once.
pub(crate) fn live_database_size_bytes(manifest: &VersionedManifest) -> u64 {
    let mut seen = HashSet::new();
    let mut total = 0_u64;
    let mut add = |view: &SsTableView| {
        if seen.insert(view.sst.id) {
            total = total.saturating_add(view.estimate_size());
        }
    };

    for view in manifest.l0() {
        add(view);
    }
    for run in manifest.compacted() {
        for view in &run.sst_views {
            add(view);
        }
    }
    for segment in manifest.segments() {
        for view in segment.l0() {
            add(view);
        }
        for run in segment.compacted() {
            for view in &run.sst_views {
                add(view);
            }
        }
    }

    total
}
