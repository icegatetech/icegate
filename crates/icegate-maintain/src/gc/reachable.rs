//! Build the set of object keys a table currently references.
//!
//! The referenced set is the union of: the current metadata pointer, every
//! retained metadata-log entry, any table/partition statistics files, and — for
//! every snapshot in the table metadata — the manifest-list path, every manifest
//! path it names, and every data/delete file path inside those manifests
//! (regardless of entry status). Including tombstone entries is deliberately
//! conservative: keeping a file one cycle too long is safe; deleting a
//! referenced one is not.

use std::collections::HashSet;

use iceberg::table::Table;
use object_store::path::Path as ObjectPath;

use crate::error::MaintainError;
use crate::gc::decide::parse_object_key;

/// Collect the bucket-relative object keys of every file the loaded `table`
/// references.
///
/// # Errors
///
/// Returns [`MaintainError::Iceberg`] if any manifest list or manifest cannot be
/// loaded, or [`MaintainError::Storage`] if a referenced URI cannot be parsed
/// into an object key. Callers MUST treat an error as fail-closed (delete
/// nothing): a partial referenced set could omit a live file.
pub async fn collect_referenced_paths(table: &Table) -> Result<HashSet<ObjectPath>, MaintainError> {
    let metadata = table.metadata();
    let file_io = table.file_io();
    let mut referenced = HashSet::new();

    // The current metadata.json pointer (what the catalog points at).
    if let Some(location) = table.metadata_location() {
        referenced.insert(parse_object_key(location)?);
    }
    // Previous metadata files retained for rollback.
    for log in metadata.metadata_log() {
        referenced.insert(parse_object_key(&log.metadata_file)?);
    }
    // Table- and partition-level statistics files (Puffin) live under
    // `metadata/`; include them so the sweep never reclaims a referenced one.
    for stats in metadata.statistics_iter() {
        referenced.insert(parse_object_key(&stats.statistics_path)?);
    }
    for stats in metadata.partition_statistics_iter() {
        referenced.insert(parse_object_key(&stats.statistics_path)?);
    }
    // Every snapshot's manifest list, its manifests, and their files.
    for snapshot in metadata.snapshots() {
        referenced.insert(parse_object_key(snapshot.manifest_list())?);
        let manifest_list = snapshot.load_manifest_list(file_io, metadata).await?;
        for manifest_file in manifest_list.entries() {
            referenced.insert(parse_object_key(&manifest_file.manifest_path)?);
            let manifest = manifest_file.load_manifest(file_io).await?;
            for entry in manifest.entries() {
                referenced.insert(parse_object_key(entry.data_file().file_path())?);
            }
        }
    }
    Ok(referenced)
}
