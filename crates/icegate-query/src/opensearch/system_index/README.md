# System indices

Index names that start with `.` are system indices: per-tenant JSON documents with their
mappings, settings and aliases (saved objects, for example), held on object storage beside
(never inside) the telemetry tables. The code is `opensearch/system_index/`; how each endpoint
answers is documented on its handler.

## Layout

Under `opensearch.system_indices.location` (an `s3://` or `s3a://` URL; by default a prefix of
the storage bucket, `SystemIndicesConfig::DEFAULT_LOCATION_PREFIX`), each tenant has one
directory named by the tenant id verbatim:

- `<tenant>/current`: the **pointer**, a small mutable object naming the current snapshot
  object;
- `<tenant>/snap-<uuid>`: immutable **snapshot objects**, each holding the tenant's whole
  system-index state (indices, mappings, settings, aliases, documents with `_seq_no` and
  `_primary_term`). A snapshot object is a zstd skippable frame holding the **lineage** (the
  predecessor's snapshot name and the ids of the operations this snapshot object applied),
  followed by the zstd frame of the snapshot JSON. Every snapshot object is therefore valid zstd,
  and its lineage is readable with two ranged reads, without decoding the snapshot;
- `<tenant>/gc-probe`: the clock probe garbage collection writes (below).

The characters `TenantId` admits (`icegate_common::TenantId`) need no escaping in an object key
(pinned against RustFS by `a_colon_tenant_segment_round_trips_create_update_conditional_get_and_head`
in `crates/icegate-query/tests/opensearch/system_store_rustfs.rs`), so the mapping from tenant to
directory is the identity and never collides. Keys are never parsed back into a tenant.

## Reads

A read sends a conditional GET of the pointer with the `ETag` the replica cached; not-modified
means the cached snapshot is current, so a steady read costs one small request. An absent pointer
is the empty state. Decoded snapshots are cached per tenant, least recently used first out, within
`cache_max_bytes`. A write is therefore visible to the next request on any replica.

## Writes

One write loop per tenant per replica takes the queued operations, applies them to the current
snapshot, writes the new snapshot object, then swaps the pointer by compare-and-swap on its
`ETag` (a create-if-absent when there is no pointer yet). Operations queued together land in one
swap, and a batch that changes nothing stores nothing and swaps nothing. Both refusals of a swap
(a stale update and a create on an existing pointer) are a conflict: the loop reloads and applies
again, retrying with jittered backoff until `engine.max_query_duration_secs`, after which the
write answers 503; contention is never a 409. A snapshot object whose swap is known to have
failed is deleted by its writer.

**Landed detection.** After a conflict or an unknown outcome (a transient error, a lost answer),
the writer cannot tell from the answer whether its swap landed. It reloads the pointer and walks
the lineage from the current snapshot object back to the one its attempt read. Finding one of its
own snapshot objects on that path means that attempt landed, and the writer answers with the
outcomes it computed then, so an `incrementCounter` is never applied twice and a create never
conflicts with itself. Not finding one means none landed, and the writer applies again on the
current state; its operation ids found in a snapshot object no attempt of its wrote end the write
with a 500. The walk is exact: there is no window of recent operations that a burst of other
writes could overflow. A lineage frame missing on that path ends the write with the 503 answer
instead of applying again.

## Scroll

A scroll keeps no server state. Its id is the cursor itself (base64url JSON): the snapshot
object the initial search read, the concrete index, the offset of the next page in `_id` order,
the initial request (query string and body), its opening time and its keep-alive. A snapshot
object never changes and every page evaluates at the opening time, so paging it by offset returns
every matching document exactly once, on any replica, while writes continue. The expiry is the
opening time plus the initial search's `scroll`, at most `max_scroll_lifetime_secs`. A
continuation's `scroll` is validated the same way and never extends the expiry: the garbage
collection bound below holds only for a scroll that ends at most `max_scroll_lifetime_secs` after
it opened. A superseded snapshot a scroll reads is fetched and decoded once, then kept in the
decoded-snapshot cache under `cache_max_bytes` beside the pointer states, which it never evicts.
A continuation past the expiry, or whose snapshot object is gone, answers 404
`search_context_missing_exception`; it never falls back to a newer snapshot. A scroll id is not
signed: one whose keep-alive exceeds `max_scroll_lifetime_secs`, or whose opening lies more than
`engine.max_query_duration_secs` after the request, is refused as malformed, and a client that
rewrites an id reads at most its own tenant's snapshot objects until garbage collection removes
them.

## Garbage collection

A snapshot object is deleted only when the pointer does not name it and its newest successor
(the newest snapshot object whose lineage names it) was written more than
`max_scroll_lifetime_secs + engine.max_query_duration_secs` ago. That covers every scroll opened
on it (it expires at most `max_scroll_lifetime_secs` after it opened, while the object was
current; the swap that ended that came at most the query deadline after the successor was
written) and every write attempt that read it (bounded by the query deadline). Ages are measured
on the object store's clock only: a pass writes `<tenant>/gc-probe` and reads its
`Last-Modified`, then compares it with the listed `Last-Modified` of the successors. A snapshot
object without a successor, a tie and a successor stamped after the probe keep the object; a
lineage frame that cannot be read stops the pass before it deletes anything, and a snapshot
object that disappears between the listing and its lineage read is skipped. A pass reads the
pointer before it lists and keeps the object the pointer names: an object current at that read
stays, and an object superseded before it had the successor it was swapped to written before the
listing, so the listing shows that successor and its age. The bound above therefore holds for a
pass that starts late too; a pointer that cannot be read stops the pass. Deleting an object that
is already gone counts as success, so concurrent passes on several replicas are safe.

The write loop of a replica runs a pass for its tenant after a successful swap, at most once per
`max_scroll_lifetime_secs` and not before that long after the replica started. A scroll opened on
one replica may continue on another, and its expiry is checked against the request time of the
replica that serves it, so clock skew between replicas eats into the same
`engine.max_query_duration_secs` margin as the gap between writing a successor and swapping the
pointer to it: the scroll bound holds while the two together stay within it.

## Bounds and growth

- `max_snapshot_bytes` bounds the decoded (uncompressed JSON) snapshot. A write whose result
  would exceed it answers 413 in the error envelope and leaves the pointer unchanged. The default
  is sized to hold two maximum saved-object imports plus one reindexed copy of both; see
  `SystemIndicesConfig`'s doc for the figure.
- `cache_max_bytes` bounds the decoded snapshots one replica caches; it is at least
  `max_snapshot_bytes`.
- IceGate serves no `DELETE /<index>` and never retires a superseded copy of an index on its own:
  an index leaves the snapshot only through a `remove_index` action of `POST /_aliases`. A client
  that migrates by copying its documents into a new index and keeps the old one adds a full copy
  per migration. Repeated migrations approach `max_snapshot_bytes`. After that, writes answer 413
  until the bound is raised or an old copy is removed with `remove_index`.
- A snapshot object that no pointer ever named stays when its writer could not delete it: a
  delete that failed, a write that ended in an error before it settled its swap, or a replica
  that stopped between storing the object and settling its swap. Garbage collection never deletes
  it, because no snapshot object names it as a predecessor. Each such object holds one snapshot
  within `max_snapshot_bytes` (decoded), and their number grows with those failures, not with the
  write rate.
- Each replica remembers when it last started a garbage-collection pass for a tenant: one small
  entry for every tenant whose writes on that replica started a pass, kept for the replica's life
  and never evicted. It grows with the number of tenants, not with the write rate.

## Object-store requirements and the RustFS evidence

The store needs conditional create, compare-and-swap on `ETag`, conditional GET with
`If-None-Match`, and listings that report the same whole-second `Last-Modified` as a HEAD. At
startup it probes the first three under the location and refuses to start when one fails. It
builds its own S3 client (`system_index/client.rs`) rather than the layered storage registry,
whose read cache would serve a stale pointer.

These guarantees are pinned against RustFS by
`crates/icegate-query/tests/opensearch/rustfs_conditional.rs` and
`crates/icegate-query/tests/opensearch/system_store_rustfs.rs`, run against the RustFS image and
the standalone (single-volume) mode pinned in `crates/icegate-common/src/testing/container.rs`. Changing the RustFS
image or mode of a deployment requires running both against it first:

```bash
cargo test -p icegate-query --test opensearch_tests -- rustfs_conditional system_store_rustfs
```
