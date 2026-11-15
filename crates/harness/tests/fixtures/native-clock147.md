# Native Explain sampled clock regression

`databricks_native_explain_clock147.jsonl` contains byte-identical original
rows 3, 5, 11, 13 and 14 (zero-based) of the complete native #147 capture,
SHA-256 `3b69f7884da6677f923ee09ff9d6bb2fb67433a2485de6661b945f4f7bbb78fa`.
It exercises public Submit → Confirm → unchanged Submit → Explain/reverse
Explain with the exact original requests and responses. This is a native
regression fixture, separate from the hosted corpus under `golden/fixtures`.

The clock sidecar was extracted independently from original Postgres WAL,
not inferred from Explain output. The successful native test had already
removed its temporary schema. A read-only copy of retained/recycled WAL file
`00000001000000000000005A` contained logical segment45, identified by its actual
page addresses. The identical bytes are pinned by `wal_sha256` in the sidecar.
Postgres17 `pg_waldump` CRC-validated the original records. A complete heap-tuple
decoder used the migrated 38-attribute layout, null bitmap, alignment and
varlena lengths; it asserted complete tuple consumption.

The first audit INSERT is LSN `0/451F9560`, xid167963; the unchanged audit
INSERT is `0/451FABC8`, xid167968, relation `1663/16384/402593`, block0,
tuple2. Its COMMIT is `0/451FB140`, `2026-11-14 21:13:27.038031 UTC`.
An independent full-page image at `0/45201BD8` contains both complete tuples;
its decoded attributes exactly equal the original INSERT attributes. The
sidecar pins the recovery decoder SHA-256. Raw WAL, full-page image and full
audit rows remain in the private evidence archive because they contain other
local test data.

For the unchanged request, captured client-send `1794690806989` precedes the
actual persisted `display_submitted_at_ms=1794690806996`; the UUID was minted
at `1794690807001`, before the response at `1794690807002`. The source epoch
is `1794690778000`. The actual sampled age is 28996ms, displayed as 28 seconds;
substituting the later UUID gives 29001ms, displayed as 29 seconds. The handler
samples time before its asynchronous store reads, and mints IDs afterward.
The production formatter and clock source are unchanged by this repair.

The initial minimal public replay failed twice (first 0.98 seconds). Injecting
only the independently recovered persisted clocks passed (1.45seconds), with
both original Explain trees compared exactly. The final regression additionally
retains and reloads the real audit clocks from its own production router.
Negative controls reject capture hash, completeness, duplicate ID, organization,
unknown ID, interval and provenance errors. Original full 89 traffic remains
unchanged and is separately replayed with all 12 recovered Submit audit clocks.
