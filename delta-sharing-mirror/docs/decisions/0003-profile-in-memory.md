# ADR 0003: The sharing profile stays in memory; it is never written to /tmp

**Status:** accepted · 2026-10 (deviation from handoff §10 `profile.py`, which suggested a /tmp file)

## Context
The `delta-sharing` URL form `"<profile-file>#share.schema.table"` requires the profile JSON on
disk. The handoff asked that the bearer token appear only in Secrets Manager.

## Decision
Parse the secret with `DeltaSharingProfile.from_json` and build `DataSharingRestClient` /
`DeltaSharingReader` directly (the same objects the URL helpers construct internally, pinned at
`delta-sharing==1.4.2`). The token never touches the filesystem, environment or logs. Exception text
is passed through `log.redact()` which strips `Bearer …`, `bearerToken` values and pre-signed URL
signatures.

## Consequences
- We depend on two classes that are one level below the documented helper functions. The version
  is pinned and their signatures are checked by the integration tests, so an upgrade that changes
  them fails CI rather than production.
- Only the endpoint **host** is logged.
