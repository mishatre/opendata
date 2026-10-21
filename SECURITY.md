# Security policy

## Supported versions

Only the latest published minor release receives security fixes until `1.0.0`; after that, the latest major version does.

## Reporting a vulnerability

Report suspected vulnerabilities privately through GitHub's
[security advisories](https://github.com/mishatre/opendata/security/advisories/new)
instead of a public issue. Include the affected version, a reproduction, and the
impact you expect.

You should receive an acknowledgement within seven days. Fixes are released as a
patch version and credited in the changelog unless you ask to stay anonymous.

## Scope

The library downloads publisher-controlled files over HTTP. Treat all discovered
metadata, structure, and CSV content as untrusted input: it is parsed under
bounded buffers and size limits, but it is not a security boundary against a
malicious publisher.

Cursors are opaque application state, not credentials, and the cache directory is
assumed to be private to the deployment.
