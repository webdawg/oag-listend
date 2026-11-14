# oag-listend

**License: GNU Affero General Public License v3.0 or later.**

The global daemon that serves audio. From the founding statement (`USER_INPUT_RECORD.md`,
verbatim):

> this is the global daemon that serves all audio information to all networks ... a real audio
> serving service that serves audio to any present and future technology - any way that anyone
> connects to this service - it just serves audio

> the service also listens too all incoming but only accepting audio as an input and nothing else

## The shape

One stable **audio surface** behind pluggable **carriers**. The audio is the service; a carrier
(TCP, HTTP, UDP, IPFS, Reticulum, a protocol that does not exist yet) is one way to reach it. A
new network is a new carrier, never a change to the audio.

Two rules, both enforced at the surface so no carrier can forget them:

- **Outbound:** an item is identified by the BLAKE3 of its bytes, so a receiver on any carrier can
  verify what it got.
- **Inbound:** only audio is accepted. `AudioStore::accept` admits bytes only if they carry a
  recognised audio signature -- checked on the bytes, never on a declared type. Everything else is
  rejected before it is stored.

## Status

Founded. The audio surface exists (`AudioItem`, `AudioStore`, `AudioFormat::sniff`) with 5 tests.
No carrier is wired yet; the first is TCP, borrowed from the imagination layer's `Transport` seam.
See `SPEC.md` for the place in the stack and what this explicitly is not.

## Build

```
cargo test -p oag-listend
```

`scripts/` holds the same build-generation pruner as every other OAG repository; see
`scripts/README.md`.
