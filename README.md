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

Two implemented rules, both enforced at the surface so no carrier can forget them:

- **Outbound:** an item is identified by the BLAKE3 of its bytes, so a receiver on any carrier can
  verify what it got.
- **Inbound:** only audio is accepted. `AudioStore::accept` admits bytes only if they carry a
  recognised audio signature -- checked on the bytes, never on a declared type. Everything else is
  rejected before it is stored.

One additional individual directive is recorded for the next implementation step:

- **Even pings are music:** every liveness ping must answer with audio rather than an empty control
  pong. The first canonical ping will be a generated WAV rendition of “Twinkle, Twinkle, Little
  Star.” See `SPEC.md` and `USER_INPUT_RECORD.md` Entry 3.

## Turn it on

    cargo run -p oag-listend -- --listen 127.0.0.1:7100 --http 127.0.0.1:7101 --seed 20261112

The daemon binds, prints four generated sounds, and serves until interrupted. Then exchange audio
with it over the native protocol:

    cargo run -p oag-listend --example push_pull -- --addr 127.0.0.1:7100

or over HTTP, with any client:

    curl -X POST --data-binary @tone.wav http://127.0.0.1:7101/audio   # -> the content hash
    curl http://127.0.0.1:7101/audio/<hash>                            # -> the same bytes
    curl http://127.0.0.1:7101/                                        # -> what this is

Both carriers share one store, and both accept audio and nothing else.

## Status

**Serves.** Two carriers are live and tested: 20 unit tests plus 4 end-to-end tests
(`cargo test -p oag-listend`). The native protocol and HTTP share one store, and both were verified
by hand: audio posted to HTTP came back byte-identical, and HTML labelled `audio/wav` was refused.
The audio surface (`AudioItem`, `AudioStore`, `AudioFormat::sniff`) is unchanged; the rules did not
move to the wire, they were already at the surface. See `SPEC.md` for the carriers and the bounds.

## Build

```
cargo test -p oag-listend
```

`scripts/` holds the same build-generation pruner as every other OAG repository; see
`scripts/README.md`.
