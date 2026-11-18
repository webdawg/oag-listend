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

One additional individual directive is implemented:

- **Even pings are music:** every liveness ping answers with audio rather than an empty control
  pong. The canonical ping is a generated WAV rendition of “Twinkle, Twinkle, Little Star,” the
  same bytes and the same content hash on every daemon. Send `Ping` on the native protocol, or
  `GET /ping` over HTTP. See `SPEC.md` and `USER_INPUT_RECORD.md` Entry 3.

## Turn it on

    cargo run -p oag-listend -- --listen 127.0.0.1:7100 --http 127.0.0.1:7101 --udp 127.0.0.1:7120 \
        --unix /tmp/oag-listend.sock \
        --dir /var/lib/oag-listend --seed 20261112

The daemon binds, prints four generated items -- silence, tone, noise, and the canonical ping -- and
serves until interrupted. The generated set seeds an empty store only -- a restart with `--dir` does not add more -- and a generated sound the
store refuses is printed as a rejection rather than dropped.

`--seed U64` chooses the generated sounds (the default comes from the clock and process id);
`--no-generate` skips them entirely, for an operator who wants the daemon to serve only what it is
given.

`--ipfs-api ADDR` also offers every accepted item to a Kubo node at `ADDR`, so audio reaches a network
whose clients never have to connect here. It is best-effort: the bytes are written locally first, and
a mirror failure is reported rather than allowed to lose audio. It is also idempotent -- a repeated
publish, and every restart, adds what is already recorded exactly once rather than rewriting the
library. At shutdown the daemon prints how many items were mirrored and any failures, with each CID:

    oag-listend --ipfs-api 127.0.0.1:5001
    ...
    oag-listend shutting down
      ipfs mirror: 1 mirrored, 0 failed
        5f1a88dee8bd40af -> bafywired

`GET /ipfs/{cid}` also *reads* by content identifier. Audio this daemon already holds is served
straight from the store, so a Kubo node that is down does not take held audio with it; only an
unfamiliar CID goes out to the node; what comes back is then held to
the same audio rule as every other inbound path, so a non-audio object is refused (never echoed,
never stored) and an unreachable node answers 502. It answers with `Cache-Control: immutable`, an
`ETag` of the CID, and `X-Content-Hash` carrying the BLAKE3 of the bytes actually sent, so the
receiver can prove what it got. Without `--ipfs-api` the route does not exist.

A client that fetches audio is told where else it lives: the reply carries the CID when the item
has been mirrored -- as `X-IPFS-CID` on HTTP, and as an optional trailing field on the native
protocol's audio response (a daemon that predates it sends nothing, and still decodes).

A restart with `--ipfs-api` also offers what is already stored, not only audio published after it
started, so an existing library reaches IPFS too; that pass is bounded as a whole so a large store
cannot hold startup open.

A TLS (`https://`) endpoint is refused plainly rather than silently downgraded to a cleartext
request that would appear to work.

A chunked reply is refused rather than misread as audio -- treating the framing as bytes would
silently corrupt what is served. This client has been tested against stubs that mimic `/api/v0/add`
and `/api/v0/cat`; it has **not** been tested against a real Kubo node. See `SPEC.md`. Then exchange audio with it over the native protocol:

    cargo run -p oag-listend --example push_pull -- --addr 127.0.0.1:7100
    cargo run -p oag-listend --example push_pull -- --udp --addr 127.0.0.1:7120
    cargo run -p oag-listend --example push_pull -- --unix /tmp/oag-listend.sock

or over HTTP, with any client:

    curl -X POST --data-binary @tone.wav http://127.0.0.1:7101/audio   # -> the content hash
    curl http://127.0.0.1:7101/audio/<hash>                            # -> the same bytes
    curl http://127.0.0.1:7101/                                        # -> what this is
    curl http://127.0.0.1:7101/audio                                  # -> the content hashes, sorted
    curl "http://127.0.0.1:7101/audio?offset=2&limit=2"               # -> one page, with X-Total-Count
    curl -I http://127.0.0.1:7101/audio/<hash>                         # -> headers, no body
    curl -r 0-1023 http://127.0.0.1:7101/audio/<hash>                  # -> 206, a byte range
    curl -H 'If-None-Match: "<hash>"' http://127.0.0.1:7101/audio/<hash> # -> 304, revalidate
    curl http://127.0.0.1:7101/ping                                       # -> the musical ping
    curl http://127.0.0.1:7101/ipfs/<cid>                                 # -> audio fetched from IPFS

All four carriers share one store, and all accept audio and nothing else.

`--dir` persists the audio content-addressed under a directory, so a restart loads it back:

    # first run: post a sound, then stop the daemon
    curl -X POST --data-binary @tone.wav http://127.0.0.1:7101/audio
    # second run: the same sound is served again
    curl http://127.0.0.1:7101/audio/<hash>

Every entry is re-checked on load (name must be the BLAKE3 of the bytes, bytes must sniff as
audio), and a write failure refuses the inbound rather than losing it silently. The write is
asynchronous, so a slow disk delays the answer rather than stalling every carrier. The startup report
separates `loaded`, `skipped`, `evicted`, and `truncated`, so a bound that pushed entries out -- or
a directory with more entries than the load will consider -- is visible rather than counted as held.

`--max-bytes` is enforced on the way in: audio larger than the bound is refused with `413 Payload Too
Large` rather than written and immediately evicted, and an oversized startup file is skipped without
being read.
A single item is also capped by what one frame can carry (`MAX_FRAME_BYTES - 256`), so a store
bound above the frame cap cannot admit an item that no client could fetch.
Verified by hand: a 4044-byte WAV against `--max-bytes 100` returned
`audio is 4044 bytes, larger than the 100 byte store bound`, while a small WAV was accepted.

The HTTP content type comes from the bytes, not from what the publisher claimed; `Range`, `HEAD`,
`GET /audio`, and `Expect: 100-continue` are supported, so a browser can stream and seek and a
client can discover what exists. CORS is allowed (`Access-Control-Allow-Origin: *`) and `OPTIONS`
is answered, so a page on another origin can `fetch()` the audio. Verified by hand with `curl`.

## Run it as a service

`scripts/oag-listend.service` is a systemd unit for all four carriers with a persistent store.
Install and start it with:

    cargo build --release -p oag-listend
    sudo install -m 0755 target/release/oag-listend /usr/local/bin/oag-listend
    sudo useradd --system --no-create-home --shell /usr/sbin/nologin oag
    sudo install -m 0644 scripts/oag-listend.service /etc/systemd/system/oag-listend.service
    sudo systemctl daemon-reload
    sudo systemctl enable --now oag-listend

`--check` opens the store and prints the load report without serving and without contacting an IPFS
node, so a store can be verified quickly before the service is started; `--version` prints the binary version and the frame protocol
version.

The daemon handles both SIGINT and SIGTERM, so `systemctl stop` is a clean stop: the accept loops
return, the Unix socket is removed, and the process exits 0.

The default bind is `0.0.0.0`, which serves every interface; when a carrier binds a non-loopback
address the daemon prints a note saying so. Use `--listen 127.0.0.1:...` to keep it local.

## Status

**Serves, and survives a restart.** Five ways to reach the audio are live and tested: the native
protocol over TCP, HTTP, UDP, a Unix socket, and an IPFS mirror -- 64 unit tests plus 59 end-to-end
tests (`cargo test -p oag-listend`). The store mutex covers lookup and insert, not byte
copies or disk writes, so one slow client does not stall the others. Runs as a systemd service
(`scripts/oag-listend.service`) and shuts down cleanly on SIGINT and SIGTERM. The native protocol, HTTP, UDP, and the Unix socket share one store,
and all were verified by hand: audio posted to HTTP came back byte-identical, `push_pull` verified
over TCP and UDP, HTML labelled `audio/wav` was refused on every carrier, and a daemon restarted
with `--dir` loaded 5 items and served the posted WAV byte-identical.
The audio surface (`AudioItem`, `AudioStore`, `AudioFormat::sniff`) is unchanged; the rules did not
move to the wire, they were already at the surface. See `SPEC.md` for the carriers and the bounds.

## Build

```
cargo test -p oag-listend
```

Run every gate in one command before pushing:

```
scripts/check.sh             # fmt, clippy, rustdoc, tests
scripts/check.sh --release   # the same, plus the suite in release
```

`scripts/` also holds the same build-generation pruner as every other OAG repository; see
`scripts/README.md`.
