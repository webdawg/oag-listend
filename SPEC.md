# oag-listend

**License: GNU Affero General Public License v3.0 or later (AGPL-3.0-or-later).**

**Status: serves, and survives a restart.** Four carriers are live and share one store: the
native length-prefixed protocol over TCP, HTTP, UDP, and a Unix domain socket. The store is content-addressed, so `--dir` persists
every accepted sound under its BLAKE3 and a restart loads it back. The daemon publishes a small
seeded set of generated sounds, accepts audio, and serves it back with the BLAKE3 of the bytes. The
end-to-end tests `the_daemon_serves_audio_and_refuses_everything_else`,
`http_serves_audio_and_refuses_the_rest`, `the_two_carriers_serve_the_same_audio`,
`udp_carries_the_same_audio`, `audio_accepted_before_a_shutdown_is_served_after_it`,
`http_range_requests_and_head`, `the_served_content_type_comes_from_the_bytes`, and
`http_lists_the_audio_and_honours_expect_continue` are green.
The audio rules have not moved.

## The idea, recorded verbatim

> this is the global daemon that serves all audio information to all networks

> we are building a reall audio serving service that serves audio to any present and future
> technology - any way that anyone connects to this service - it just serves audio

> the service also listens too all incoming but only accepting audio as an input and nothing else

## What it is

A daemon whose only job is to serve audio. Not a player, not a library manager, not a codec
project: a service that, however a client reaches it, answers with audio.

The founding statement makes two claims that shape the architecture:

1. **"To all networks"** and **"any way that anyone connects."** The transport is not the
   service. The service is the audio; a transport is one way to reach it. This is the same
   carrier seam the imagination layer already uses (`socket::Transport` -- TCP, UDP, HTTP, an HTTP
   CONNECT tunnel, IPFS, Reticulum). oag-listend owns the audio; carriers are pluggable and
   interchangeable, and a new network means a new carrier, never a change to the audio.

2. **"Any present and future technology."** The daemon must not assume a client. A browser, a
   phone, a radio bridge, a mesh node, a protocol that does not exist yet -- each is a carrier
   that implements the seam. The audio surface is the stable contract; everything in front of it
   is replaceable.

## Individual directive: even pings are music

> even pings are music - start with twinkle twinkle little star

A liveness probe, heartbeat, keepalive, or carrier-level ping must not introduce a silent control
plane beside the audio service. When oag-listend answers a ping, the answer itself is audio. The
first canonical ping sound is a generated WAV rendition of the opening phrase of **“Twinkle,
Twinkle, Little Star.”** A future carrier may choose a different encoding appropriate to its
network, but it must preserve the musical content rather than replacing it with an empty pong.

This directive is recorded now but is not implemented by the current carriers yet: the native
protocol's `Hello` response is metadata, not the future musical ping. Compatibility must be
preserved by adding an explicit audio ping rather than silently changing the existing `Hello`
frame.

Implementation TODO:

- [ ] Add a deterministic Twinkle melody generator to `crates/oag-listend/src/wav.rs`.
- [ ] Add a native protocol ping operation whose response is the Twinkle audio item and its BLAKE3
      content hash.
- [ ] Expose the same musical liveness response on HTTP and every later carrier.
- [ ] Test that a ping response is recognized audio, hash-verifiable, bounded, and never an empty
      metadata-only pong.

## The inbound rule

The daemon listens on every carrier, and the only thing it accepts is audio. Anything else that
arrives is rejected before it is stored -- not parsed, not forwarded, not logged as content.

This is enforced at the audio surface, not per carrier, so a new carrier cannot forget it: every
inbound path goes through `AudioStore::accept`, which admits bytes only if they carry a
recognised audio signature. The check is on the bytes, never on a declared content type, because a
declared type is a claim and the bytes are the evidence. For ISO BMFF the whole `ftyp` brand list
is read, so a generic `isom`/`mp42` video is rejected while an M4A that carries `isom` as its major
brand and `M4A ` as a compatible brand is accepted. For Ogg, the beginning-of-stream page's codec
header is read, so Vorbis/Opus/FLAC pass and Theora (video) does not. A carrier may *additionally* reject
earlier (an HTTP carrier can refuse a non-audio `Content-Type` before reading the body), but the
surface is the guarantee.

## Place in the stack

- **Layer 0, open-agent-graph** holds the evidence: what audio exists, who published it, its
  content hash and pinned CID. oag-listend reads that; it does not duplicate it.
- **Layer 1, oag-mesh** federates: an audio item known in one cluster is discoverable in another.
- **Layer 1 parallel, oag-imagination** supplies the carrier seam and the data-pad mechanism -- a
  large audio file is a thousand pads, gathered and reconstructed.
- **Layer 2, oag-collisions** infers: two independently published recordings that collide are the
  same recording.
- **oag-listend** is the serving surface above all of them. It is a consumer of the layers below
  and a writer to none of them.

## What this explicitly is not

- **Not a transcoder.** It serves the bytes it holds. Format negotiation is a carrier concern.
- **Not a rights system.** It serves what it is given. Provenance is layer 0's job.
- **Not a single protocol.** The day it is "the HTTP audio server" is the day it has failed the
  founding statement.

## The carrier that exists: TCP

`oag-listend` (the binary) binds an address, and `examples/push_pull.rs` exchanges audio with it.
The protocol is length-prefixed frames with exactly four operations -- hello, publish, fetch,
list -- and nothing else. Every inbound publish goes through `AudioStore::accept`; every outbound
fetch returns the bytes and the hash they must verify against.

    oag-listend --listen 0.0.0.0:7100 --http 0.0.0.0:7101 --udp 0.0.0.0:7120 \
                --dir /var/lib/oag-listend --seed 20261112
    cargo run -p oag-listend --example push_pull -- --addr 127.0.0.1:7100
    cargo run -p oag-listend --example push_pull -- --udp --addr 127.0.0.1:7120

Bounds: every read and write syscall has a 10 s timeout -- per syscall, not per transfer, so a
slow client uploading or downloading the largest item is not cut off mid-transfer -- and each
session and each HTTP connection also has a total bound (30 minutes), covering the read, the route,
and the response, so a client that dribbles a byte every few seconds in either direction cannot hold
one of the 64 session slots open indefinitely. A frame may not exceed 16 MiB, the store is capped by
`--max-bytes` (1 GiB by default), and at most 64 sessions are served at once per carrier.
A single item is capped by `servable_bound` -- the store bound capped by what one frame can carry
(`MAX_FRAME_BYTES - 256`) -- so no accepted item can be one that a client's own decoder would
reject. A store bound above the frame cap does not admit unfetchable items.
A fresh daemon publishes four generated sounds (silence, tone, noise) through the same inbound
rule a client's bytes use, so a fetch-only client still receives audio. The set seeds an *empty*
store: a restart with a persistent store is left alone, because otherwise every start would add
four more sounds and, with the default clock seed, slowly evict the operator's own audio. A
generated sound the store refuses is reported, not dropped.

## The HTTP carrier

The same audio surface over HTTP/1.1, so a browser can reach it. `GET /` reports the daemon name,
the protocol version, the item count, and the store's byte usage against its bound. `GET /audio` lists the content hashes one per line,
sorted by hash, paginated with `?offset=&limit=` and reported through `X-Total-Count` and
`X-Offset` -- so a client that only fetches can discover what exists, and the walk is the same
across a restart. `GET /audio/{hash}` serves the bytes with a content type derived from the bytes,
with `Range` (206 and `Content-Range`, or 416), `HEAD`, and an `ETag` (the content hash) with
`Cache-Control: public, max-age=31536000, immutable` and `If-None-Match` (including a comma-separated validator list, and `*`) -> 304, since a
content-addressed item cannot change. `POST /audio` accepts a body through
`AudioStore::accept` (201 with the content hash, 400 if the bytes are not audio) and answers
`Expect: 100-continue`. A query string is never part of the path, so `/audio/{hash}?x=1` serves the
audio rather than 404-ing. Every response carries `Access-Control-Allow-Origin: *` and `OPTIONS` is
answered, so a page on another origin can `fetch()` the audio. Anything else is 404.

It is a hand-rolled request/response loop with no framework: one request, one response, then
close. All four carriers bind around one shared `Arc<Mutex<AudioStore>>`, so a publish on any is
served on any.

## The UDP carrier

The imagination layer's datagram seam, serving the same audio: one datagram is one request, the
reply goes back to the sender, and the frame codec is reused unchanged. A datagram is at most
65,507 bytes, so a fetch whose response does not fit is answered with a rejection -- never a silent
truncation. Small exchanges (hello, list, a generated tone, a short recording) are this carrier's
job; a long recording belongs on TCP or HTTP.

## Persistence

`--dir PATH` makes the store content-addressed: `<PATH>/<hash>` is the audio, `<PATH>/<hash>.json`
is the declared media type. An accepted item is written before it is answerable -- a write failure
is a refusal, not a silent loss. The write is asynchronous (`tokio::fs`), so a slow disk delays the
answer instead of stalling every carrier's executor thread; the eviction unlink stays synchronous,
being metadata-only. On startup every entry is re-checked: the name must be the BLAKE3
of the bytes and the bytes must still sniff as audio, so a hand-placed file cannot smuggle anything
in. The load is reported as `loaded` (entries now held), `skipped` (not read, or rejected), and
`evicted` (read and accepted, then pushed out by the byte bound while later entries loaded) -- never
silent, and never counting an evicted entry as held.

Eviction deletes the file, so the directory and the store agree on what is held and the
`--max-bytes` bound is a bound on disk too, not just memory. Without `--dir` the store is in-memory.

The bound is enforced on the way in: `accept` refuses audio larger than `--max-bytes` rather than
writing it and immediately evicting it, and a startup file larger than the bound is skipped without
being read. A bound that is only checked after the read or write is not a bound.

## The hello

`Hello` answers with the daemon name (`oag-listend`), the frame protocol version, and the item
count. A client can therefore tell what it reached and whether it speaks the same protocol, instead
of inferring from a bare count. An unrecognised name still decodes, so a future daemon is
recognisable rather than a protocol error.

## Concurrency

All carriers share one store behind one mutex, so accepts and inserts serialise. The mutex covers
the lookup and the insert, not the byte copies: `serve` returns an `Arc<AudioItem>`, so a large
`Fetch` releases the lock before copying the audio, and the persistent write is asynchronous. A
slow disk or a large item delays the answer that caused it, not every other client.

## Listing order and pages

The list operation is paginated: `List { offset, limit }` returns one page and the store's `total`
(`limit` 0 means the default page, not one item),
so a client can walk the whole store without guessing. The page is bounded (`MAX_LIST_LIMIT` on a
stream carrier, a smaller cap on UDP) because an unbounded listing would eventually exceed the frame
cap -- the daemon would send a frame its own client has to reject.

The list operation returns content hashes sorted by hash. Insertion order is reproducible within a
run but not across a restart, because a restart rebuilds the store in directory-iteration order;
a sorted walk is reproducible always. A listing whose order changes between a client's requests --
or across a daemon restart -- is a reproducibility hazard with no benefit.

## The Unix domain socket carrier

`--unix PATH` serves the same frame protocol over a filesystem socket, so a local process reaches
the daemon by path rather than by port: no port to allocate or collide, and the file's permissions
decide who may connect. It shares the TCP session loop, so the protocol, the bounds, and the
inbound rule are identical by construction. A stale socket file from a killed daemon is removed on
bind and on shutdown.

## Saying what it hears

The default bind is `0.0.0.0`, which serves every interface. When a carrier binds a non-loopback
address the daemon says so, rather than letting an operator discover it later.

## Running it

`scripts/oag-listend.service` runs all four carriers with a persistent store under systemd. The
daemon handles SIGINT and SIGTERM, so `systemctl stop` is a clean stop: the accept loops return, the
Unix socket is removed, and the process exits 0.

## Next

The remaining imagination carriers are IPFS and Reticulum, and both are deliberately not
implemented: IPFS needs a Kubo node (external infrastructure) or an embedded libp2p node
(heavyweight), and Reticulum needs a physical interface. Each would reuse `AudioStore::accept` and
add no operations, but neither can be tested against a real network from this repository, so the
honest status is "not built", not "built and untested".
