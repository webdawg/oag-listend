# oag-listend

**License: GNU Affero General Public License v3.0 or later (AGPL-3.0-or-later).**

**Status: serves, and survives a restart.** Five carriers are live and share one store -- the
native length-prefixed protocol over TCP, HTTP, UDP, a Unix domain socket, and (opt-in, see
"The Reticulum carrier") Reticulum -- plus an HTTP CONNECT tunnel that reaches that same surface
through a proxy, and `--ipfs-api`, which additionally mirrors accepted audio to IPFS. The store is
content-addressed, so `--dir` persists every accepted sound under its BLAKE3 and a restart loads it
back. The daemon publishes a small seeded set of generated sounds, accepts audio, and serves it back
with the BLAKE3 of the bytes.

`cargo test -p oag-listend` runs 73 unit and 77 end-to-end tests, all green, in debug and
release (`79 unit and 81 end-to-end` with `--features reticulum`). End to end they cover:

- the native back-and-forth: publish, fetch, verify, reject non-audio, not-found, and frames written
  together in one write answered in order;
- the CONNECT tunnel: hello, publish, fetch, verify and refusal all reached through a proxy;
- eviction under the byte bound, and eviction removing the recorded CID sidecar;
- the per-frame cap refusing an oversized length before any body is read, and short or lying
declared lengths on the stream carriers ending rather than waiting;
- native listing pagination, including pages past the end;
- each of the six named audio signatures, in both directions, over the wire, and HTTP serving every
format under its own content type;
- HTTP refusal, streaming, ranges, HEAD, validators, CORS, oversized bodies with and without
`Expect`, header floods, chunked and other transfer encodings, and duplicated `Content-Length`
headers;
- the shared store across carriers, UDP, the Unix socket, and persistence across a restart;
- concurrency: many publishers, a stalled reader not stalling others, the session cap, and a hung
IPFS node not stalling the daemon;
- the IPFS mirror: mirroring accepted audio, serving by CID (local-first for what is held),
idempotence within a run and across a restart, refused cleartext downgrades, refused chunked
replies, a refused non-plausible CID from the node, and refusals that never echo or store what
came back;
- the musical ping on every carrier, answering with audio rather than an empty control pong;
- the Reticulum carrier over its own TCP interface, when built with `--features reticulum`: a
  client publishes, fetches and verifies, answers a ping with audio, and is refused non-audio;
- hostile input: deterministic fuzzing of the codec and of all four carriers, with liveness asserted
afterwards;
- bounds that are only real if observed: an item at the servable bound is fetched back whole and one
  byte more is refused; oversized sidecars are ignored while small ones are read; a load over the
  bound evicts and unlinks; every load-path skip reason is counted; the mirror log is capped;
- a client hanging up mid-reply leaves both the native and HTTP carriers serving;
- IPv6: all four carriers serve over `[::1]`, the stream and HTTP ones connecting to the literal
  address rather than the one the carrier reported;
- a daemon whose stdout cannot be written (`> /dev/full`) still serves audio and still exits 0;
- the operator surface: the daemon binary (`--version`, `--help`, usage exit codes, start/serve/stop,
`--check`, refused startup generation, non-loopback warnings) and the `push_pull` client's CLI.

`./scripts/check.sh [--release]` holds the whole bar in one command: fmt, clippy with warnings
denied, rustdoc with warnings denied, examples built, the test suite, and `systemd-analyze verify`
on the shipped unit. The audio rules have not moved.

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

Implemented. `wav::twinkle()` generates the opening phrase (C C G G A A G) deterministically; the
native protocol's `Ping` operation, HTTP's `GET /ping`, and the UDP and Unix carriers all answer
with that audio item and its BLAKE3 hash. The existing `Hello` frame is unchanged, so compatibility
is preserved. The rendition is kept short on purpose -- 44,844 bytes -- so it fits the smallest
carrier's datagram; a longer one would have made UDP answer with a rejection instead of music. The
canonical ping is also part of the startup set, so it is durable and fetchable by hash.

- [x] Add a deterministic Twinkle melody generator to `crates/oag-listend/src/wav.rs`.
- [x] Add a native protocol ping operation whose response is the Twinkle audio item and its BLAKE3
      content hash.
- [x] Expose the same musical liveness response on HTTP and every later carrier.
- [x] Test that a ping response is recognized audio, hash-verifiable, bounded, and never an empty
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

## The native TCP carrier

`oag-listend` (the binary) binds an address, and `examples/push_pull.rs` exchanges audio with it.
The protocol is length-prefixed frames with exactly five operations -- hello, publish, fetch,
list, ping -- and nothing else. Every inbound publish goes through `AudioStore::accept`, reached via
the same `accept_and_mirror` entry point every carrier uses; every outbound fetch returns the bytes
and the hash they must verify against; a ping returns the musical liveness item.

    oag-listend --listen 127.0.0.1:7100 --http 127.0.0.1:7101 --udp 127.0.0.1:7120 \
                --unix /tmp/oag-listend.sock --dir /var/lib/oag-listend --seed 20261112

(The systemd unit uses `/run/oag-listend/audio.sock`, which its `RuntimeDirectory=` creates; a
bare `cargo run` should use a path it can create.)
    cargo run -p oag-listend --example push_pull -- --addr 127.0.0.1:7100
    cargo run -p oag-listend --example push_pull -- --udp --addr 127.0.0.1:7120
    cargo run -p oag-listend --example push_pull -- --unix /tmp/oag-listend.sock

Bounds: every read and write syscall has a 10 s timeout -- per syscall, not per transfer, so a
slow client uploading or downloading the largest item is not cut off mid-transfer -- and each
session and each HTTP connection also has a total bound (30 minutes), covering the read, the route,
and the response, so a client that dribbles a byte every few seconds in either direction cannot hold
one of the 64 session slots open indefinitely. A frame may not exceed 16 MiB, the store is capped by
`--max-bytes` (1 GiB by default), and at most 64 sessions are served at once per carrier.
A single item is capped by `servable_bound` -- the store bound capped by what one frame can carry
(`MAX_FRAME_BYTES - 256`) -- so no accepted item can be one that a client's own decoder would
reject. A store bound above the frame cap does not admit unfetchable items.
A fresh daemon publishes three seeded sounds -- silence, tone, noise -- plus the canonical ping
through the same inbound rule a client's bytes use, so a fetch-only client still receives audio. The set seeds an *empty*
store: a restart with a persistent store is left alone, because otherwise every start would add
four more sounds and, with the default clock seed, slowly evict the operator's own audio. A
generated sound the store refuses is reported, not dropped.

## The HTTP carrier

The same audio surface over HTTP/1.1, so a browser can reach it. `GET /` reports the daemon name,
the protocol version, the item count, and the store's byte usage against its bound. `GET /ping`
answers with the canonical musical liveness item (see above), not a control string. `GET /audio` lists the content hashes one per line,
sorted by hash, paginated with `?offset=&limit=` and reported through `X-Total-Count` and
`X-Offset` -- so a client that only fetches can discover what exists, and the walk is the same
across a restart. `GET /audio/{hash}` serves the bytes with a content type derived from the bytes,
with `Range` (206 and `Content-Range`, or 416), `HEAD`, and an `ETag` (the content hash) with
`Cache-Control: public, max-age=31536000, immutable` and `If-None-Match` (including a comma-separated validator list, and `*`) -> 304, since a
content-addressed item cannot change. `POST /audio` accepts a body through
`AudioStore::accept` -- reached through the same `accept_and_mirror` entry point as every other
carrier (201 with the content hash, 400 if the bytes are not audio, 413 if they are
audio but exceed the store bound or the frame cap) and answers
`Expect: 100-continue`. Any transfer encoding is refused -- `Transfer-Encoding: chunked` and every
other value alike -- with a message saying to send a `Content-Length`, and so is a request carrying
more than one `Content-Length` header: two of them make the body's extent ambiguous and a reverse
proxy in front may pick a different one than the daemon (RFC 7230 3.3.3 requires rejecting it). The
unread body is drained after a half-close so the refusal is not lost to a TCP reset. A query string is never
part of the path, so `/audio/{hash}?x=1` serves the
audio rather than 404-ing. `GET /ipfs/{cid}` serves audio by content identifier -- held items from
the store, anything else fetched from the configured Kubo node (see "The IPFS mirror") -- under the
same sniff rule and with `X-Content-Hash` and immutable caching. Every response carries
`Access-Control-Allow-Origin: *` and `OPTIONS` is answered, so a page on another origin can `fetch()`
the audio. Anything else is 404.

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

`--check` is purely a store verification: it does not serve and does not contact IPFS, so operators
and scripts can call it for a quick answer even when a mirror is configured.

`--dir PATH` makes the store content-addressed: `<PATH>/<hash>` is the audio, `<PATH>/<hash>.json`
is the declared media type, and `<PATH>/<hash>.cid` is the IPFS CID once the item has been
mirrored. An accepted item is written before it is answerable -- a write failure
is a refusal, not a silent loss. The write is asynchronous (`tokio::fs`), so a slow disk delays the
answer instead of stalling every carrier's executor thread. The eviction unlink is asynchronous
too, and is taken off the lock: a slow disk must not stall every other client's accept.

On startup every entry is re-checked: the name must be the BLAKE3 of the bytes and the bytes must
still sniff as audio, so a hand-placed file cannot smuggle anything in. The load is reported as
`loaded` (entries now held), `skipped` (not read, or rejected), `evicted` (read and accepted, then
pushed out by the byte bound while later entries loaded), and `truncated` (the directory held more
than the `MAX_LOAD_ENTRIES` the load will consider) -- never silent, and never counting an evicted
entry as held. Only regular files are read -- a FIFO or device named like a hash would block
forever, and a directory would error. A sidecar larger than 4 KiB is not a media type and is ignored
rather than read into memory; a `.cid` sidecar must be a plausible identifier within 128 bytes.
The same bound applies on the way out, at `AudioItem::new`: a declared media type larger than
4 KiB is not kept. Without that, a publisher could send a declared type almost as large as a
frame -- megabytes -- which would be written to disk and then ignored by every later load, so
`--max-bytes` would bound the audio but not the bytes actually stored. Equal bounds on the way
out and the way in are what make the store bound a bound on disk. The audio is never refused
because of its metadata.
Neither sidecar counts toward the entry cap, and eviction removes both with the audio.

Eviction unlinks the file, so the directory and the store agree on what is held and the
`--max-bytes` bound is a bound on disk too, not just memory. Unlinking stays off the lock and
awaited inside `accept`, so by the time a publish is answered the file it pushed out is already
gone; a load drains it inline, since startup has no lock to protect. If a file is removed out of band, a
republish of the same bytes restores it, so an item cannot silently become one that the next start
will lose; the existence check costs a syscall only on a duplicate publish, never on the read path. Without `--dir` the store is in-memory.

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

## The Reticulum carrier

`--reticulum ADDR` (with `--features reticulum`) serves the same frame protocol over a Reticulum
TCP interface, so a Reticulum client reaches the same audio. `--reticulum-seed SEED` sets the
identity seed; it must be stable across restarts because it derives the destination's address.

No radio is involved: the carrier speaks Reticulum over its own `TcpServer` interface, exactly as
the imagination layer's loopback test does. What that proves is the audio surface end to end -- a
client publishes, fetches, verifies, and is refused -- and what it does not prove is interop with
another Reticulum implementation. That limitation is recorded here rather than discovered later.

The carrier is **opt-in** because the `reticulum` crate brings a gRPC/protobuf build and a crypto
stack -- about 160 transitive packages, against the three dependencies the rest of the daemon uses.
The default build and the default gate do not compile it, and the audio rules are untouched: an
incoming frame goes through the same `crate::server::handle`, so `AudioStore::accept` is the
inbound rule here too.

Bounds: at most 64 concurrent links, the same session cap as every other carrier, so a peer
cannot make the daemon hold an unbounded number of buffers; one reassembly buffer per link,
capped at a frame plus one packet, so a peer that dribbles without ever completing a frame is
dropped rather than accumulated; a frame larger than `MAX_FRAME_BYTES` is refused outright; and
the reply is chunked to a packet's capacity and each send is bounded (10 s), so a stalled link
abandons the rest of its frame. Each link is served by its own worker off a bounded queue, so
a slow accept -- or a hung IPFS mirror -- delays only that link and never the event loop. Every complete frame in
a payload is answered, in order -- a packet may carry several, exactly as a client may pipeline
them on a stream carrier.

## Saying what it hears

The default bind is `0.0.0.0`, which serves every interface. When a carrier binds a non-loopback
address the daemon says so, rather than letting an operator discover it later.

## Running it

`scripts/oag-listend.service` runs all four carriers with a persistent store under systemd. The
daemon handles SIGINT and SIGTERM, so `systemctl stop` is a clean stop: accepting stops at once, then
in-flight exchanges are given up to `SHUTDOWN_GRACE` (2 s) to finish rather than being cut off
mid-transfer, the Unix socket is removed, and the process exits 0. A client that never reads cannot
hold a stop open past that bound.

The daemon's own logging must not be able to kill it. Every line goes through a writer that ignores
a failed write, because under systemd stdout *is* the journal: an unavailable journal, a full disk,
or a pipe whose reader exited would otherwise panic the process and stop the audio. `oag-listend
--listen 127.0.0.1:7100 > /dev/full` keeps serving; a test asserts exactly that.

**The unit has been run, not just linted.** With a user-level systemd manager and a derivative unit
(same `ExecStart` flags, `RuntimeDirectory`, `StateDirectory`, `ReadWritePaths` and all four sandbox
directives), `systemctl start` reaches `active/running` and serves audio, systemd creates both
directories, and `systemctl stop` leaves the unit inactive with the Unix socket removed and
`oag-listend shutting down` in the journal.

**One gotcha found while doing that.** `PrivateTmp=true` masks **`/tmp` and `/var/tmp`** for the
service. The unit installs the binary to `/usr/local/bin`, which is unaffected -- but an operator who
repoints `ExecStart` at a build in either directory will get `Unable to locate executable ...: No such
file or directory`, which reads like a missing binary rather than a masked path.

## The IPFS mirror

`--ipfs-api ADDR` also offers every accepted item to a Kubo node, so the audio reaches a network
whose clients never have to connect to this daemon. It is a **mirror**, not a carrier and not a new
operation: the frame protocol permits only publish / fetch-by-hash / list / hello (+ ping), so IPFS
cannot add a way to ask for something. The BLAKE3 content hash remains the identity; the CID is an
additional address for the same bytes.

Every carrier reaches it through one function, `accept_and_mirror`, because mirroring per carrier
would fork the behaviour. It is best-effort: the bytes are written locally first, and a mirror
failure is **recorded, never propagated**. Losing a copy must not lose the audio.

The accept and the network call are deliberately separate: `accept` holds the store lock for the
rule, the durable write, and the insert, and the add happens **after** that lock is released. A Kubo
node that hangs therefore stalls only its own publish, never another client's fetch. The last 64 outcomes are retained and printed at shutdown, with each
CID (`ipfs mirror: N mirrored, M failed`, then one line per outcome).

On startup with `--ipfs-api`, everything already held is offered too -- a restart would otherwise
mirror only audio published afterwards, and an existing library would never reach IPFS. That pass is
bounded as a whole (`STARTUP_MIRROR_BUDGET`, 30 s) so a large store cannot hold startup open, while
each item keeps its own per-call bound.

A client that fetches an item is told where else it lives: the store remembers the CID per content
hash, `Response::Audio` carries it as an **optional trailing field**, and HTTP exposes it as
`X-IPFS-CID`. The field is appended after the bytes, so a decoder that stops after the declared length
ignores it and a reply from a daemon that predates it still decodes -- no protocol version change,
because nothing that already worked stops working. Eviction drops the remembered CID.

`GET /ipfs/{cid}` is the read half. A CID for audio this daemon already holds is served straight
from the store -- those bytes are verifiable here and need no network -- and only an unfamiliar CID
is sent to `/api/v0/cat`. Whatever comes back then faces the same sniff rule as every other inbound
path. What comes back is served only if it is audio; anything else is refused
without being echoed or stored, an implausible identifier is refused before any request is sent, and
an unreachable node answers `502`. Responses carry `Cache-Control: immutable`, an `ETag` of the CID,
and `X-Content-Hash` with the BLAKE3 of the bytes actually sent, so the receiver can verify them. The
bytes are identified by the CID the caller asked for -- this crate deliberately carries no multihash
implementation, so it cannot check that they hash to it.

The write client hand-rolls `POST /api/v0/add` as multipart form data and reads `Hash` from the
reply, with a 10 s bound per read and write and a 64 KiB cap on the reply. The CID it reads back is
run through the same plausibility rule as a request-supplied one: the mirror is cleartext HTTP, so
that reply comes from the network, and a `Hash` that is not a CID would otherwise be stored,
persisted, and written into `X-IPFS-CID` and `ETag` on every later fetch -- a header-injection path.
Refusing it records a mirror failure, which costs nothing: the audio is already durable locally. A
`https://` address is refused: speaking plain HTTP to a TLS port would look configured while sending
audio in clear. Chunked replies are decoded by both halves, because Kubo answers `POST /api/v0/add`
with `Transfer-Encoding: chunked` and refusing it made the whole write half fail. **Validated against
a real Kubo node** (0.43.1): a published item is added and pinned, `POST /api/v0/cat` returns exactly
the bytes, `GET /ipfs/{cid}` serves them byte-identically with `X-IPFS-CID` and an ETag of the content
hash, a non-audio object fetched by CID is refused, a CID the daemon does **not** hold is fetched
from the node and served byte-identically, and a restart reports `offered 0 of N` with the node's
object count unchanged.

## Next

Of the imagination `Transport` seam, every carrier is now implemented in oag-listend: TCP, UDP,
HTTP, IPFS, the HTTP CONNECT tunnel (a client-side wrapper, so nothing to build), and Reticulum
(opt-in). The only remaining work is the one that needs infrastructure this repository does not
have:

- **Test both halves of the IPFS mirror against a real Kubo node** (`/api/v0/add` and
  `/api/v0/cat`). Add chunked-reply decoding only if it proves necessary.
- **Test the Reticulum carrier against a real Reticulum transport instance.** It is exercised over
  its own `TcpServer` interface on loopback, which proves the audio surface but not interop with
  another Reticulum implementation.
