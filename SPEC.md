# oag-listend

**License: GNU Affero General Public License v3.0 or later (AGPL-3.0-or-later).**

**Status: founded, not yet implemented.** This file records the idea and the shape it implies.
No audio is served yet. The status line will change when a carrier serves its first byte.

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

## The inbound rule

The daemon listens on every carrier, and the only thing it accepts is audio. Anything else that
arrives is rejected before it is stored -- not parsed, not forwarded, not logged as content.

This is enforced at the audio surface, not per carrier, so a new carrier cannot forget it: every
inbound path goes through `AudioStore::accept`, which admits bytes only if they carry a
recognised audio signature. The check is on the bytes, never on a declared content type, because a
declared type is a claim and the bytes are the evidence. A carrier may *additionally* reject
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

## First slice

The smallest honest instance: one carrier (TCP, borrowed from the imagination layer's seam), one
audio item, served end to end with its content hash verified by the receiver. Everything after
that is another carrier.
