# PocketSocket

[PocketBase](https://pocketbase.io) plus one WebSocket route. Clients share a JSON field of a record in memory and PocketBase writes it on an interval.

But, why?! I love PocketBase and think it actually makes a pretty good game backend, except for the realtime part. Its realtime is SSE (one-way) and every player update would be a DB write. So PocketSocket keeps the hot state in memory, relays it over WebSockets, and lets PocketBase do everything else.

## Design

- Everything is stock PocketBase: auth, collections, API rules, admin UI, SDKs, `pb_hooks`, `pb_migrations`, `pb_public`. `main.go` is a [PocketBase Go app](https://pocketbase.io/docs/go-overview/), so you would extend it the same way.
- Added one route, four flags, one dependency. No schema, no game logic, no protocol beyond JSON.
- Removed the `update` command, which would replace the binary with stock PocketBase. `hooksDir`, `hooksWatch`, `hooksPool`, `migrationsDir`, `automigrate`, and `indexFallback` are stock defaults. Add any back with one line from [examples/base/main.go](https://github.com/pocketbase/pocketbase/blob/master/examples/base/main.go).
- State lives in memory while anyone is connected. Every write to the record is a normal save, so [realtime subscriptions](https://pocketbase.io/docs/api-realtime/) and hooks fire at flush cadence.
- Single process, same as PocketBase.

## Run

```sh
go build -o pocketsocket .
./pocketsocket serve
```

`make` does the same with the version stamped from the latest git tag, `make VERSION=1.2.3` picks one, and `make dist` builds the release zips.

Or download a binary from [releases](../../releases). [PocketBase commands and flags](https://pocketbase.io/docs/going-to-production/) apply. `pb_public`, `pb_hooks`, and `pb_migrations` are siblings of `pb_data`, so they follow `--dir`.

- `--flush` how often state is written, default `30s`. `0` writes only when the last client disconnects and on shutdown.
- `--ping` how often connections are pinged, default `30s`. `0` disables. An unanswered ping closes the connection after 5s, so keep this below your proxy's idle timeout.
- `--rate` max messages per second per connection, default `60`. `0` disables. Excess messages are dropped. Connection attempts are ordinary requests, so a [rate limit rule](https://pocketbase.io/docs/going-to-production/#rate-limits) with label `/ws/` caps them per IP.
- `--max` max connections per record field, default `100`, `0` for unlimited. A connection past the cap is closed with code `1013`. Every message fans out to everyone else, so at 10 Hz a room of 50 sends each client about 500 messages a second.

## Use

Add a `json` field to any collection ([docs](https://pocketbase.io/docs/collections/)). Connect to:

```
ws://host/ws/{collection}/{recordId}/{field}?token={authToken}
```

- The collection's `viewRule` decides who can connect and `updateRule` who can send. `token` is the SDK's `pb.authStore.token`, optional for public rules, and stripped from the request log.
- On connect the server sends the field's current value as one JSON object.
- A plain GET on the same URL, no upgrade, returns `{"connections": n, "max": N}`. Hooks can read the same numbers without a request from `$app.store()`, under `pocketsocket:{collection}/{id}/{field}` and `pocketsocket:max`.
- Send any JSON object. Its top level keys are merged into the shared object and the message is relayed as is to every other client. A `null` value deletes the key. Anything else is dropped.
- A key belongs to the connection that last wrote it. When that connection closes the key is deleted and `null` is relayed for it. Keys loaded from the record have no owner and stay.
- While a buffer is live its field belongs to the socket. REST writes to that field are overwritten at the next flush. Other fields are untouched.
- Messages are limited to 32 KiB. State is capped at the field's max size, 1 MiB by default ([docs](https://pocketbase.io/docs/collections/#json)). A patch that would exceed it is dropped.

```js
const ws = new WebSocket(`${pb.baseURL.replace('http', 'ws')}/ws/games/${id}/state?token=${pb.authStore.token}`);
let state = {};
ws.onmessage = (e) => Object.assign(state, JSON.parse(e.data));
ws.send(JSON.stringify({ [pb.authStore.record.id]: { x, y } }));
```

## Demo

A browser client, a `rooms` collection, a `/join` matchmaking hook, and a Fly deploy, in [demo](demo).

Run it with `./pocketsocket serve --dir demo/pb_data` and open http://127.0.0.1:8090 in two tabs.

## License

[MIT](LICENSE). PocketBase is also MIT.
