# PocketSocket demo

Every visitor is a circle. Arrow keys move it, and everyone in the same room sees it.

## Run

```sh
./pocketsocket serve
```

Open http://127.0.0.1:8090 in two tabs. Try `--max 2` and a third tab to watch it land in a new room.

## Files

- `pb_migrations` creates a public `rooms` collection with a `state` json field.
- `pb_hooks/join.pb.js` adds `POST /join`. It reads room occupancy from `$app.store()` and returns the oldest room with a free seat, or creates one. A game that needs sign-in adds one line: `if (!e.auth) return e.unauthorizedError();`.
- `pb_public/index.html` is the whole client. Each tab gets a random circle in `sessionStorage`, moves at 60 fps, sends position and velocity at 10 Hz while moving, and dead-reckons the others. It calls `/join` before connecting and again if the socket closes with code `1013`.

## Deploy

```sh
fly launch
```

Once, from this folder. `fly deploy` after that. The Dockerfile pins a PocketSocket release; bump `VERSION` to move. Fly creates the volume from `fly.toml` and terminates TLS. Make the first admin with `fly ssh console -C "/pb/pocketsocket superuser upsert you@example.com password"`.
