package main

import (
	"context"
	"encoding/json"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/plugins/jsvm"
	"github.com/pocketbase/pocketbase/plugins/migratecmd"
)

const writeTimeout = 5 * time.Second

// buffer holds one record field in memory while anyone is connected to it.
type buffer struct {
	mu    sync.Mutex
	key   [3]string // collection, record id, field
	state map[string]json.RawMessage
	owner map[string]*websocket.Conn
	conns map[*websocket.Conn]bool
	size  int // approximate bytes of state, checked against the field's max size
	limit int
	dirty bool
}

var (
	app     = pocketbase.New()
	flush   time.Duration
	ping    time.Duration
	rate    int
	max     int
	mu      sync.Mutex
	buffers = map[[3]string]*buffer{}
)

// main wires up stock PocketBase, the /ws route, the flush ticker and four flags.
func main() {
	var publicDir string
	app.RootCmd.PersistentFlags().DurationVar(&flush, "flush", 30*time.Second, "how often buffered state is written to the db, 0 to only write on disconnect")
	app.RootCmd.PersistentFlags().DurationVar(&ping, "ping", 30*time.Second, "how often websocket connections are pinged, 0 to disable")
	app.RootCmd.PersistentFlags().IntVar(&rate, "rate", 60, "max messages per second per connection, 0 to disable")
	app.RootCmd.PersistentFlags().IntVar(&max, "max", 100, "max connections per record field, 0 for unlimited")
	app.RootCmd.PersistentFlags().StringVar(&publicDir, "publicDir", filepath.Join(app.DataDir(), "../pb_public"), "the directory to serve static files")
	jsvm.MustRegister(app, jsvm.Config{HooksWatch: true, HooksPoolSize: 15})
	migratecmd.MustRegister(app, app.RootCmd, migratecmd.Config{TemplateLang: migratecmd.TemplateLangJS, Automigrate: true})

	app.OnServe().BindFunc(func(se *core.ServeEvent) error {
		se.Router.GET("/ws/{collection}/{id}/{field}", serve)
		se.Router.GET("/{path...}", apis.Static(os.DirFS(publicDir), true))
		go func() {
			for range time.Tick(flush) {
				flushAll()
			}
		}()
		return se.Next()
	})
	app.OnTerminate().BindFunc(func(e *core.TerminateEvent) error {
		flushAll()
		return e.Next()
	})
	if err := app.Start(); err != nil {
		log.Fatal(err)
	}
}

// serve is the websocket handler. Rules are checked once, at connect.
func serve(e *core.RequestEvent) error {
	token := e.Request.URL.Query().Get("token")
	e.Request.URL.RawQuery = "" // keep the token out of the request log
	if e.Auth == nil && token != "" {
		e.Auth, _ = app.FindAuthRecordByToken(token, core.TokenTypeAuth)
	}
	rec, err := app.FindRecordById(e.Request.PathValue("collection"), e.Request.PathValue("id"))
	if err != nil {
		return e.NotFoundError("", err)
	}
	field := e.Request.PathValue("field")
	if _, ok := rec.Collection().Fields.GetByName(field).(*core.JSONField); !ok {
		return e.NotFoundError("", nil)
	}
	info, _ := e.RequestInfo() // cannot fail on a bodiless GET
	if ok, _ := app.CanAccessRecord(rec, info, rec.Collection().ViewRule); !ok {
		return e.ForbiddenError("", nil)
	}
	canWrite, _ := app.CanAccessRecord(rec, info, rec.Collection().UpdateRule)
	key := [3]string{rec.Collection().Name, rec.Id, field}
	if e.Request.Header.Get("Upgrade") == "" {
		return e.JSON(200, map[string]int{"connections": occupancy(key), "max": max})
	}

	conn, err := websocket.Accept(e.Response, e.Request, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
	if err != nil {
		return err
	}
	defer conn.CloseNow()

	ctx := e.Request.Context() // cancelled when this handler returns
	go keepalive(ctx, conn)

	b := join(rec, key, conn)
	if b == nil {
		conn.Close(websocket.StatusTryAgainLater, "full")
		return nil
	}
	defer leave(b, conn)

	n, window := 0, time.Now()
	for {
		_, msg, err := conn.Read(ctx)
		if err != nil {
			return nil
		}
		if now := time.Now(); now.Sub(window) >= time.Second {
			window, n = now, 0
		}
		if n++; rate > 0 && n > rate {
			continue
		}
		var patch map[string]json.RawMessage
		if !canWrite || json.Unmarshal(msg, &patch) != nil {
			continue
		}
		b.mu.Lock()
		grow := 0
		for k, v := range patch {
			grow += weight(k, v) - weight(k, b.state[k])
		}
		if b.size+grow <= b.limit {
			for k, v := range patch {
				if string(v) == "null" {
					delete(b.state, k)
					delete(b.owner, k)
				} else {
					b.state[k] = v
					b.owner[k] = conn
				}
			}
			b.size += grow
			b.dirty = true
			b.broadcast(msg, conn)
		}
		b.mu.Unlock()
	}
}

// keepalive pings until the connection dies or the handler returns.
func keepalive(ctx context.Context, conn *websocket.Conn) {
	for ping > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(ping):
		}
		pctx, cancel := context.WithTimeout(ctx, writeTimeout)
		err := conn.Ping(pctx)
		cancel()
		if err != nil {
			conn.CloseNow()
			return
		}
	}
}

// weight is what a key costs in the marshalled state. A missing or null value costs nothing.
func weight(k string, v json.RawMessage) int {
	if v == nil || string(v) == "null" {
		return 0
	}
	return len(k) + len(v) + 4 // quotes, colon, comma
}

// write is Write with a deadline. A stuck client must not hold a buffer lock.
func write(conn *websocket.Conn, msg []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	conn.Write(ctx, websocket.MessageText, msg)
}

// broadcast relays msg to everyone in the buffer but except.
func (b *buffer) broadcast(msg []byte, except *websocket.Conn) {
	for c := range b.conns {
		if c != except {
			write(c, msg)
		}
	}
}

// occupancy is how many connections currently share a buffer.
func occupancy(key [3]string) int {
	mu.Lock()
	defer mu.Unlock()
	b := buffers[key]
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.conns)
}

// join finds or loads the buffer and sends the caller a snapshot. Returns nil if the buffer is full.
func join(rec *core.Record, key [3]string, conn *websocket.Conn) *buffer {
	mu.Lock()
	defer mu.Unlock()
	b := buffers[key]
	if b == nil {
		b = &buffer{key: key, state: map[string]json.RawMessage{}, owner: map[string]*websocket.Conn{}, conns: map[*websocket.Conn]bool{}}
		json.Unmarshal([]byte(rec.GetString(key[2])), &b.state)
		b.size = len(rec.GetString(key[2]))
		b.limit = int(rec.Collection().Fields.GetByName(key[2]).(*core.JSONField).CalculateMaxBodySize())
		buffers[key] = b
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if max > 0 && len(b.conns) >= max {
		return nil
	}
	b.conns[conn] = true
	snap, _ := json.Marshal(b.state)
	write(conn, snap)
	return b
}

// leave drops the connection's keys and, if it was the last one, saves and forgets the buffer.
func leave(b *buffer, conn *websocket.Conn) {
	mu.Lock()
	defer mu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.conns, conn)
	gone := map[string]any{}
	for k, c := range b.owner {
		if c == conn {
			gone[k] = nil
			b.size -= weight(k, b.state[k])
			delete(b.state, k)
			delete(b.owner, k)
		}
	}
	if len(gone) > 0 {
		b.dirty = true
		msg, _ := json.Marshal(gone)
		b.broadcast(msg, nil)
	}
	if len(b.conns) == 0 {
		b.save()
		delete(buffers, b.key)
	}
}

// flushAll saves dirty buffers. Grab the list under mu, then save one at a time.
func flushAll() {
	mu.Lock()
	list := slices.Collect(maps.Values(buffers))
	mu.Unlock()
	for _, b := range list {
		b.mu.Lock()
		b.save()
		b.mu.Unlock()
	}
}

// save writes state back to the record if it changed.
func (b *buffer) save() {
	if !b.dirty {
		return
	}
	rec, err := app.FindRecordById(b.key[0], b.key[1])
	if err != nil {
		return
	}
	rec.Set(b.key[2], b.state)
	if err := app.Save(rec); err != nil {
		app.Logger().Error("PocketSocket: save failed", "key", b.key, "error", err)
		return
	}
	b.dirty = false
}
