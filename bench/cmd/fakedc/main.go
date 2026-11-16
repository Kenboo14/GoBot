// Command fakedc runs a local fake Telegram DC for load testing.
//
//	go run ./bench/cmd/fakedc -info bench/out/dc.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"

	"github.com/Kenboo14/GoBot/internal/fakedc"
)

func main() {
	var (
		addr     = flag.String("addr", "127.0.0.1:0", "MTProto listen address")
		ctrl     = flag.String("control", "127.0.0.1:0", "HTTP control listen address")
		infoPath = flag.String("info", filepath.Join("bench", "out", "dc.json"), "where to write DC info")
		contacts = flag.Int("contacts", 50, "users returned by contacts.getContacts")
		dialogs  = flag.Int("dialogs", 100, "peers returned by messages.getDialogs")
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	srv, err := fakedc.New(fakedc.Options{Contacts: *contacts, Dialogs: *dialogs})
	if err != nil {
		log.Fatal(err)
	}

	cl, err := net.Listen("tcp", *ctrl)
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/load", func(w http.ResponseWriter, r *http.Request) {
		rate, err := strconv.ParseFloat(r.URL.Query().Get("rate"), 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		srv.SetRate(rate)
		_ = json.NewEncoder(w).Encode(srv.Stats())
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(srv.Stats())
	})
	mux.HandleFunc("/reset", func(w http.ResponseWriter, _ *http.Request) {
		srv.Reset()
		_ = json.NewEncoder(w).Encode(srv.Stats())
	})
	go func() { _ = http.Serve(cl, mux) }()

	err = srv.Serve(ctx, *addr, func(info fakedc.Info) {
		info.Control = cl.Addr().String()
		if err := os.MkdirAll(filepath.Dir(*infoPath), 0o755); err != nil {
			log.Fatal(err)
		}
		if err := info.WriteFile(*infoPath); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("fakedc ready: mtproto=%s:%d control=%s info=%s\n", info.Addr, info.Port, info.Control, *infoPath)
	})
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
