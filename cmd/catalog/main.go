/*
Copyright 2026 Politecnico di Torino - NetGroup.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Command catalog serves the offer catalog over HTTP.
//
//	go run ./cmd/catalog                       # in-memory, :8080
//	go run ./cmd/catalog -addr :9090
//	go run ./cmd/catalog -redis localhost:6379 # persistent
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/netgroup-polito/federation-autoscaler/internal/broker/store"
	"github.com/netgroup-polito/federation-autoscaler/internal/catalog"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	freshFor := flag.Duration("fresh-for", store.DefaultFreshFor, "how long an offer counts as fresh after its last publish")
	redisAddr := flag.String("redis", "", "Redis address; empty keeps offers in memory only")
	flag.Parse()

	var offers store.OfferStore
	if *redisAddr != "" {
		offers = store.NewRedisStore(redis.NewClient(&redis.Options{Addr: *redisAddr}), "", *freshFor)
		log.Printf("catalog: storing offers in Redis at %s", *redisAddr)
	} else {
		offers = store.NewMemoryStore(*freshFor)
		log.Printf("catalog: storing offers in memory (lost on restart)")
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           catalog.NewHandler(offers),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("catalog: listening on %s", *addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("catalog: %v", err)
	}
}
