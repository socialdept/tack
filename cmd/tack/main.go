// Tack follows atproto labelers and delivers the labels an app cares about.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/socialdept/tack/internal/labelers"
	"github.com/socialdept/tack/internal/outbox"
	"github.com/socialdept/tack/internal/server"
	"github.com/socialdept/tack/internal/store"
)

// defaultLabelers are followed on first start, unless TACK_LABELERS says
// otherwise: Bluesky Moderation and Blacksky.
var defaultLabelers = []string{
	"did:plc:ar7c4by46qjdydhdevvrndac",
	"did:plc:d2mkddsbmnrgr3domzg5qexf",
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel(env("TACK_LOG_LEVEL", "info"))}))

	if err := run(log); err != nil {
		log.Error("tack stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbURL := env("TACK_DATABASE_URL", "sqlite://./data/tack.db")
	if path, ok := strings.CutPrefix(dbURL, "sqlite://"); ok {
		if dir := dirOf(path); dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return err
			}
		}
	}

	s, err := store.Open(dbURL)
	if err != nil {
		return err
	}

	if err := seedLabelers(s, log); err != nil {
		return err
	}

	identity.DefaultPLCURL = env("TACK_PLC_URL", identity.DefaultPLCURL)

	box := &outbox.Outbox{
		Store:        s,
		Log:          log.With("part", "outbox"),
		Mode:         outbox.ModeFor(os.Getenv("TACK_WEBHOOK_URL"), envBool("TACK_DISABLE_ACKS", false)),
		URL:          os.Getenv("TACK_WEBHOOK_URL"),
		Password:     os.Getenv("TACK_ADMIN_PASSWORD"),
		BatchSize:    envInt("TACK_WEBHOOK_BATCH_SIZE", 0),
		Flush:        envDuration("TACK_WEBHOOK_BATCH_FLUSH", 500*time.Millisecond),
		Parallelism:  envInt("TACK_WEBHOOK_BATCH_PARALLELISM", 1),
		RetryTimeout: envDuration("TACK_RETRY_TIMEOUT", time.Minute),
	}

	manager := &labelers.Manager{
		Store:       s,
		Directory:   identity.DefaultDirectory(),
		Verify:      envBool("TACK_VERIFY", true),
		AllSubjects: envBool("TACK_ALL_SUBJECTS", false),
		Log:         log.With("part", "labelers"),
		Notify:      box.Wake,
		QueryRate:   envFloat("TACK_QUERY_RATE", 4),
	}

	if err := manager.Start(ctx); err != nil {
		return err
	}

	go box.Run(ctx)
	log.Info("delivering", "mode", box.Mode)

	go manager.Backfill(ctx, envInt("TACK_BACKFILL_WORKERS", 4))
	go sweep(ctx, s, log)

	api := &server.Server{Context: ctx, Store: s, Labelers: manager, Password: os.Getenv("TACK_ADMIN_PASSWORD")}
	if box.Mode != outbox.ModeWebhook {
		api.Channel = &outbox.Channel{Outbox: box}
	}
	if api.Password == "" {
		log.Warn("TACK_ADMIN_PASSWORD is not set: the admin API is open to anyone who can reach it")
	}

	httpServer := &http.Server{Addr: env("TACK_BIND", ":2490"), Handler: api.Handler(), ReadHeaderTimeout: 10 * time.Second}

	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdown)
	}()

	log.Info("tack listening", "addr", httpServer.Addr, "database", redact(dbURL))

	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

// seedLabelers follows TACK_LABELERS, or the defaults, the first time Tack
// starts. After that the API decides, so a removed default stays removed.
func seedLabelers(s *store.Store, log *slog.Logger) error {
	existing, err := s.Labelers()
	if err != nil || len(existing) > 0 {
		return err
	}

	dids := defaultLabelers
	if configured := os.Getenv("TACK_LABELERS"); configured != "" {
		dids = nil
		for _, did := range strings.Split(configured, ",") {
			if did = strings.TrimSpace(did); did != "" {
				dids = append(dids, did)
			}
		}
	}

	for _, did := range dids {
		if _, err := s.AddLabeler(did); err != nil {
			return err
		}
	}

	log.Info("following labelers", "count", len(dids))

	return nil
}

// sweep drops expired labels every hour.
func sweep(ctx context.Context, s *store.Store, log *slog.Logger) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n, err := s.SweepExpired(); err != nil {
				log.Warn("sweeping expired labels failed", "err", err)
			} else if n > 0 {
				log.Info("swept expired labels", "count", n)
			}
		}
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}

func envInt(key string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}

	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return v
	}

	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil {
		return v
	}

	return fallback
}

func envBool(key string, fallback bool) bool {
	if v, err := strconv.ParseBool(os.Getenv(key)); err == nil {
		return v
	}

	return fallback
}

func logLevel(name string) slog.Level {
	var level slog.Level
	if err := level.UnmarshalText([]byte(name)); err != nil {
		return slog.LevelInfo
	}

	return level
}

func dirOf(path string) string {
	if i := strings.LastIndex(path, "/"); i > 0 {
		return path[:i]
	}

	return ""
}

// redact hides a password in a database URL for the log.
func redact(url string) string {
	at := strings.LastIndex(url, "@")
	scheme := strings.Index(url, "://")

	if at < 0 || scheme < 0 {
		return url
	}

	return url[:scheme+3] + "***" + url[at:]
}
