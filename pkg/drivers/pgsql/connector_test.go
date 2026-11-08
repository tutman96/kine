package pgsql

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/k3s-io/kine/pkg/drivers"
)

func TestSuppliedConfigIsCopied(t *testing.T) {
	base, err := pgx.ParseConfig("postgres://user@localhost/CaseSensitive?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &drivers.Config{DataSourceName: "%invalid", PostgreSQL: &drivers.PostgreSQLConfig{ConnConfig: base}}
	got, err := connectionConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got == base || got.Database != "CaseSensitive" {
		t.Fatal("supplied configuration was not copied intact")
	}
	got.RuntimeParams["application_name"] = "copy"
	got.Password = "synthetic"
	if base.Password != "" || base.RuntimeParams["application_name"] != "" {
		t.Fatal("base configuration mutated")
	}
}

func TestConnectorRedactsCallbackErrorsOnEveryAttempt(t *testing.T) {
	base, err := pgx.ParseConfig("postgres://user@localhost/tenant?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	secret := "synthetic-token-do-not-log"
	calls := 0
	connector := newConnector(base, stdlib.OptionBeforeConnect(func(_ context.Context, cfg *pgx.ConnConfig) error {
		calls++
		if cfg.Password != "" {
			t.Fatal("password retained from prior attempt")
		}
		cfg.Password = secret
		return errors.New(secret)
	}))
	for range 2 {
		_, err := connector.Connect(context.Background())
		if !errors.Is(err, errConnect) || strings.Contains(err.Error(), secret) {
			t.Fatalf("unsafe error: %v", err)
		}
	}
	if calls != 2 || base.Password != "" {
		t.Fatal("callback did not run fresh for each physical connection attempt")
	}
}

func TestConnectorOptionsEveryPath(t *testing.T) {
	for _, skip := range []bool{false, true} {
		t.Run(map[bool]string{false: "administrative", true: "provisioned"}[skip], func(t *testing.T) {
			base, err := pgx.ParseConfig("postgres://user@localhost/tenant?sslmode=disable")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			stop := errors.New("synthetic before-connect failure")
			options := []stdlib.OptionOpenDB{stdlib.OptionBeforeConnect(func(_ context.Context, cfg *pgx.ConnConfig) error {
				calls++
				want := "postgres"
				if skip {
					want = "tenant"
				}
				if cfg.Database != want {
					t.Errorf("connected to %q, want %q", cfg.Database, want)
				}
				cfg.Password = "synthetic fresh password"
				cancel()
				return stop // never dial a real server
			})}
			if skip {
				_, _, err = New(ctx, &sync.WaitGroup{}, &drivers.Config{PostgreSQL: &drivers.PostgreSQLConfig{
					ConnConfig: base, Options: options, SkipCreateDatabase: true,
				}})
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("startup error = %v", err)
				}
			} else {
				if err := createDBIfNotExist(ctx, base, options...); err != nil {
					t.Fatal(err)
				}
			}
			if calls != 1 || base.Password != "" || base.Database != "tenant" {
				t.Fatalf("callback calls=%d; base must remain unchanged", calls)
			}
		})
	}
}
