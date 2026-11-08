package endpoint

import (
	"context"
	"sync"
	"testing"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
)

func TestPostgreSQLConfigPropagation(t *testing.T) {
	want := &drivers.PostgreSQLConfig{SkipCreateDatabase: true}
	called := false
	drivers.Register("test-postgresql-options", func(_ context.Context, _ *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
		called = true
		if cfg.PostgreSQL != want {
			t.Fatal("endpoint lost PostgreSQL options")
		}
		return false, nil, nil
	})
	_, err := Listen(context.Background(), Config{Endpoint: "test-postgresql-options://unused", PostgreSQL: want})
	if err != nil || !called {
		t.Fatalf("called=%v, err=%v", called, err)
	}
}
