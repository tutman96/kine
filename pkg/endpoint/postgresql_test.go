package endpoint

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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

func TestCallerCancellationReachesDriverStartup(t *testing.T) {
	for _, alreadyCanceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "during startup", true: "before startup"}[alreadyCanceled], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{})
			drivers.Register("test-cancel-startup", func(driverCtx context.Context, _ *sync.WaitGroup, _ *drivers.Config) (bool, server.Backend, error) {
				close(entered)
				<-driverCtx.Done()
				return false, nil, driverCtx.Err()
			})
			if alreadyCanceled {
				cancel()
			}
			result := make(chan error, 1)
			go func() {
				_, err := Listen(ctx, Config{Endpoint: "test-cancel-startup://unused"})
				result <- err
			}()
			timeout := time.NewTimer(3 * time.Second)
			defer timeout.Stop()
			select {
			case <-entered:
			case <-timeout.C:
				t.Fatal("driver startup did not begin")
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("startup cancellation was not preserved: %v", err)
				}
			case <-timeout.C:
				t.Fatal("caller cancellation did not reach driver startup")
			}
		})
	}
}
