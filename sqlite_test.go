package schema

import (
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSQLiteLocking(t *testing.T) {
	testfile := filepath.Join(os.TempDir(), "sqlite_test.db")
	os.Remove(testfile)
	defer os.Remove(testfile)

	db, err := sql.Open("sqlite3", testfile)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var inflight int32

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			s := &sqliteDialect{}
			if err := s.Lock(db); err != nil {
				t.Error(err)
			}
			atomic.AddInt32(&inflight, 1)
			if !atomic.CompareAndSwapInt32(&inflight, 1, 1) {
				t.Error("expected 1 concurrent sqlite migration")
			}

			time.Sleep(500 * time.Millisecond)

			atomic.AddInt32(&inflight, -1)
			if err := s.Unlock(db); err != nil {
				t.Error(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
