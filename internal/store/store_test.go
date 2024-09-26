package store

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func TestInsertGetListPrune(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "t.db"), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	var ids []int64
	for i := 0; i < 5; i++ {
		r := &Request{Channel: "a", ReceivedAt: time.UnixMilli(int64(1000 + i)), Method: "POST", Path: "/x",
			Headers: http.Header{"X-N": {string(rune('0' + i))}}, Body: []byte{byte(i), 0xff}}
		if err := st.Insert(ctx, r); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.ID)
	}
	st.Insert(ctx, &Request{Channel: "b", ReceivedAt: time.UnixMilli(5000), Method: "GET", Path: "/"})

	list, err := st.List(ctx, "a", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 || list[0].ID != ids[4] || list[2].ID != ids[2] {
		t.Fatalf("prune/order: %d items, first %d", len(list), list[0].ID)
	}
	if _, err := st.Get(ctx, ids[0]); !errors.Is(err, ErrNotFound) {
		t.Fatal("old request should be pruned")
	}
	if err := st.SetResponse(ctx, ids[4], 201, http.Header{"A": {"b"}}, []byte("ok"), "", 42*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Get(ctx, ids[4])
	if got.RespStatus != 201 || got.RespHeaders.Get("A") != "b" || string(got.RespBody) != "ok" ||
		got.Duration != 42*time.Millisecond || got.Headers.Get("X-N") != "4" || got.Body[1] != 0xff {
		t.Fatalf("round trip: %+v", got)
	}
	chs, _ := st.Channels(ctx)
	if len(chs) != 2 || chs[0].Name != "b" || chs[1].Count != 3 {
		t.Fatalf("channels %+v", chs)
	}
}
