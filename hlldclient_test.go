package hlldClient

import (
	"testing"
	"time"
)

const (
	Addr = "127.0.0.1:4553"
	key  = "hlld-test"
)

func TestHlldClient(t *testing.T) {
	t.Log("start test")
	c := NewHlldClient(Addr)

	attr := SetAttr{
		InMemory:  true,
		Eps:       0.01,
		Precision: 16,
	}
	err := c.Create(key, &attr)
	if err != nil {
		t.Log(err)
		t.Fail()
	}

	err = c.Set(key, "abc")
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	count, err := c.List(key)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	t.Log(key, count)

	err = c.Bulk(key, "ed", "op", "cff")
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	count, err = c.List(key)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	t.Log(key, count)

	err = c.Drop(key)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	// test delete in progress
	err = c.Create(key, nil)
	for i := 0; i < 3; i++ {
		if err == nil {
			break
		}
		if err == ErrKeyDeleteInProgress {
			t.Log("try:%d", i)
			time.Sleep(time.Second)
			err = c.Create(key, nil)
		}
	}
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	err = c.Drop(key)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
}
