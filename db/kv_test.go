package db_test

import (
	"testing"

	"github.com/wujie1993/gopkg/db"
)

func init() {
	db.InitKV()
}

func TestEtcd(t *testing.T) {
	key := "/prophet/host/1"
	value := "{\"name\":\"host1\",\"address\":\"192.168.1.1\"}"

	// 写入
	if err := db.KV.Set(key, value); err != nil {
		t.Fatal(err)
	}

	// 读取
	respValue, err := db.KV.Get(key)
	if err != nil {
		t.Fatal(err)
	} else if respValue != value {
		t.Fatal("get result incorrect")
	}

	// 列举
	if result, err := db.KV.List("/prophet/"); err != nil {
		t.Fatal(err)
	} else if result[key] != value {
		t.Fatal("list result incorrect")
	} else {
		t.Logf("list: %v+", result)
	}

	// 删除
	if result, err := db.KV.Delete(key); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("delete %s", result)
	}

	if respValue, err = db.KV.Get(key); err != nil {
		t.Fatal(err)
	} else if respValue != "" {
		t.Fatal("delete unsuccessful")
	}
}
