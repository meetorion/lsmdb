package lsmdb

import (
	"go.uber.org/zap"
	"testing"
)

func Test_lsmDB_PUT(t *testing.T) {
	db, err := Open("./test/lsm")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	err = db.PUT([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Error(err)
	}
	zap.S().Infof("%#v\n", db)
}

func Test_lsmDB_DEL(t *testing.T) {
	db, err := Open("./test/lsm")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	err = db.DEL([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Error(err)
	}

	value,ok, err := db.GET([]byte("key1"))
	if err != nil {
		t.Error(err)
	}
	zap.S().Info(value, ok)
}

func Test_lsmDB_GET(t *testing.T) {
	db, err := Open("./test/lsm")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	value, ok, err := db.GET([]byte("key1"))
	if err != nil {
		t.Error(err)
	}
	zap.S().Info(value, ok)
}

func Test_lsmDB_Merge(t *testing.T) {
	db, err := Open("./test/lsm")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	err = db.Merge()
	if err != nil {
		t.Error(err)
	}
}