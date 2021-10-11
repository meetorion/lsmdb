package index

import (
	"go.uber.org/zap"
	"testing"
)

func TestSkipList_Add(t *testing.T) {
	skl := NewSkipList(5)
	err := skl.Add([]byte("0"), []byte("小明"))
	if err != nil {
		t.Error(err)
	}
	err = skl.Add([]byte("5"), []byte("小花"))
	if err != nil {
		t.Error(err)
	}
	err= skl.Add([]byte("4"), []byte("小军"))
	if err != nil {
		t.Error(err)
	}

	skl.OutPut()
	zap.S().Infof("%#v", skl)
}