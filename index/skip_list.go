package index

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"lsmdb/initialize"
	"math/rand"
	"time"
)

const SKIPLIST_P = 0.5

type SkipList struct {
	headers []*headerNode
	length   int64
	maxLevel int
}

type headerNode struct {
	right *node
	down *headerNode
	level int
}

type node struct {
	key   []byte
	value []byte
	level  int
	right *node
	down *node
}

func init()  {
	initialize.InitLogger()
}

func NewSkipList(maxLevel int) *SkipList {
	skl := &SkipList{
		make([]*headerNode, maxLevel + 1),
		0,
		maxLevel,
	}
	for i := maxLevel; i >= 0; i-- {
		skl.headers[i] = &headerNode{}
		skl.headers[i].level = i
		skl.headers[i].right = nil
	}

	for i := maxLevel; i >= 0; i-- {
		if i - 1 >= 0 {
			skl.headers[i].down = skl.headers[i - 1]
		} else {
			skl.headers[i].down = nil
		}
	}

	return skl
}

func (s *SkipList) Add(key, value []byte) error {
	randLevel, err := s.randLevel()
	if err != nil {
		return errors.Wrap(err, "生成随机Level过程中发生错误")
	}

	befores, err := s.beforeNodeLocations(key, randLevel)
	if err != nil {
		return errors.Wrap(err, "找所有位于key在跳跃表应该插入的位置的前一个节点过程中发生错误")
	}

	s.insertTheBack(befores, key, value)
	s.length += 1

	return nil

}

func newNode(key, value []byte, level int) *node {
	ret := &node{
		key:   key,
		value: value,
		level: level,
		right: nil,
		down:  nil,
	}
	return ret
}

func (s *SkipList) beforeNodeLocations(key []byte, maxLevel int) ([]*node, error) {
	ret := make([]*node, maxLevel + 1)
	head := s.headers[maxLevel]
	rival := head.right
	pre := rival

	for level := maxLevel; level >= 0; level-- {
		if rival == nil {
			ret[level] = nil
			head = head.down
			if head != nil {
				rival = head.right
			}
			continue
		}

		for {
			if rival != nil && bytes.Compare(rival.key, key) < 0 {
				pre = rival
				rival = rival.right
			} else {
				break
			}
		}

		if pre == rival {
			ret[level] = nil
		} else {
			ret[level] = pre
		}
		pre = pre.down
		rival = pre
	}

	return ret, nil
}

func (s *SkipList) insertTheBack(leaders []*node, key []byte, value []byte) {
	maxLevel := len(leaders) - 1
	node := newNode(key, value, maxLevel)
	for i := maxLevel; i >= 0; i-- {
		leader := leaders[i]

		if leader == nil {
			node.right = s.headers[i].right
			s.headers[i].right = node
		} else {
			node.right = leader.right
			leader.right = node
		}

		if i > 0 {
			tmp := newNode(key, value,i - 1)
			node.down = tmp
			node = tmp
		}
	}
}

//该方法有 1/2 的概率返回 1, 1/4 的概率返回 2, 1/8的概率返回 3，以此类推
func (s *SkipList) randLevel() (int, error) {
	level := 1

	for {
		if r := rand.New(rand.NewSource(time.Now().Unix())); r.Float64() < SKIPLIST_P && level < s.maxLevel {
			level += 1
		} else {
			break
		}
	}

	if level < 0 || level > s.maxLevel {
		return 0, errors.Wrap(errors.New("参数错误"), "生成随机Level错误，返回了超出范围的值")
	}

	return level, nil
}

func (s *SkipList) OutPut() {
	head := s.headers[s.maxLevel]
	for i := s.maxLevel; i >= 0; i-- {
		node := head.right
		for  {
			if node == nil {
				fmt.Printf("nil")
				break
			}
			fmt.Printf("key:%s,level:%d  ", string(node.key),node.level)
			node = node.right
		}
		fmt.Println()
		if head != nil {
			head = head.down
		}

	}
}
