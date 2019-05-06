package db

import (
	"fmt"
	"os"
	"testing"
)

const (
	keyAdd   = 1
	keyLimit = 2
)

var dbAddrs string
var dbPWD string
var key string

func init() {
	dbAddrs = os.Args[1]
	dbPWD = os.Args[2]
	key = os.Args[3]

	fmt.Println("database address:", dbAddrs)
	fmt.Println("database password:", dbPWD)
	fmt.Println("key:", key)
}

func clearKey(t *testing.T, acc *Accessor, key string) bool {
	err := acc.DelKey(key)
	if err != nil {
		t.Errorf("acc.DelKey error: %v\n", err)
		return false
	}

	err = acc.DelKeyIdx(key)
	if err != nil {
		t.Errorf("acc.DelKeyIdx error: %v\n", err)
		return false
	}

	return true
}

func testAddToken(t *testing.T, acc *Accessor, key string,
	idx int, add int, limit int, targetIdx int) bool {
	idx, err := acc.AddToken(key, idx, add, limit)
	if err != nil {
		t.Errorf("acc.AddToken error: %v\n", err)
		return false
	}

	if idx != targetIdx {
		t.Errorf("idx should be equal to %d\n", targetIdx)
		return false
	}

	return true
}

func testKeyNum(t *testing.T, acc *Accessor, key string, keyNum int) bool {
	fetchKeyNum, err := acc.GetKeyNum(key)
	if err != nil {
		t.Errorf("acc.GetKeyNum error: %v\n", err)
		return false
	}

	if fetchKeyNum != keyNum {
		t.Errorf("fetchKeyNum should be %d, fetchKeyNum is %d\n", keyNum, fetchKeyNum)
		return false
	}

	return true
}

func testKeyIdx(t *testing.T, acc *Accessor, key string, keyIdx int) bool {
	fetchIdx, err := acc.GetKeyIdx(key)
	if err != nil {
		t.Errorf("acc.GetKeyIdx error: %v\n", err)
		return false
	}

	if fetchIdx != keyIdx {
		t.Errorf("fetchIdx should be %d, fetchIdx is %d\n", keyIdx, fetchIdx)
		return false
	}

	return true
}

func testFetchKey(t *testing.T, acc *Accessor, key string, result bool) bool {
	fetchRes, err := acc.FetchToken(key)
	if err != nil {
		t.Errorf("acc.FetchToken error: %v\n", err)
		return false
	}

	if fetchRes != result {
		t.Errorf("acc.FetchToken should be %v, result is %v", result, fetchRes)
		return false
	}

	return true
}

// Test_NewAccessor
// 注: 这个测试会先移除对应的key值
func Test_NewAccessor(t *testing.T) {
	acc, err := NewAccessor(dbAddrs, dbPWD)
	if err != nil {
		t.Errorf("NewAccessor error: %v\n", err)
		return
	}
	defer acc.Close()

	suc := clearKey(t, acc, key)
	if !suc {
		return
	}

	targetIdx := 1
	suc = testAddToken(t, acc, key, 0, keyAdd, keyLimit, targetIdx)
	if !suc {
		return
	}

	suc = testKeyNum(t, acc, key, keyAdd)
	if !suc {
		return
	}

	suc = testAddToken(t, acc, key, -1, keyAdd, keyLimit, targetIdx)
	if !suc {
		return
	}

	suc = testKeyNum(t, acc, key, keyAdd)
	if !suc {
		return
	}

	suc = testAddToken(t, acc, key, targetIdx, keyAdd, keyLimit, targetIdx+1)
	if !suc {
		return
	}
	targetIdx++

	suc = testKeyNum(t, acc, key, keyAdd*2)
	if !suc {
		return
	}

	suc = testAddToken(t, acc, key, targetIdx, keyAdd, keyLimit, targetIdx+1)
	if !suc {
		return
	}
	targetIdx++

	suc = testKeyIdx(t, acc, key, targetIdx)
	if !suc {
		return
	}

	suc = testKeyNum(t, acc, key, keyLimit)
	if !suc {
		return
	}

	suc = testFetchKey(t, acc, key, true)
	if !suc {
		return
	}

	suc = testFetchKey(t, acc, key, true)
	if !suc {
		return
	}

	suc = testFetchKey(t, acc, key, false)
	if !suc {
		return
	}
}
