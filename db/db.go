package db

import (
	"bytes"
	"errors"
	"strconv"
	"strings"

	"github.com/go-redis/redis"
)

// 说明:
// 这段代码用来实现一个简单的令牌桶法, 实现思路如下:
// (1) 使用lua脚本原子获取令牌和增加令牌
// (2) 通过idx来标识这次修改的序号, 从而确保多个增加令牌桶的调用, 在多个增加令牌的应用存在的情况下,不会出现较大的异常
//
// redis中存储的结构
// 以下描述中[:variable:]表示会改变的值, 其中[:key:]对使用的字母有所限制
// {[:key:]}			类型: redis中的string, 实现存储一个数字,表示修改的idx
// [:key:]				类型: redis中的string, 实现存储一个数字,表示当前的限流

// CheckKeyValid 用来检测一个key是否有效, 这里包括两部分
// (1) [:key:]和{[:key:]} hash到同一个槽
// (2) [:key1:]和{[:key2:]}不会相同
func CheckKeyValid(keyStr string) bool {
	// 如果keyStr中存在右括号(})则视为无效,这个条件可能稍微严格了一点,
	// 因为如果keyStr的第一个字母为右括号(}),应该也不会有问题
	if len(keyStr) == 0 || strings.Contains(keyStr, "}") {
		return false
	}

	return true
}

// getKeyIdx 用来获取修改的idx
func getKeyIdx(keyStr string) string {
	var buf bytes.Buffer
	buf.WriteByte('{')
	buf.WriteString(keyStr)
	buf.WriteByte('}')
	return buf.String()
}

// Accessor 用来表示访问的对象
type Accessor struct {
	client redis.UniversalClient
}

// NewAccessor 用来构建一个访问redis的句柄
func NewAccessor(addrs string, pwd string) (*Accessor, error) {
	addrSlice := strings.Split(addrs, " ")
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    addrSlice,
		Password: pwd,
	})

	// 尝试连接, 查看是否正常
	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &Accessor{
		client: client,
	}, nil
}

// Close 用来清理连接
func (acc *Accessor) Close() error {
	return acc.client.Close()
}

// GetKeyNum 用来获取Key的值
func (acc *Accessor) GetKeyNum(key string) (int, error) {
	// 查看Key的值
	res, err := acc.client.Get(key).Result()
	if err != nil {
		return 0, err
	}

	limitNum, err := strconv.Atoi(res)
	if err != nil {
		return 0, err
	}

	return limitNum, nil
}

// DelKey 用来移除Key的值
func (acc *Accessor) DelKey(key string) error {
	// 移除Key
	_, err := acc.client.Del(key).Result()
	return err
}

// GetKeyIdx 用来获取Key的修改索引
func (acc *Accessor) GetKeyIdx(key string) (int, error) {
	// 查看Key对应的idx的值
	res, err := acc.client.Get(getKeyIdx(key)).Result()
	if err != nil {
		return 0, err
	}

	keyIdx, err := strconv.Atoi(res)
	if err != nil {
		return 0, err
	}

	return keyIdx, nil
}

// DelKeyIdx 用来移除Key的idx
func (acc *Accessor) DelKeyIdx(key string) error {
	// 移除Key对应的idx
	_, err := acc.client.Del(getKeyIdx(key)).Result()
	return err
}

// FetchToken 用来获取某个key的一个令牌
func (acc *Accessor) FetchToken(key string) (bool, error) {
	/*
	 * KEYS[1] 表示特定的key, 这个key是当前的令牌数
	 */
	keyFetchScript :=
		`--[[测试显示, 通过call, 可以将error返回给客户端, 即使没有使用return]]--
		local curNum = redis.call("DECR", KEYS[1])
		if (curNum >= 0)
		then
			return true
		end

		redis.call("INCR", KEYS[1])
		return false
		`

	keyFetchCmd := redis.NewScript(keyFetchScript)
	res, err := keyFetchCmd.Run(acc.client, []string{key}).Result()
	if err != nil && err != redis.Nil {
		return false, err
	}

	if err == redis.Nil {
		return false, nil
	}

	if val, ok := res.(int64); ok {
		return (val == 1), nil
	}

	return false, errors.New("res should be int64")
}

// AddToken 用来添加某个key的令牌
func (acc *Accessor) AddToken(key string, keyIdx int, keyAdd int, keyLimit int) (int, error) {
	/* KEYS[1] 表示特定key,这个key是当前的令牌
	 * KEYS[2] 表示特定key的idx
	 * ARGV[1] 表示修改的key的增加的值
	 * ARGV[2] 表示修改的key的最大值
	 * ARGV[3] 表示修改的key的idx的序号
	 */
	// 实现思路, 先判断这个key当前的序号与修改调用的序号是否一致,如果一致, 则进行修改,否则返回当前的序号
	keyAddScript :=
		`--[[测试显示, 通过call, 可以将error返回给客户端, 即使没有使用return]]--
		local curIdx = redis.call("INCR", KEYS[2])
		if (curIdx ~= (ARGV[3]+1))
		then
			curIdx = redis.call("DECR", KEYS[2])
			return curIdx
		end
		local curNum = redis.call("INCRBY", KEYS[1], ARGV[1])
		local maxNum = tonumber(ARGV[2])
		if (curNum > maxNum) 
		then 
			redis.call("SET", KEYS[1], ARGV[2])
		end
		return curIdx
		`
	keyAddCmd := redis.NewScript(keyAddScript)
	res, err := keyAddCmd.Run(acc.client, []string{key, getKeyIdx(key)},
		keyAdd, keyLimit, keyIdx).Result()
	if err != nil && err != redis.Nil {
		return 0, err
	}

	if idx, ok := res.(int64); ok {
		return int(idx), nil
	}

	return 0, errors.New("res should be int64")
}
