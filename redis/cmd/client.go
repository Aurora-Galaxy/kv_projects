package main

import (
	"github.com/tidwall/redcon"
	"kv_projects/errs"
	bt_redis "kv_projects/redis"
	"kv_projects/utils"
	"strconv"
	"strings"
)

type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

// 保存不同命令对应的处理函数
var supportCommands = map[string]cmdHandler{
	// string
	"set": set,
	"get": get,
	// hash
	"hset": hSet,
	"hget": hGet,
	"hdel": hDel,
	// set
	"sadd":      sAdd,
	"sismember": sIsMember,
	"srem":      sRem,
	// list
	"lpush": lPush,
	"rpush": rPush,
	"lpop":  lPop,
	"rpop":  rPop,
	//ZSet
	"zadd":   zAdd,
	"zscore": zScore,
}

type BitcaskClient struct {
	db     *bt_redis.RedisDataStructure
	server *BitcaskServer
}

// 处理客户端传回的命令，并进行相应操作
func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	// 获取执行的命令类型，将其转为小写
	command := strings.ToLower(string(cmd.Args[0]))

	// 根据命令的类型，获取对应的处理函数
	cmdFunc, ok := supportCommands[command]
	if !ok {
		conn.WriteError("Err unsupported command : '" + command + "'")
		return
	}

	// 获取当前连接的客户端信息
	client, _ := conn.Context().(*BitcaskClient)
	switch command {
	case "quit":
		_ = conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		// 只截取了命令里的参数，具体命令前面已经处理
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if err == errs.ErrKeyNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		// 将得到的结果返回到页面显示
		conn.WriteAny(res)
	}
}

// ============================String 数据结构================================
/**
 * set
 * @Description: set方法
 * @param conn
 * @param args 只接收命令的参数，不接收命令类型
 * @return interface{}
 * @return error
 */
func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("set")
	}

	key, value := args[0], args[1]
	if err := cli.db.Set(key, value, 0); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("get")
	}
	key := args[0]
	res, err := cli.db.Get(key)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ============================Hash 数据结构================================
func hSet(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("hset")
	}
	key, filed, value := args[0], args[1], args[2]
	res, err := cli.db.HSet(key, filed, value)
	if err != nil {
		return nil, err
	}
	if res {
		// redcon.SimpleInt(1) 返回的格式为 (integer) 1
		return redcon.SimpleInt(1), nil
	} else {
		return redcon.SimpleInt(0), nil
	}
}
func hGet(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("hget")
	}
	key, filed := args[0], args[1]
	res, err := cli.db.HGet(key, filed)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func hDel(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("hdel")
	}
	key, filed := args[0], args[1]
	res, err := cli.db.HDel(key, filed)
	if err != nil {
		return nil, err
	}
	if res {
		// redcon.SimpleInt(1) 返回的格式为 (integer) 1
		return redcon.SimpleInt(1), nil
	} else {
		return redcon.SimpleInt(0), nil
	}
}

// ============================Set 数据结构================================
func sAdd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("sadd")
	}
	key, member := args[0], args[1]
	res, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		// redcon.SimpleInt(1) 返回的格式为 (integer) 1
		return redcon.SimpleInt(1), nil
	} else {
		return redcon.SimpleInt(0), nil
	}
}

func sIsMember(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("sIsMember")
	}
	key, member := args[0], args[1]
	res, err := cli.db.SIsMember(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		// redcon.SimpleInt(1) 返回的格式为 (integer) 1
		return redcon.SimpleInt(1), nil
	} else {
		return redcon.SimpleInt(0), nil
	}
}

func sRem(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("srem")
	}
	key, member := args[0], args[1]
	res, err := cli.db.SRem(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		// redcon.SimpleInt(1) 返回的格式为 (integer) 1
		return redcon.SimpleInt(1), nil
	} else {
		return redcon.SimpleInt(0), nil
	}
}

// ============================List 数据结构================================
func lPush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("lpush")
	}
	key, element := args[0], args[1]
	res, err := cli.db.LPush(key, element)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

func rPush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("rpush")
	}
	key, element := args[0], args[1]
	res, err := cli.db.RPush(key, element)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

func lPop(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("lpop")
	}
	key := args[0]
	res, err := cli.db.LPop(key)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func rPop(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("rpop")
	}
	key := args[0]
	res, err := cli.db.RPop(key)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ============================ZSet 数据结构================================
func zAdd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("zadd")
	}
	key, score, member := args[0], utils.FloatFromBytes(args[1]), args[2]
	res, err := cli.db.ZAdd(key, score, member)
	if err != nil {
		return nil, err
	}
	if res {
		// redcon.SimpleInt(1) 返回的格式为 (integer) 1
		return redcon.SimpleInt(1), nil
	} else {
		return redcon.SimpleInt(0), nil
	}
}

func zScore(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("zscore")
	}
	key, member := args[0], args[1]
	res, err := cli.db.ZScore(key, member)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString(strconv.FormatFloat(res, 'f', -1, 64)), nil
}
