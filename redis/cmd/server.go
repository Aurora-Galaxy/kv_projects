package main

import (
	"github.com/tidwall/redcon"
	"kv_projects/conf"
	bs_redis "kv_projects/redis"
	"log"
	"sync"
)

const addr = "127.0.0.1:6380"

type BitcaskServer struct {
	dbs    map[int]*bs_redis.RedisDataStructure
	server *redcon.Server
	mutex  sync.RWMutex
}

func main() {
	// 打开 redis 数据结构服务
	conf.DefaultOptions.DirPath = "./temp"
	redisDataStructure, err := bs_redis.NewRedisDataStructure(conf.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// 初始化 BitcaskServer
	bitcaskServer := &BitcaskServer{dbs: make(map[int]*bs_redis.RedisDataStructure)}
	bitcaskServer.dbs[0] = redisDataStructure

	// 初始化 redis 服务端
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)

	bitcaskServer.listen()

}

func (svr *BitcaskServer) listen() {
	log.Println("bitcask server running, ready to accept connections.")
	_ = svr.server.ListenAndServe()
}

// 处理新的数据库连接
func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.mutex.Lock()
	defer svr.mutex.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	// 将连接的上下文设置为创建的 cli ，随后在该连接上接收的命令可以访问客户端实例的信息和状态
	conn.SetContext(cli)
	return true
}

// conn , err 为 redcon.NewServer 指定的参数
func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, v := range svr.dbs {
		_ = v.Close()
	}
	_ = svr.server.Close()
}
