package main

import (
	"bufio"
	"fmt"
	"net"
)

// redis 解析协议示例
func example() {
	conn, err := net.Dial("tcp", "localhost:6379")
	if err != nil {
		panic(err)
	}

	// 向 redis 发送命令
	cmd := "set kv-test bitcask-storage-yyds\r\n"
	_, err = conn.Write([]byte(cmd))
	if err != nil {
		panic(err)
	}

	// 解析 redis 的响应
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		panic(err)
	}
	fmt.Println(response)

}
