//go:build cli
// +build cli

package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

func main() {
	socketPath := "/tmp/acfunlive.sock"
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "无法连接到服务: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	reader := bufio.NewReader(os.Stdin)
	connReader := bufio.NewReader(conn)
	connWriter := bufio.NewWriter(conn)

	fmt.Println("连接成功，输入命令 ('quit' to exit client)")

	// 启动一个 goroutine 读取服务端的响应并打印
	go func() {
		for {
			conn.SetReadDeadline(time.Now().Add(5 * time.Minute)) // 设置读取超时
			line, err := connReader.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					fmt.Fprintf(os.Stderr, "从服务端读取错误: %v\n", err)
				} else {
					fmt.Fprintln(os.Stderr, "服务端连接已关闭")
				}
				os.Exit(1) // 服务端断开，客户端也退出
			}
			fmt.Print("服务响应: ", line)
		}
	}()

	// 读取用户输入并发送给服务端
	for {
		fmt.Print("> ") // 客户端提示符
		input, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "读取输入错误: %v\n", err)
			}
			break // 读取输入错误，退出循环
		}

		command := strings.TrimSpace(input)
		if command == "quit" {
			// 客户端输入 quit，只退出客户端，不发送给服务端
			break
		}

		_, err = connWriter.WriteString(command + "\n")
		if err != nil {
			fmt.Fprintf(os.Stderr, "发送命令到服务端错误: %v\n", err)
			break
		}
		connWriter.Flush()
	}

	fmt.Println("客户端退出")
}
