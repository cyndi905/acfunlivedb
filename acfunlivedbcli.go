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
	"sync/atomic" // 使用原子操作标记状态，更简单可靠
)

func main() {
	socketPath := "/tmp/acfunlive.sock"
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "无法连接到服务: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("连接成功，输入命令 ('quit' 或 'exit' 退出客户端)")

	// busy 标志位：1 表示服务端忙碌，0 表示空闲
	var isBusy int32 = 0
	// 通知通道，用于在服务端处理完后立即唤醒输入提示
	idleChan := make(chan struct{}, 1)

	// 1. 启动读取协程
	go func() {
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					fmt.Println("\n[服务端已断开连接]")
				} else {
					fmt.Printf("\n[读取错误]: %v\n", err)
				}
				os.Exit(0)
			}

			// 处理特殊的协议信号
			cleanLine := strings.TrimSpace(line)
			if cleanLine == ":::BUSY:::" {
				atomic.StoreInt32(&isBusy, 1)
				continue // 不打印信号本身
			} else if cleanLine == ":::IDLE:::" {
				atomic.StoreInt32(&isBusy, 0)
				// 发送信号尝试唤醒提示符
				select {
				case idleChan <- struct{}{}:
				default:
				}
				continue // 不打印信号本身
			}

			// 打印正常的业务输出（包括进度条）
			fmt.Print(line)
		}
	}()

	// 2. 主循环：读取用户输入
	scanner := bufio.NewScanner(os.Stdin)
	for {
		// 如果服务端忙碌，则阻塞等待 idleChan 信号
		if atomic.LoadInt32(&isBusy) == 1 {
			<-idleChan
		}

		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}
		if input == "exit" || input == "quit" {
			break
		}

		// 发送前双重检查状态，防止在等待输入时服务端突然变忙（虽然在单用户下很少见）
		if atomic.LoadInt32(&isBusy) == 1 {
			fmt.Println("服务端正在处理其他任务，请稍后...")
			continue
		}

		_, err := fmt.Fprintln(conn, input)
		if err != nil {
			fmt.Printf("发送失败: %v\n", err)
			break
		}

		// 发送完指令后，立即进入忙碌状态，等待服务端确认 :::BUSY:::
		atomic.StoreInt32(&isBusy, 1)
	}
	fmt.Println("客户端已退出")
}
