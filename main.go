package main

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/orzogc/acfundanmu"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fastjson"
	_ "modernc.org/sqlite"
)

const userAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"

type live struct {
	liveID      string // 直播ID
	uid         int    // 主播uid
	name        string // 主播昵称
	streamName  string // 直播源ID
	startTime   int64  // 直播开始时间，单位为毫秒
	title       string // 直播间标题
	duration    int64  // 录播时长，单位为毫秒
	playbackURL string // 录播链接
	backupURL   string // 录播备份链接
	liveCutNum  int    // 直播剪辑编号
	coverUrl    string // 直播封面
	likeCount   int64  // 点赞数
	onlineCount int64  // 在线人数
}

// 设备 ID
//var deviceID string

var client = &fasthttp.Client{
	MaxIdleConnDuration: 90 * time.Second,
	ReadTimeout:         10 * time.Second,
	WriteTimeout:        10 * time.Second,
}
var oldList map[string]*live
var (
	liveListParserPool fastjson.ParserPool
	liveCutParserPool  fastjson.ParserPool
	quit               = make(chan struct{})
	ac                 *acfundanmu.AcFunLive
	dbMutex            = sync.RWMutex{}
	serviceMode        = flag.Bool("service", false, "Run in service mode") // <-- 添加服务模式的命令行参数
	socketPath         = "/tmp/acfunlive.sock"                              // <-- 定义用于服务模式的 Socket 文件路径
)

var livePool = &sync.Pool{
	New: func() interface{} {
		return new(live)
	},
}

// 检查错误
func checkErr(err error) {
	if err != nil {
		log.Fatalf("遇到致命错误: %v", err)
	}
}

// 尝试运行，三次出错后结束运行
func runThrice(f func() error) error {
	var err error
	for retry := 0; retry < 3; retry++ {
		if err = f(); err != nil {
			log.Printf("%v", err)
		} else {
			return nil
		}
		time.Sleep(10 * time.Second)
	}
	return fmt.Errorf("运行三次都出现错误：%v", err)
}

// 获取正在直播的直播间列表数据
func fetchLiveList() (list map[string]*live, e error) {
	defer func() {
		if err := recover(); err != nil {
			e = fmt.Errorf("fetchLiveList() error: %v", err)
		}
	}()

	const liveListURL = "https://live.acfun.cn/api/channel/list?count=%d&pcursor=0"
	//const liveListURL = "https://live.acfun.cn/rest/pc-direct/live/channel"

	p := liveListParserPool.Get()
	defer liveListParserPool.Put(p)
	var v *fastjson.Value

	for count := 10000; count < 1e9; count *= 10 {
		req := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(req)
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseResponse(resp)
		req.SetRequestURI(fmt.Sprintf(liveListURL, count))
		req.Header.SetMethod(fasthttp.MethodGet)
		req.Header.SetUserAgent(userAgent)
		req.Header.SetCookie("_did", ac.GetDeviceID())
		req.Header.Set("Accept-Encoding", "gzip")
		err := client.Do(req, resp)
		checkErr(err)
		var body []byte
		if string(resp.Header.Peek("content-encoding")) == "gzip" || string(resp.Header.Peek("Content-Encoding")) == "gzip" {
			body, err = resp.BodyGunzip()
			checkErr(err)
		} else {
			body = resp.Body()
		}

		v, err = p.ParseBytes(body)
		checkErr(err)
		v = v.Get("channelListData")
		if !v.Exists("result") || v.GetInt("result") != 0 {
			panic(fmt.Errorf("获取正在直播的直播间列表失败，响应为 %s", string(body)))
		}
		if string(v.GetStringBytes("pcursor")) == "no_more" {
			break
		}
		if count == 1e8 {
			panic(fmt.Errorf("获取正在直播的直播间列表失败"))
		}
	}

	liveList := v.GetArray("liveList")
	list = make(map[string]*live, len(liveList))
	for _, liveRoom := range liveList {
		l := livePool.Get().(*live)
		l.liveID = string(liveRoom.GetStringBytes("liveId"))
		l.uid = liveRoom.GetInt("authorId")
		l.name = string(liveRoom.GetStringBytes("user", "name"))
		l.streamName = string(liveRoom.GetStringBytes("streamName"))
		l.startTime = liveRoom.GetInt64("createTime")
		l.title = string(liveRoom.GetStringBytes("title"))
		covers := liveRoom.GetArray("coverUrls")
		if len(covers) > 0 {
			l.coverUrl = string(covers[0].GetStringBytes())
		} else {
			// 当没有封面 URL 时，给个默认值或留空
			l.coverUrl = ""
		}
		l.duration = 0
		l.playbackURL = ""
		l.backupURL = ""
		l.liveCutNum = 0
		l.onlineCount = liveRoom.GetInt64("onlineCount")
		l.likeCount = liveRoom.GetInt64("likeCount")
		list[l.liveID] = l
	}

	return list, nil
}

// 获取直播剪辑编号
func fetchLiveCut(uid int, liveID string) (num int, e error) {
	defer func() {
		if err := recover(); err != nil {
			num = 0
			e = fmt.Errorf("fetchLiveCut() error: %v", err)
		}
	}()

	const liveCutInfoURL = "https://live.acfun.cn/rest/pc-direct/live/getLiveCutInfo?authorId=%d&liveId=%s"

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(fmt.Sprintf(liveCutInfoURL, uid, liveID))
	req.Header.SetMethod(fasthttp.MethodGet)
	req.Header.SetUserAgent(userAgent)
	req.Header.SetCookie("_did", ac.GetDeviceID())
	req.Header.Set("Accept-Encoding", "gzip")
	err := client.Do(req, resp)
	checkErr(err)
	var body []byte
	if string(resp.Header.Peek("content-encoding")) == "gzip" || string(resp.Header.Peek("Content-Encoding")) == "gzip" {
		body, err = resp.BodyGunzip()
		checkErr(err)
	} else {
		body = resp.Body()
	}

	p := liveCutParserPool.Get()
	defer liveCutParserPool.Put(p)
	v, err := p.ParseBytes(body)
	checkErr(err)
	if !v.Exists("result") || v.GetInt("result") != 0 {
		panic(fmt.Errorf("获取uid为 %d 的主播的liveID为 %s 的直播剪辑信息失败，响应为 %s", uid, liveID, string(body)))
	}

	status := v.GetInt("liveCutStatus")
	if status != 1 {
		return 0, nil
	}
	url := string(v.GetStringBytes("liveCutUrl"))
	re := regexp.MustCompile(`/[0-9]+`)
	nums := re.FindAllString(url, -1)
	if len(nums) != 1 {
		panic(fmt.Errorf("获取uid为 %d 的主播的liveID为 %s 的直播剪辑编号失败，响应为 %s", uid, liveID, string(body)))
	}
	num, err = strconv.Atoi(nums[0][1:])
	checkErr(err)

	return num, nil
}

// 处理退出信号
func quitSignal(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	select {
	case <-ch:
	case <-quit:
	}

	signal.Stop(ch)
	signal.Reset(os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	log.Println("正在退出本程序，请等待")
	cancel()
}

// stime以毫秒为单位，返回具体开播时间
func startTime(stime int64) string {
	t := time.Unix(stime/1e3, 0)
	return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
}

// dtime以毫秒为单位，返回具体直播时长
func duration(dtime int64) string {
	t := time.Unix(dtime/1e3, 0).UTC()
	return fmt.Sprintf("%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
}
func processCommand(ctx context.Context, commandLine string, output io.Writer) {
	const helpMsg = `Commands:
listall <uid>...     - 查询改uid的主包所有直播记录
list10 <uid>...      - 查询改uid的主包10条直播记录
getplayback <liveID>... - 通过liveID获取回放地址
detail <liveID>...   - 查询该条直播信息
checkrec             - 扫描已结束的直播记录并补全信息
scanstreamer         - 扫描新增的主包
fix                  - 修复直播记录（直播记录显示结束的但实际还在直播的记录）
quit                 - 退出 (仅在窗口模式下可用，服务模式运行时无法使用)
` // 更新帮助信息

	cmd := strings.Fields(commandLine)
	if len(cmd) == 0 {
		_, err := fmt.Fprintln(output, helpMsg)
		if err != nil {
			return
		} // 输出到指定的 Writer
		return
	}

	switch cmd[0] {
	case "quit":
		// 在服务模式下，quit 命令不应该直接退出整个程序，
		// 这里我们让它在终端模式下发送退出信号，服务模式下提示无效。
		if !*serviceMode {
			_, err := fmt.Fprintln(output, "Shutting down...")
			if err != nil {
				return
			}
			quit <- struct{}{}
		} else {
			_, err := fmt.Fprintln(output, "Quit command is not available in service mode directly. Use OS signals or dedicated shutdown mechanism.")
			if err != nil {
				return
			}
		}
	case "fix":
		_, err := fmt.Fprintln(output, "正在修复duration信息，请等待")
		if err != nil {
			return
		}
		// 异步执行，日志会输出到主日志文件
		go func() {
			if len(oldList) == 0 {
				log.Println("目前没有正在直播的，请稍后再尝试") // 这个日志还是去文件
				_, err := fmt.Fprintln(output, "目前没有正在直播的，请稍后再尝试")
				if err != nil {
					return
				} // 这个输出到客户端
			} else {
				for _, live := range oldList {
					updateLiveDuration(ctx, live.liveID, 0)
					log.Printf("已修复liveId为 %s 的duration信息为0", live.liveID) // 这个日志还是去文件
					_, err := fmt.Fprintf(output, "已修复liveId为 %s 的duration信息为0\n", live.liveID)
					if err != nil {
						return
					} // 这个输出到客户端
				}
				_, err2 := fmt.Fprintln(output, "修复完成")
				if err2 != nil {
					return
				}
			}
		}()
	case "scanstreamer":
		_, err := fmt.Fprintln(output, "正在扫描并主包，请等待")
		if err != nil {
			return
		}
		go scanStreamer(ctx) // 异步执行，日志会输出到主日志文件
	case "checkrec":
		_, err := fmt.Fprintln(output, "正在扫描并补全直播信息，请等待")
		if err != nil {
			return
		}
		go func() { // 异步执行，日志会输出到主日志文件
			recs, err := queryDurationZero(ctx)
			if err != nil {
				log.Println(err) // 这个日志还是去文件
				_, err := fmt.Fprintln(output, "Error querying records with duration 0:", err)
				if err != nil {
					return
				} // 输出到客户端
			} else {
				if len(recs) != 0 {
					count := 0
					var mu sync.Mutex
					// 筛选当前不在直播中的记录
					var liveIDsToUpdate []string
					for _, liveID := range recs {
						if _, ok := oldList[liveID]; !ok {
							liveIDsToUpdate = append(liveIDsToUpdate, liveID)
						}
					}

					if len(liveIDsToUpdate) == 0 {
						_, err := fmt.Fprintln(output, "暂无需要补全时长的不在直播的记录")
						if err != nil {
							return
						}
						return
					}

					_, err := fmt.Fprintf(output, "找到 %d 条需要补全时长的记录\n", len(liveIDsToUpdate))
					if err != nil {
						return
					}

					for i, liveID := range liveIDsToUpdate {
						select {
						case <-ctx.Done():
							_, err := fmt.Fprintln(output, "补全任务已取消")
							if err != nil {
								return
							}
							log.Println("补全任务已取消") // 这个日志还是去文件
							return
						default:
							// Seed the random number generator
							r := rand.New(rand.NewSource(time.Now().UnixNano()))
							// Generate a random number between 5 and 10
							sleepTime := time.Duration(5+r.Intn(6)) * time.Second
							time.Sleep(sleepTime)

							info, err := getPlayback(liveID) // 注意：getPlayback 内部使用了 runThrice 和 log.Printf
							if err != nil {
								log.Printf("Error getting playback for liveID %s: %v", liveID, err) // 这个日志还是去文件
								_, err := fmt.Fprintf(output, "Error getting playback for liveID %s: %v\n", liveID, err)
								if err != nil {
									return
								} // 输出到客户端
							} else {
								if info.Duration != 0 {
									mu.Lock()
									updateLiveDuration(ctx, liveID, info.Duration)
									count++
									mu.Unlock()
									log.Printf("liveID为 %s 的记录已更新直播时长为：%d，进度: %d/%d", liveID, info.Duration, i+1, len(liveIDsToUpdate)) // 这个日志还是去文件
									_, err := fmt.Fprintf(output, "liveID为 %s 的记录已更新直播时长为：%d，进度: %d/%d\n", liveID, info.Duration, i+1, len(liveIDsToUpdate))
									if err != nil {
										return
									} // 输出到客户端
								} else {
									log.Printf("liveID为 %s 的记录无需更新直播时长，进度: %d/%d", liveID, i+1, len(liveIDsToUpdate)) // 这个日志还是去文件
									_, err := fmt.Fprintf(output, "liveID为 %s 的记录无需更新直播时长，进度: %d/%d\n", liveID, i+1, len(liveIDsToUpdate))
									if err != nil {
										return
									} // 输出到客户端
								}
							}
						}
					}
					log.Printf("已为%d条记录更新直播时长", count) // 这个日志还是去文件
					_, err = fmt.Fprintf(output, "已为%d条记录更新直播时长\n", count)
					if err != nil {
						return
					} // 输出到客户端
				} else {
					log.Printf("暂无duration为0的数据") // 这个日志还是去文件
					_, err := fmt.Fprintln(output, "暂无duration为0的数据")
					if err != nil {
						return
					} // 输出到客户端
				}
			}
		}()

	default: // 处理带参数的命令
		if len(cmd) < 2 {
			_, err := fmt.Fprintln(output, "命令缺少参数:", cmd[0])
			if err != nil {
				return
			}
			_, err = fmt.Fprintln(output, helpMsg)
			if err != nil {
				return
			}
			return
		}
		switch cmd[0] {
		case "listall":
			for _, u := range cmd[1:] {
				uid, err := strconv.ParseUint(u, 10, 64)
				if err != nil {
					_, err := fmt.Fprintln(output, "无效的UID:", u)
					if err != nil {
						return
					}
					_, err = fmt.Fprintln(output, helpMsg)
					if err != nil {
						return
					}
				} else {
					handleQuery(ctx, int(uid), -1, output) // <-- 传递 output Writer
				}
			}
		case "list10":
			for _, u := range cmd[1:] {
				uid, err := strconv.ParseUint(u, 10, 64)
				if err != nil {
					_, err := fmt.Fprintln(output, "无效的UID:", u)
					if err != nil {
						return
					}
					_, err = fmt.Fprintln(output, helpMsg)
					if err != nil {
						return
					}
				} else {
					handleQuery(ctx, int(uid), 10, output) // <-- 传递 output Writer
				}
			}
		case "getplayback":
			_, err := fmt.Fprintln(output, "查询录播链接，请等待")
			if err != nil {
				return
			}
			for _, liveID := range cmd[1:] {
				playback, err := getPlayback(liveID) // 注意：getPlayback 内部使用了 runThrice 和 log.Printf
				if err != nil {
					log.Println(err) // 这个日志还是去文件
					_, err := fmt.Fprintln(output, "Error getting playback for liveID", liveID, ":", err)
					if err != nil {
						return
					} // 输出到客户端
				} else {
					if playback.Duration != 0 {
						// 仅在数据库中存在该 liveID 时才更新 duration
						if queryExist(ctx, liveID) { // queryExist 需要 dbMutex
							updateLiveDuration(ctx, liveID, playback.Duration) // updateLiveDuration 需要 dbMutex
						} else {
							log.Printf("liveID %s 不在数据库中，跳过更新 duration", liveID)
							_, err := fmt.Fprintf(output, "liveID %s 不在数据库中，跳过更新 duration\n", liveID)
							if err != nil {
								return
							}
						}
					}
					_, err := fmt.Fprintf(output, "liveID为 %s 的录播查询结果是：\n录播链接：%s\n录播备份链接：%s\n",
						liveID, playback.URL, playback.BackupURL,
					)
					if err != nil {
						return
					} // 输出到客户端
				}
			}
		case "detail":
			for _, u := range cmd[1:] {
				err := runThrice(func() error {
					summary, err := ac.GetSummary(u)
					if summary != nil {
						// 使用 fmt.Fprintf 将结果输出到 output Writer
						_, err := fmt.Fprintf(output, "liveID为 %s 的直播信息：duration:%d，点赞总数:%d，看过人数:%d\n", u, summary.Duration, summary.LikeCount, summary.WatchCount)
						if err != nil {
							return err
						}
					} else if err == nil {
						// 如果 summary 为 nil 但 err 也为 nil，可能是直播不存在或已删除
						_, err := fmt.Fprintf(output, "liveID为 %s 的直播信息未找到或已删除\n", u)
						if err != nil {
							return err
						}
					}
					return err // runThrice 依赖这里的错误返回
				})
				if err != nil {
					log.Printf("Error getting detail for liveID %s: %v", u, err) // 这个日志还是去文件
					_, err := fmt.Fprintf(output, "Error getting detail for liveID %s: %v\n", u, err)
					if err != nil {
						return
					} // 输出到客户端
				}
			}
		default:
			_, err := fmt.Fprintln(output, "未知命令:", cmd[0])
			if err != nil {
				return
			}
			_, err = fmt.Fprintln(output, helpMsg)
			if err != nil {
				return
			}
		}
	}

}

// 处理查询
func handleQuery(ctx context.Context, uid, count int, output io.Writer) {
	l := live{}
	dbMutex.RLock()
	defer dbMutex.RUnlock()
	rows, err := selectUIDStmt.QueryContext(ctx, uid, count)
	checkErr(err)
	defer rows.Close()
	hasUID := false
	for rows.Next() {
		hasUID = true
		err = rows.Scan(&l.liveID, &l.uid, &l.name, &l.streamName, &l.startTime, &l.title, &l.duration, &l.playbackURL, &l.backupURL, &l.liveCutNum)
		checkErr(err)
		// 使用 fmt.Fprintf 将结果输出到 output Writer
		_, err := fmt.Fprintf(output, "开播时间：%s 主播uid：%d 昵称：%s 直播标题：%s liveID: %s streamName: %s 直播时长：%s 直播剪辑编号：%d\n",
			startTime(l.startTime), l.uid, l.name, l.title, l.liveID, l.streamName, duration(l.duration), l.liveCutNum,
		)
		if err != nil {
			return
		}
	}
	err = rows.Err()
	checkErr(err)
	if !hasUID {
		log.Printf("没有uid为 %d 的主播的记录", uid) // 日志
		_, err := fmt.Fprintf(output, "没有uid为 %d 的主播的记录\n", uid)
		if err != nil {
			return
		} // 输出到客户端
	}
}

// **终端模式输入处理函数**
func startInteractiveHandler(ctx context.Context) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("进入交互模式，输入 'help' 查看命令列表") // 提示用户
	for {
		select {
		case <-ctx.Done():
			fmt.Println("交互模式退出")
			return // Context Cancelled, exit handler
		default:
			fmt.Print("> ") // 提示符
			if !scanner.Scan() {
				// 如果 Scan 失败，可能是 EOF 或错误
				if err := scanner.Err(); err != nil {
					log.Printf("Error reading from stdin: %v", err)
				}
				// 通常 EOF 意味着输入结束，可以考虑退出
				quit <- struct{}{} // 发送退出信号
				return
			}
			commandLine := scanner.Text()
			// 调用通用的命令处理函数，输出到 os.Stdout
			processCommand(ctx, commandLine, os.Stdout)
		}
	}
}

// **服务模式 Socket 服务器**
func startSocketServer(ctx context.Context) {
	// 启动前先移除旧的 Socket 文件
	os.Remove(socketPath)

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("无法监听 Unix Socket: %v", err)
	}
	log.Printf("服务模式已启动，监听 Socket: %s", socketPath)

	// 确保退出时清理 Socket 文件
	go func() {
		<-ctx.Done()
		log.Println("Context Done, closing socket listener")
		listener.Close()
		os.Remove(socketPath) // 清理 Socket 文件
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			// 如果 Context Done，Accept 会返回错误，这里判断后正常退出
			select {
			case <-ctx.Done():
				return
			default:
				log.Printf("接受连接错误: %v", err)
				continue
			}
		}
		log.Printf("接受到新连接")
		// 为每个连接启动一个 goroutine 处理
		go handleSocketConnection(ctx, conn)
	}
}

// **处理单个 Socket 连接**
func handleSocketConnection(ctx context.Context, conn net.Conn) {
	defer func() {
		log.Printf("连接关闭: %s", conn.RemoteAddr().Network()) // Unix Socket 没有 RemoteAddr() 严格意义上的网络地址
		err := conn.Close()
		if err != nil {
			return
		}
	}()

	scanner := bufio.NewScanner(conn)
	writer := bufio.NewWriter(conn)

	_, err := fmt.Fprintln(writer, "连接成功，输入 'help' 查看命令列表")
	if err != nil {
		return
	}
	err = writer.Flush()
	if err != nil {
		return
	} // 确保立即发送欢迎消息

	for {
		select {
		case <-ctx.Done():
			_, err := fmt.Fprintln(writer, "服务正在关闭，连接断开")
			if err != nil {
				return
			}
			err = writer.Flush()
			if err != nil {
				return
			}
			return // Context Cancelled, exit handler
		default:
			// 设置读取超时，防止连接僵死
			err := conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
			if err != nil {
				return
			} // 例如 5 分钟无活动自动断开
			if !scanner.Scan() {
				if err := scanner.Err(); err != nil {
					if err == io.EOF {
						log.Println("客户端连接已关闭 (EOF)")
					} else {
						log.Printf("从连接读取命令错误: %v", err)
					}
				}
				return // 读取错误或 EOF，关闭连接
			}
			commandLine := scanner.Text()
			log.Printf("收到命令: %s", commandLine) // 记录收到的命令

			// 调用通用的命令处理函数，输出到 Socket 连接
			processCommand(ctx, commandLine, writer)
			err = writer.Flush()
			if err != nil {
				return
			} // 每次处理完命令后刷新缓冲区，发送结果
		}
	}
}

// 获取指定liveID的playback
func getPlayback(liveID string) (playback *acfundanmu.Playback, err error) {
	err = runThrice(func() error {
		playback, err = ac.GetPlayback(liveID)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("获取liveID为 %s 的playback失败：%w", liveID, err)
	}

	if playback.URL != "" {
		aliURL, txURL := playback.Distinguish()
		if aliURL != "" && txURL != "" {
			playback.URL = aliURL
			playback.BackupURL = txURL
		} else {
			log.Printf("无法获取liveID为 %s 的阿里云录播链接或腾讯云录播链接", liveID)
		}
	}

	return playback, nil
}

// 准备table
func prepare_table(ctx context.Context) {
	// 检查table是否存在
	row := db.QueryRowContext(ctx, checkTable)
	var n, n1 int
	err := row.Scan(&n)
	checkErr(err)
	if n == 0 {
		// table不存在
		_, err = db.ExecContext(ctx, createTable)
		checkErr(err)
	} else {
		// table存在，检查liveCutNum是否存在
		row = db.QueryRowContext(ctx, checkLiveCutNum)
		err = row.Scan(&n)
		checkErr(err)
		if n == 0 {
			// liveCutNum不存在，插入liveCutNum
			_, err = db.ExecContext(ctx, insertLiveCutNum)
			checkErr(err)
		}
		// table存在，检查isSync是否存在
		row = db.QueryRowContext(ctx, checkIsSync)
		err = row.Scan(&n)
		checkErr(err)
		if n == 0 {
			// isSync不存在，插入isSync
			_, err = db.ExecContext(ctx, insertIsSync)
			checkErr(err)
		}
	}
	r := db.QueryRowContext(ctx, checkStreamerTable)
	err = r.Scan(&n1)
	checkErr(err)
	if n1 == 0 {
		// table不存在
		_, err = db.ExecContext(ctx, createStreamerTable)
		checkErr(err)
	}
	_, err = db.ExecContext(ctx, createLiveIDIndex)
	checkErr(err)
	_, err = db.ExecContext(ctx, createUIDIndex)
	checkErr(err)
	_, err = db.ExecContext(ctx, createStreamerUIDIndex)
	checkErr(err)
	_, err = db.ExecContext(ctx, createNameIndex)
	checkErr(err)
	_, err = db.ExecContext(ctx, createDurationStartTimeIndex)
	checkErr(err)
	_, err = db.ExecContext(ctx, createDurationIndex)
	checkErr(err)
	_, err = db.ExecContext(ctx, createStartTimeIndex)
	checkErr(err)
}

func main() {
	flag.Parse() // 解析命令行参数
	logFile, err := os.OpenFile("error.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("无法打开日志文件: %v", err)
	}
	defer logFile.Close()
	// 同时将日志输出到文件和控制台
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(multiWriter)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go quitSignal(cancel)

	path, err := os.Executable()
	checkErr(err)
	dir := filepath.Dir(path)
	dbFile := filepath.Join(dir, "acfunlive.db")

	db, err = sql.Open("sqlite", dbFile)

	// 设置 busy_timeout
	_, err = db.Exec("PRAGMA busy_timeout = 3000;")
	checkErr(err)
	defer db.Close()
	err = db.Ping()
	checkErr(err)
	prepare_table(ctx)

	insertStmt, err = db.PrepareContext(ctx, insertLive)
	checkErr(err)
	defer insertStmt.Close()
	updateLiveCutStmt, err = db.PrepareContext(ctx, updateLiveCut)
	checkErr(err)
	defer updateLiveCutStmt.Close()
	updateDurationStmt, err = db.PrepareContext(ctx, updateDuration)
	checkErr(err)
	defer updateDurationStmt.Close()
	updateCoverUrlAndLikeCountAndMaxOnlineCountStmt, err = db.PrepareContext(ctx, updateCoverUrlAndLikeCountAndMaxOnlineCount)
	checkErr(err)
	defer updateCoverUrlAndLikeCountAndMaxOnlineCountStmt.Close()
	updateLikeCountAndMaxOnlineCountStmt, err = db.PrepareContext(ctx, updateLikeCountAndMaxOnlineCount)
	checkErr(err)
	defer updateLikeCountAndMaxOnlineCountStmt.Close()
	selectUIDStmt, err = db.PrepareContext(ctx, selectUID)
	checkErr(err)
	defer selectUIDStmt.Close()
	selectLiveIDStmt, err = db.PrepareContext(ctx, selectLiveID)
	checkErr(err)
	defer selectLiveIDStmt.Close()
	selectDurationZeroStmt, err = db.PrepareContext(ctx, selectDurationZero)
	checkErr(err)
	defer selectDurationZeroStmt.Close()
	scanStreamerStmt, err = db.PrepareContext(ctx, scanStreamerToTable)
	checkErr(err)
	defer scanStreamerStmt.Close()

	ac, err = acfundanmu.NewAcFunLive()
	checkErr(err)
	//deviceID, err = acfundanmu.GetDeviceID()
	//checkErr(err)
	// **根据模式启动不同的命令处理goroutine**
	if *serviceMode {
		go startSocketServer(childCtx) // 启动 Socket 服务器
	} else {
		go startInteractiveHandler(childCtx) // 启动终端输入处理
	}

	oldList = make(map[string]*live)
Loop:
	for {
		select {
		case <-childCtx.Done():
			log.Println("收到退出信号，正在停止...")
			break Loop
		default:
			var newList map[string]*live
			err = runThrice(func() error {
				newList, err = fetchLiveList()
				return err
			})
			if err != nil {
				log.Printf("获取直播间列表数据出现过多错误，跳过本次更新：%v", err)
				time.Sleep(10 * time.Second) // 错误时等待更长时间
				continue                     // 跳过本次循环
			}

			if len(newList) == 0 {
				log.Println("没有人在直播")
			}

			for _, l := range newList {
				if _, ok := oldList[l.liveID]; !ok {
					// 新的liveID
					insert(ctx, l)
					go func(uid int, liveID string) {
						var num int
						var err error
						err = runThrice(func() error {
							num, err = fetchLiveCut(uid, liveID)
							return err
						})
						if err != nil {
							log.Printf("获取uid为 %d 的主播的liveID为 %s 的直播剪辑编号失败，放弃获取", uid, liveID)
							return
						}
						updateLiveCutNum(ctx, liveID, num)
					}(l.uid, l.liveID)
				} else {
					oldLive := oldList[l.liveID]
					// 确保oldLive不是nil指针
					if oldLive != nil && (*oldLive).coverUrl == l.coverUrl {
						updateLikeAndMaxOnlineCount(ctx, l.liveID, l.likeCount, l.onlineCount)
					} else {
						// 封面发生改变时同时更新封面和点赞数
						updateCoverAndLikeAndMaxOnlineCount(ctx, l.liveID, l.coverUrl, l.likeCount, l.onlineCount)
					}
					//updateMaxOnline(ctx, l.liveID, l.onlineCount)
				}
			}

			for _, l := range oldList {
				if _, ok := newList[l.liveID]; !ok {
					// liveID对应的直播结束
					go func(l *live) {
						defer livePool.Put(l)
						time.Sleep(10 * time.Second)
						var summary *acfundanmu.Summary
						var err error
						err = runThrice(func() error {
							summary, err = ac.GetSummary(l.liveID)
							return err
						})
						if err != nil {
							log.Printf("获取 %s（%d） 的liveID为 %s 的直播总结出现错误，放弃获取", l.name, l.uid, l.liveID)
							return
						}
						if summary.Duration == 0 {
							log.Printf("直播时长为0，无法获取 %s（%d） 的liveID为 %s 的直播时长", l.name, l.uid, l.liveID)
							return
						}
						insert(ctx, l)
						updateLiveDuration(ctx, l.liveID, summary.Duration)
					}(l)
					// 有可能出现还在直播中但直播列表中没有的情况，需要再次确认是否下播
					//if IsLiveOnByPage(l.uid) == false {
					//}
				} else {
					livePool.Put(l)
				}
			}

			oldList = newList
			time.Sleep(20 * time.Second)
		}
	}
}
