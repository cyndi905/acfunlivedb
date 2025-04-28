package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"math/rand"
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

// 处理查询
func handleQuery(ctx context.Context, uid, count int) {
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
		fmt.Printf("开播时间：%s 主播uid：%d 昵称：%s 直播标题：%s liveID: %s streamName: %s 直播时长：%s 直播剪辑编号：%d\n",
			startTime(l.startTime), l.uid, l.name, l.title, l.liveID, l.streamName, duration(l.duration), l.liveCutNum,
		)
	}
	err = rows.Err()
	checkErr(err)
	if !hasUID {
		log.Printf("没有uid为 %d 的主播的记录", uid)
	}
}

// 处理输入
func handleInput(ctx context.Context) {
	const helpMsg = `请输入"listall 主播的uid"、"list10 主播的uid"、"getplayback liveID"、“detail liveID”、“checkrec”、“scanstreamer”或"quit"`

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		cmd := strings.Fields(scanner.Text())
		if len(cmd) == 0 {
			log.Println(helpMsg)
			continue
		}
		if len(cmd) == 1 {
			switch cmd[0] {
			case "quit":
				quit <- struct{}{}
				break
			case "fix":
				log.Println("正在修复duration信息，请等待")
				if len(oldList) == 0 {
					log.Println("目前没有正在直播的，请稍后再尝试")
				} else {
					for _, live := range oldList {
						updateLiveDuration(ctx, live.liveID, 0)
						log.Printf("已修复liveId为 %s 的duration信息为0", live.liveID)
					}
				}
			case "scanstreamer":
				log.Println("正在扫描并主包，请等待")
				go scanStreamer(ctx)
			case "checkrec":
				log.Println("正在扫描并补全直播信息，请等待")
				recs, err := queryDurationZero(ctx)
				if err != nil {
					log.Println(err)
				} else {
					if len(recs) != 0 {
						count := 0
						var mu sync.Mutex
						go func() {
							// 筛选当前不在直播中的记录
							var liveIDsToUpdate []string
							for _, liveID := range recs {
								if _, ok := oldList[liveID]; !ok {
									liveIDsToUpdate = append(liveIDsToUpdate, liveID)
								}
							}

							for i, liveID := range liveIDsToUpdate {
								// Seed the random number generator
								r := rand.New(rand.NewSource(time.Now().UnixNano()))
								// Generate a random number between 5 and 10
								sleepTime := time.Duration(5+r.Intn(6)) * time.Second
								time.Sleep(sleepTime)

								info, err := getPlayback(liveID)
								if err != nil {
									log.Printf("Error getting playback for liveID %s: %v", liveID, err)
								} else {
									if info.Duration != 0 {
										mu.Lock()
										updateLiveDuration(ctx, liveID, info.Duration)
										count++
										mu.Unlock()
										log.Printf("liveID为 %s 的记录已更新直播时长为：%d，进度: %d/%d", liveID, info.Duration, i+1, len(liveIDsToUpdate))
									} else {
										log.Printf("liveID为 %s 的记录无需更新直播时长，进度: %d/%d", liveID, i+1, len(liveIDsToUpdate))
									}
								}
							}
							log.Printf("已为%d条记录更新直播时长", count)
						}()
					} else {
						log.Printf("暂无duration为0的数据")
					}
				}
			default:
				log.Println(helpMsg)
				continue
			}
		} else {
			switch cmd[0] {
			case "listall":
				for _, u := range cmd[1:] {
					uid, err := strconv.ParseUint(u, 10, 64)
					if err != nil {
						log.Println(helpMsg)
					} else {
						handleQuery(ctx, int(uid), -1)
					}
				}
			case "list10":
				for _, u := range cmd[1:] {
					uid, err := strconv.ParseUint(u, 10, 64)
					if err != nil {
						log.Println(helpMsg)
					} else {
						handleQuery(ctx, int(uid), 10)
					}
				}
			case "getplayback":
				log.Println("查询录播链接，请等待")
				for _, liveID := range cmd[1:] {
					playback, err := getPlayback(liveID)
					if err != nil {
						log.Println(err)
					} else {

						if playback.Duration != 0 {
							if queryExist(ctx, liveID) {
								updateLiveDuration(ctx, liveID, playback.Duration)
							}
						}
						log.Printf("liveID为 %s 的录播查询结果是：\n录播链接：%s\n录播备份链接：%s",
							liveID, playback.URL, playback.BackupURL,
						)
					}
				}
			case "detail":
				for _, u := range cmd[1:] {
					err := runThrice(func() error {
						summary, err := ac.GetSummary(u)
						if summary != nil {
							log.Printf("liveID为 %s 的直播信息：duration:%d，点赞总数:%s，看过人数:%s", u, summary.Duration, summary.LikeCount, summary.WatchCount)
						}
						return err
					})
					if err != nil {
						log.Printf(err.Error())
					}
				}
			default:
				log.Println(helpMsg)
			}
		}
	}
	err := scanner.Err()
	checkErr(err)
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
}

func main() {
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
	//updateMaxOnlineCountStmt, err = db.PrepareContext(ctx, updateMaxOnlineCount)
	//checkErr(err)
	//defer updateMaxOnlineCountStmt.Close()
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
	go handleInput(childCtx)

	oldList = make(map[string]*live)
Loop:
	for {
		select {
		case <-childCtx.Done():
			break Loop
		default:
			var newList map[string]*live
			err = runThrice(func() error {
				newList, err = fetchLiveList()
				return err
			})
			if err != nil {
				panic("获取直播间列表数据出现过多错误")
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
				} else {
					livePool.Put(l)
				}
			}

			oldList = newList
			time.Sleep(20 * time.Second)
		}
	}
}
