package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const cursorFile = "last_sacn_start_time.txt"

// 新建table
const createTable = `CREATE TABLE IF NOT EXISTS acfunlive (
	liveID TEXT PRIMARY KEY,
	uid INTEGER NOT NULL,
	name TEXT NOT NULL,
	streamName TEXT NOT NULL UNIQUE,
	startTime INTEGER NOT NULL,
	title TEXT NOT NULL,
	duration INTEGER NOT NULL,
	playbackURL TEXT NOT NULL,
	backupURL TEXT NOT NULL,
	liveCutNum INTEGER NOT NULL DEFAULT 0,
	coverUrl TEXT NOT NULL,
	likeCount INTEGER DEFAULT 0,
	maxOnlineCount INTEGER DEFAULT 0,
	isSync INTEGER DEFAULT 0
);
`

const createStreamerTable = `CREATE TABLE IF NOT EXISTS streamer (
	uid INTEGER NOT NULL PRIMARY KEY,
	name TEXT NOT NULL
)
`

func getExecutableDir() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Dir(exe), nil
}

// readLastStartTimeMS 从文件读取上次处理的最大 startTime（毫秒时间戳）
func readLastStartTimeMS() int64 {
	dir, err := getExecutableDir()
	if err != nil {
		log.Printf("获取可执行文件目录失败: %v", err)
		return 0
	}
	cursorPath := filepath.Join(dir, cursorFile)
	data, err := os.ReadFile(cursorPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("首次运行，从时间戳 0 开始")
			return 0
		}
		log.Printf("读取游标文件失败: %v", err)
		return 0
	}
	ts, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		log.Printf("解析时间戳失败（内容: %q）: %v", string(data), err)
		return 0
	}
	return ts
}

// writeLastStartTimeMS 将最新的 startTime 写入游标文件
func writeLastStartTimeMS(ts int64) {
	dir, err := getExecutableDir()
	if err != nil {
		log.Printf("获取可执行文件目录失败: %v", err)
		return
	}
	cursorPath := filepath.Join(dir, cursorFile)
	err = os.WriteFile(cursorPath, []byte(strconv.FormatInt(ts, 10)), 0644)
	if err != nil {
		log.Printf("写入游标文件失败: %v", err)
	}
}

// 获取本次处理的最大 startTime（用于更新游标）
func getMaxStartTimeSince(db *sql.DB, lastTS int64) (int64, error) {
	var maxTS int64
	err := db.QueryRow(`
		SELECT COALESCE(MAX(startTime), ?)
		FROM acfunlive
		WHERE startTime > ?
	`, lastTS, lastTS).Scan(&maxTS)
	return maxTS, err
}

const scanStreamerToTable = `INSERT INTO streamer (uid, name)
SELECT a.uid, a.name
FROM acfunlive AS a
WHERE a.startTime > ?
  AND a.startTime = (
    SELECT MAX(b.startTime)
    FROM acfunlive AS b
    WHERE b.uid = a.uid
      AND b.startTime > ?
  )
ON CONFLICT(uid)
DO UPDATE SET name = excluded.name
WHERE streamer.name != excluded.name;`

// 插入live
const insertLive = `INSERT OR IGNORE INTO acfunlive
(liveID, uid, name, streamName, startTime, title, duration, playbackURL, backupURL, liveCutNum, coverUrl, likeCount, maxOnlineCount,isSync)
VALUES
(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,0);
`

// 根据uid查询
const selectUID = `SELECT liveID, uid, name, streamName, startTime, title, duration, playbackURL, backupURL, liveCutNum, coverUrl, likeCount, maxOnlineCount FROM acfunlive
WHERE uid = ?
ORDER BY startTime DESC
LIMIT ?;
`

const (
	selectLiveID                                = `SELECT uid FROM acfunlive WHERE liveID = ?;`                                                    // 根据liveID查询
	createLiveIDIndex                           = `CREATE INDEX IF NOT EXISTS liveIDIndex ON acfunlive (liveID);`                                  // 生成liveID的index
	createUIDIndex                              = `CREATE INDEX IF NOT EXISTS uidIndex ON acfunlive (uid);`                                        // 生成uid的index
	createNameIndex                             = `CREATE INDEX IF NOT EXISTS nameIndex ON streamer (name);`                                       // 生成name的index
	createStreamerUIDIndex                      = `CREATE INDEX IF NOT EXISTS uidIndex ON streamer (uid);`                                         // 生成主播表uid的index
	createDurationStartTimeIndex                = `CREATE INDEX IF NOT EXISTS idx_duration_startTime ON acfunlive (duration ASC, startTime DESC);` // 三个加快查询速度的索引
	createStartTimeIndex                        = `CREATE INDEX IF NOT EXISTS idx_uid_startTime ON acfunlive (startTime DESC);`
	createDurationIndex                         = `CREATE INDEX IF NOT EXISTS idx_lives_duration ON acfunlive (duration DESC);`
	checkTable                                  = `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='acfunlive';`                                                                                              // 检查table是否存在
	checkStreamerTable                          = `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='streamer';`                                                                                               // 检查主播table是否存在
	checkLiveCutNum                             = `SELECT COUNT(*) AS CNTREC FROM pragma_table_info('acfunlive') WHERE name='liveCutNum';`                                                                                   // 检查liveCutNum是否存在
	insertLiveCutNum                            = `ALTER TABLE acfunlive ADD COLUMN liveCutNum INTEGER NOT NULL DEFAULT 0;`                                                                                                  // 插入直播剪辑编号
	checkIsSync                                 = `SELECT COUNT(*) AS CNTREC FROM pragma_table_info('acfunlive') WHERE name='isSync';`                                                                                       // 检查isSync是否存在
	insertIsSync                                = `ALTER TABLE acfunlive ADD COLUMN isSync INTEGER DEFAULT 0;`                                                                                                               // 插入记录同步标志
	updateLiveCut                               = `UPDATE acfunlive SET liveCutNum = ?,isSync = 0 WHERE liveID = ?;`                                                                                                         // 更新直播剪辑编号
	updateDuration                              = `UPDATE acfunlive SET duration = ?,isSync = 0 WHERE liveID = ?;`                                                                                                           // 更新直播时长
	updateCoverUrlAndLikeCountAndMaxOnlineCount = `UPDATE acfunlive SET coverUrl = ?, likeCount = ?, maxOnlineCount = CASE WHEN ? > maxOnlineCount THEN ? ELSE maxOnlineCount END,isSync = 0,duration = 0 WHERE liveID = ?;` // 更新封面和点赞数和最高在线人数
	//updateMaxOnlineCount             = `UPDATE acfunlive SET maxOnlineCount = ?,isSync = 0 WHERE liveID = ? AND ? > maxOnlineCount;`// 更新最高在线人数
	updateLikeCountAndMaxOnlineCount = `UPDATE acfunlive SET likeCount = ?, maxOnlineCount = CASE WHEN ? > maxOnlineCount THEN ? ELSE maxOnlineCount END, isSync = 0,duration = 0 WHERE liveID = ?;` // 更新点赞数和最高在线人数
	selectDurationZero               = `SELECT liveId FROM acfunlive WHERE duration = 0;`                                                                                                            // 查询duration为0的数据
)

var (
	db                                              *sql.DB
	insertStmt                                      *sql.Stmt
	updateLiveCutStmt                               *sql.Stmt
	updateDurationStmt                              *sql.Stmt
	selectUIDStmt                                   *sql.Stmt
	selectLiveIDStmt                                *sql.Stmt
	updateCoverUrlAndLikeCountAndMaxOnlineCountStmt *sql.Stmt
	//updateMaxOnlineCountStmt                        *sql.Stmt
	updateLikeCountAndMaxOnlineCountStmt *sql.Stmt
	selectDurationZeroStmt               *sql.Stmt
	scanStreamerStmt                     *sql.Stmt
)

// 插入live
func insert(ctx context.Context, l *live) {
	dbMutex.Lock()
	defer dbMutex.Unlock()
	_, err := insertStmt.ExecContext(ctx,
		l.liveID, l.uid, l.name, l.streamName, l.startTime, l.title, l.duration, l.playbackURL, l.backupURL, l.liveCutNum, l.coverUrl, l.likeCount, l.onlineCount,
	)
	checkErr(err)
}

func scanStreamer(ctx context.Context) {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	lastTS := readLastStartTimeMS()

	// 执行增量更新（传两次 lastTS）
	result, err := scanStreamerStmt.ExecContext(ctx, lastTS, lastTS)
	if err != nil {
		log.Printf("执行 scanStreamer 失败: %v", err)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	log.Printf("已更新主播表，影响行数: %d", rowsAffected)

	// 更新游标
	newMaxTS, err := getMaxStartTimeSince(db, lastTS)
	if err != nil {
		log.Printf("获取最大 startTime 失败: %v", err)
		return
	}

	if newMaxTS > lastTS {
		writeLastStartTimeMS(newMaxTS)
		log.Printf("游标已更新至 startTime=%d (%s)",
			newMaxTS,
			time.UnixMilli(newMaxTS).Format("2006-01-02 15:04:05.000"))
	}
}

// 更新直播剪辑编号
func updateLiveCutNum(ctx context.Context, liveID string, num int) {
	dbMutex.Lock()
	defer dbMutex.Unlock()
	_, err := updateLiveCutStmt.ExecContext(ctx, num, liveID)
	checkErr(err)
}

// 更新直播时长
func updateLiveDuration(ctx context.Context, liveID string, duration int64) {
	dbMutex.Lock()
	defer dbMutex.Unlock()
	_, err := updateDurationStmt.ExecContext(ctx, duration, liveID)
	checkErr(err)
}

// 更新直播封面和点赞数和最高在线人数
func updateCoverAndLikeAndMaxOnlineCount(ctx context.Context, liveID string, cover string, likeCount int64, onlineCount int64) {
	dbMutex.Lock()
	defer dbMutex.Unlock()
	_, err := updateCoverUrlAndLikeCountAndMaxOnlineCountStmt.ExecContext(ctx, cover, likeCount, onlineCount, onlineCount, liveID)
	checkErr(err)
}

// 更新直播点赞数和最高在线人数
func updateLikeAndMaxOnlineCount(ctx context.Context, liveID string, likeCount int64, onlineCount int64) {
	dbMutex.Lock()
	defer dbMutex.Unlock()
	_, err := updateLikeCountAndMaxOnlineCountStmt.ExecContext(ctx, likeCount, onlineCount, onlineCount, liveID)
	checkErr(err)
}

// 更新直播最高在线人数
//func updateMaxOnline(ctx context.Context, liveID string, onlineCount int64) {
//	dbMutex.Lock()
//	defer dbMutex.Unlock()
//	_, err := updateMaxOnlineCountStmt.ExecContext(ctx, onlineCount, liveID, onlineCount)
//	checkErr(err)
//}

// 查询liveID的数据是否存在
func queryExist(ctx context.Context, liveID string) bool {
	dbMutex.RLock()
	defer dbMutex.RUnlock()
	var uid int
	err := selectLiveIDStmt.QueryRowContext(ctx, liveID).Scan(&uid)
	return err == nil
}

// 查询所有duration为0的liveId
func queryDurationZero(ctx context.Context) ([]string, error) {
	dbMutex.RLock()
	defer dbMutex.RUnlock()
	rows, err := selectDurationZeroStmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	var liveIds []string
	var liveId string
	for rows.Next() {
		err := rows.Scan(&liveId)
		if err != nil {
			return nil, err
		}
		liveIds = append(liveIds, liveId)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return liveIds, nil
}
