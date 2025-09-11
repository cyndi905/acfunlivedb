package main

import (
	"context"
	"database/sql"
	"log"

	_ "modernc.org/sqlite"
)

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

const scanStreamerToTable = `INSERT INTO streamer (uid, name)
SELECT uid, name
FROM acfunlive AS a
WHERE startTime = (
    SELECT MAX(startTime)
    FROM acfunlive AS b
    WHERE a.uid = b.uid AND a.name = b.name
)
ON CONFLICT (uid)
DO UPDATE SET name = excluded.name
WHERE streamer.name != excluded.name`

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
	_, err := scanStreamerStmt.ExecContext(ctx)
	if err == nil {
		log.Println("已更新主包表")
	}
	checkErr(err)
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
