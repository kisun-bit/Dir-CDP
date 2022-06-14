package meta

import (
	"runtime"
	"time"
)

// 忽略带有标记
const IgnoreFlag = "IGNORE_a4b4f7ec876842618aaaeeca85766096"

// 系统平台
var IsWin = runtime.GOOS == "windows"

// 通用的时间格式化串
const TimeFMT string = "2006-01-02 15:04:05"

// 数据库配置
const DatabaseDriverTemplate = "host=%s user=postgres password=postgres dbname=studio port=5432 sslmode=disable TimeZone=Asia/Shanghai"

// 服务器配置
const (
	AppPort         = 5111
	AppReadTimeout  = 60 * time.Second
	AppWriteTimeout = 60 * time.Second
	AppMode         = "release"
)

// 备份文件通道大小（不涉及上传/下载操作）
const BackupChanSize = 200

// 备份文件池大小（上传和下载）
var BackupPoolSize = runtime.NumCPU()
