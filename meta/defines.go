package meta

import (
	"os"
	"time"
)

// 任务类型
type TaskType string

const (
	TaskTypeBackup  TaskType = "b"
	TaskTypeRestore TaskType = "r"
)

// FileType文件类型
type FileType uint8

const (
	FTFile  FileType = 1 << 0
	FTDir   FileType = 1 << 1
	FTLink  FileType = 1 << 2
	FTOther FileType = 1 << 3
)

// Event触发内核文件系统监控的事件类型
type Event uint32

const (
	Win32EventCreate Event = iota + 1
	Win32EventDelete
	Win32EventUpdate
	Win32EventRenameFrom
	Win32EventRenameTo
	Win32EventFullScan
)

var EventCode = map[Event]string{
	Win32EventCreate:     "NT_CREATE",
	Win32EventDelete:     "NT_DELETE",
	Win32EventUpdate:     "NT_UPDATE",
	Win32EventRenameFrom: "NT_RF",
	Win32EventRenameTo:   "NT_RT",
	Win32EventFullScan:   "NT_SCANNING",
}

// 监控同步配置的目标类型
const (
	WatchingConfTargetS3   = "S3"
	WatchingConfTargetHost = "HOST"
)

// 文件流水对象的状态
const (
	FFStatusWatched  = "WATCHED"
	FFStatusSyncing  = "SYNCING"
	FFStatusError    = "ERROR"
	FFStatusFinished = "FINISHED"
)

// 恢复任务的状态
const (
	RESTORESTART  = "START"
	RESTOREFINISH = "FINISHED"
	RESTORESERROR = "ERROR"
	RESTOREING    = "RUNNING"
)

// CDP备份任务的状态
const (
	CDPUNSTART = "UNSTART"
	CDPCOPYING = "COPYING"
	CDPCDPING  = "CDPING"
)

// 路径分割符
const Sep = string(os.PathSeparator)

// CDP 触发方式
const TriggerMan = 1

// 字符串拼接的断点标记
const SplitFlag = "@jrsa@"

// 备份服务器IP地址信息（可能存在多个）
const ServerIPsWin = `C:\rongan\rongan-fnotify\server.ips`

// 备份过程出错，记录错误上下文的文件
const ServerCtxWin = `C:\rongan\rongan-fnotify\server_ctx.db`

// 备份过程的锁文件目录
const HandlerBaseDir = `C:\rongan\rongan-fnotify\handles\`

// 文件备份/恢复通道的默认缓冲区大小
const DefaultDRQueueSize = 100

// 重试间隔时间
var DefaultRetryTimeInterval = 5 * time.Second

// 基于扫描时枚举路径的默认通道缓存
const DefaultEnumPathChannelSize = 10

const (
	UnsetInt = -1
	UnsetStr = ""
)

// 指定时间段备份的时间区间类型
const (
	TimeStrategyPre     = "pre"
	TimeStrategyNext    = "next"
	TimeStrategyCenter  = "center"
	TimeStrategyNoLimit = "none"
)

var OneDay = 24 * time.Hour
