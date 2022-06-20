package meta

import "time"

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

func (e Event) Str() string {
	return EventCode[e]
}

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
