package meta

import (
	"os"
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

var FFStatusMsg = map[string]string{
	FFStatusWatched:  "捕捉变更",
	FFStatusSyncing:  "正在同步",
	FFStatusError:    "同步错误",
	FFStatusFinished: "同步完成",
}

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
const ServerIPsWin = `C:\rongan\server.ips`
