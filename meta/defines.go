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

var EventDesc = map[string]string{
	"NT_CREATE":   "创建",
	"NT_DELETE":   "删除",
	"NT_UPDATE":   "更新",
	"NT_RF":       "重命名",
	"NT_RT":       "重命名",
	"NT_SCANNING": "检测",
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

var (
	OneDay                = 24 * time.Hour
	RuntimeIOBackup       = "file-io-backup"         // 运行时文件IO标记
	RuntimeSnapshot       = "snapshot"               // 运行时快照创建
	TriggerMan      int64 = 1                        // CDP 触发方式， 默认手动
	TimeFMT               = "2006-01-02 15:04:05"    // 通用的时间格式化串
	Sep                   = string(os.PathSeparator) // 路径分割符
	SplitFlag             = "@JRSA@"                 // 字符串拼接的断点标记
)

// 快照类型
const (
	SnapTypeLVM = "lvm"
	SnapTypeVSS = "vss"
)

// 卷影副本类型
const (
	DataVolumeRollback = "DataVolumeRollback"
	ClientAccessible   = "ClientAccessible"
)

// 基于快照做恢复的三种恢复模式
const (
	RevertSnap2TargetNewDrive = "revert_2_target_new_drive"
	RevertSnap2TargetOldDrive = "revert_2_target_old_drive"
	RevertSnap2OriginOldDrive = "revert_2_origin_old_drive"
	RevertSnap2OriginNewDrive = "revert_2_origin_new_drive"
)
