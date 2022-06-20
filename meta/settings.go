package meta

import (
	"os"
	"runtime"
	"time"
)

const (
	DefaultAppPort         = 5111
	DefaultAppReadTimeout  = 60 * time.Second
	DefaultAppWriteTimeout = 60 * time.Second
	DefaultAppMode         = "release"
	DatabaseDriverTemplate = "host=%s user=postgres password=postgres dbname=studio port=5432 sslmode=disable TimeZone=Asia/Shanghai" // 数据库配置

	DefaultDRQueueSize           = 100                                       // 文件备份/恢复通道的默认缓冲区大小
	DefaultRetryTimeInterval     = 5 * time.Second                           // 重试间隔时间
	DefaultEnumPathChannelSize   = 10                                        // 基于扫描时枚举路径的默认通道缓存
	DefaultTransferRetryTimes    = 5                                         // 上传/下载文件默认的重试次数
	DefaultReloadStartDuration   = 5 * time.Second                           // 组件重启后，多久开始拉起失败任务
	DefaultReportProcessSecs     = 5 * time.Second                           // 每隔多久时间上报一次已备数据量
	DefaultCloseWalkerInterval   = 5 * time.Second                           // 每隔多久时间检查一次是否需要关闭所有枚举器
	DefaultWalkerCores           = 4                                         // 路径枚举器的默认并发枚举线程数
	DefaultTailEventHandleSecs   = 5 * time.Second                           // 尾更新事件最多等待多少时间
	DefaultMonitorRestoreHang    = 5 * time.Second                           // 每隔多少秒监控一次恢复任务是否完成
	RestoreErrFixStatusRetrySecs = 10 * time.Second                          // 恢复任务失败时，修正任务状态的重试间隔时间
	DefaultFileMode              = 0666                                      // 默认文件权限
	IgnoreFlag                   = "IGNORE_a4b4f7ec876842618aaaeeca85766096" // 同步流程忽略带有标记
	RuntimeIOBackup              = "file-io-backup"                          // 运行时文件IO标记
	TriggerMan                   = 1                                         // CDP 触发方式， 默认手动
	ServerIPsWin                 = `C:\rongan\rongan-fnotify\server.ips`     // 备份服务器IP地址信息（可能存在多个）
	ServerCtxWin                 = `C:\rongan\rongan-fnotify\server_ctx.db`  // 备份过程出错，记录错误上下文的文件
	HandlerBaseDir               = `C:\rongan\rongan-fnotify\handles\`       // 备份过程的锁文件目录

	TimeFMT   = "2006-01-02 15:04:05"    // 通用的时间格式化串
	Sep       = string(os.PathSeparator) // 路径分割符
	SplitFlag = "@JRSA@"                 // 字符串拼接的断点标记
)

var (
	IsWin = runtime.GOOS == "windows" // 系统平台
)
