package meta

import (
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

type AppSettings struct {
	Name        string `json:"Name"`
	DisplayName string `json:"DisplayName"`
	Description string `json:"Description"`
	ServicePort int64  `json:"Port"`
	Mode        string `json:"Mode"`
	WorkDir     string `json:"WorkDir"`
	Log         string `json:"Log"`
}

func init() {
	// loading
	cfgDir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic("获取执行路径失败，服务启动失败")
	}
	config, err := os.Open(filepath.Join(cfgDir, "config.json"))
	if err != nil {
		panic("缺失配置文件，服务启动失败")
	}
	defer config.Close()
	cs, err := ioutil.ReadAll(config)
	if err != nil {
		panic("读取配置文件失败，服务启动失败")
	}
	err = json.Unmarshal(cs, &ConfigSettings)
	if err != nil {
		panic("加载配置失败，服务启动失败")
	}

	// APP config
	AppIsDebugMode = ConfigSettings.Mode == "debug"
	ServerIPs = filepath.Join(ConfigSettings.WorkDir, `server.ips`)
	HandlerBaseDir = filepath.Join(ConfigSettings.WorkDir, `handles`)

	// SSL
	SSLCrt = filepath.Join(ConfigSettings.WorkDir, `cdp.crt`)
	SSLKey = filepath.Join(ConfigSettings.WorkDir, `cdp.key`)
	CRT, err := ioutil.ReadFile(SSLCrt)
	if err != nil {
		fmt.Println(err)
		panic("加载SSL证书失败")
	}
	Pool.AppendCertsFromPEM(CRT)

	// mountpoint
	if IsWin {
		MountPoints = filepath.Join(ConfigSettings.WorkDir, "exposes")
		_ = os.MkdirAll(MountPoints, DefaultFileMode)
	}
}

const (
	DefaultAppReadTimeout  = 60 * time.Second
	DefaultAppWriteTimeout = 60 * time.Second
	DatabaseDriverTemplate = "host=%s user=postgres password=postgres dbname=studio port=5432 sslmode=disable TimeZone=Asia/Shanghai" // 数据库配置

	WinShareUser = "RunstorShare"
	WinSharePwd  = "qX4kUeFc"

	DefaultDRQueueSize           = 10000000                                  // 文件备份/恢复通道的默认缓冲区大小
	DefaultRetryTimeInterval     = 5 * time.Second                           // 重试间隔时间
	DefaultEnumPathChannelSize   = 10                                        // 基于扫描时枚举路径的默认通道缓存
	DefaultTransferRetryTimes    = 5                                         // 上传/下载文件默认的重试次数
	DefaultReloadStartDuration   = 5 * time.Second                           // 组件重启后，多久开始拉起失败任务
	DefaultReportProcessSecs     = 2 * time.Second                           // 每隔多久时间上报一次已备数据量
	DefaultCloseWalkerInterval   = 5 * time.Second                           // 每隔多久时间检查一次是否需要关闭所有枚举器
	DefaultWalkerCores           = 4                                         // 路径枚举器的默认并发枚举线程数
	DefaultTailEventHandleSecs   = 3 * time.Second                           // 尾更新事件最多等待多少时间
	DefaultMonitorRestoreHang    = 5 * time.Second                           // 每隔多少秒监控一次恢复任务是否完成
	MaxTimeoutWhenFullEnd        = 3 * time.Hour                             // 全量备份临近结束时，进入CDP状态的最大超时时间
	CycleQuerySecsWhenFullEnd    = 30 * time.Second                          // 全量备份临近结束时，每隔多久时间检测一次全量任务是否完成
	RestoreErrFixStatusRetrySecs = 10 * time.Second                          // 恢复任务失败时，修正任务状态的重试间隔时间
	DefaultFileMode              = 0666                                      // 默认文件权限
	IgnoreFlag                   = "IGNORE_a4b4f7ec876842618aaaeeca85766096" // 同步流程忽略带有标记
)

var (
	ConfigSettings AppSettings
	IsWin          = runtime.GOOS == "windows" // 系统平台
	ServerIPs      string                      // 备份服务器IP地址信息（可能存在多个）
	HandlerBaseDir string                      // 备份过程的锁文件目录
	SSLCrt         string
	SSLKey         string
	MountPoints    string
	AppIsDebugMode = false
	Pool           = x509.NewCertPool()
)
