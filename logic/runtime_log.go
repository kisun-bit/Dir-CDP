package logic

import (
	"fmt"
	"gorm.io/gorm"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/models"
)

// 日志级别
const (
	LogLevelTrace = iota
	LogLevelDebug
	LogLevelInfo
	LogLevelWarn
	LogLevelError
	LogLevelFatal
)

// 日志任务状态
const (
	LogStatusRun     string = "R"
	LogStatusSuccess string = "S"
	LogStatusFailed  string = "F"
	LogStatusWarn    string = "W"
	LogStatusCancel  string = "C"
	LogStatusUnknown string = "O"
)

// CDP备份期间、恢复期间的状态集
const (
	StepInitCDP                    = "初始化CDP组件"
	StepInitArgs                   = "加载备份参数"
	StepInitArgsF                  = "加载备份参数失败，原因：%v"
	StepConnDB                     = "连接业务数据库"
	StepMallocHandle               = "分配任务句柄锁"
	StepMallocHandleF              = "分配任务句柄锁失败，原因：%v"
	StepLockHandle                 = "锁定任务句柄锁"
	StepLockHandleF                = "锁定任务句柄锁失败，原因：%v"
	StepUnLockHandle               = "释放任务句柄锁"
	StepUnLockHandleF              = "释放任务句柄锁失败，原因：%v"
	StepStartLogic                 = "执行核心业务逻辑"
	StepStartFullAndIncrWhenReload = "经检测，CDP服务存在中断时间，开始智能分析并备份中断期间的新文件"
	StepStartFullAndIncrWhenNor    = "经检测，此任务为正常发起，开始备份基础数据及持续监控更新事件"
	StepStartIncrWhenNor           = "经检测，此任务为正常发起，开始持续监控更新事件"
	StepStartMonitor               = "启用任务中断控制器，持续捕捉中断异常事件"
	StepClearFailedHistory         = "清理过期事件集"
	StepConsumeFullQueue           = "开始传输全量基础数据"
	StepConsumeIncrQueue           = "开始传输监控事件数据"
	StepEndFullTransfer            = "中断全量基础数据传输"
	StepStartWalkers               = "启用多路径枚举器，扫描源目录集"
	StepEndWalkers                 = "扫描源目录集完成"
	StepStartWatchers              = "启用多路径文件事件监控器，持续监控更新事件"
	StepEndWatchers                = "文件事件监控器检测到内部异常，已退出"
	StepCloseFullQueue             = "关闭全量扫描通道"
	StepCloseIncrQueue             = "关闭监控事件通道"
	StepAlreadyCDPing              = "任务状态不做变更，已处于持续保护状态（CDPING）"
	StepCopying2CDPing             = "任务由基础数据同步状态(COPYING)进入持续保护状态(CDPING)"
	StepUnStart2Copying            = "任务由未启用状态(UNSTART)进入基础数据同步状态(COPYING)"
	StepCDPing2Copying             = "任务由持续保护状态(CDPING)进入基础数据同步状态(COPYING)"
	StepConfigEnable               = "应用配置"
	StepConfigDisable              = "禁用配置"
	StepExitNormal                 = "任务中断完成（因禁用配置、句柄锁定失败导致的中断不予重试）"
	StepExitOccur                  = "检测到内部异常（%v）"
	StepTaskHang                   = "任务正在中断"
	StepReloadCDPFinish            = "重启CDP组件完成，等待系统调度"
	StepResetCDP                   = "重置CDP组件完成，等待重试"
	StepRetryCDPFinish             = "重试完成，开始执行任务"
	StepRetryCDPMatchSer           = "重试失败，组件运行时未记录备份服务器IP，导致重连数据库异常"
	StepServerConnErr              = "备份服务器与客户端之间网络连接异常，正在重试...，请稍后"
	StepFileScanUpS                = "基于扫描，文件%s完成同步"
	StepFileEventUpS               = "基于事件，文件%s完成同步"
	StepFileScanUpF                = "基于扫描，文件%s同步失败，原因：%v"
	StepFileEventUpF               = "基于事件，文件%s同步失败，原因：%v"
	StepFileEventDel               = "基于事件，远程文件%s完成删除"
	StepFileEventDelF              = "基于事件，源文件%v的远程文件删除失败，原因：%v"
	StepInitRestoreTask            = "初始化恢复任务"
	StepLoadArgs                   = "加载备份参数"
	StepLoadArgsF                  = "加载备份参数失败，原因：%v"
	StepInitRestorePool            = "初始化恢复工作池"
	StepInitRestorePoolF           = "初始化恢复工作池失败，原因：%v"
	StepRestoreByFileset           = "依据指定文件集恢复"
	StepRestoreByTime              = "未指定文件集恢复，将匹配过滤参数执行恢复"
	StepStartTransfer              = "正在传输数据"
	StepEndTransfer                = "数据传输完毕"
	StepClearTask                  = "清理并退出任务"
)

type Reporter struct {
	DB   *gorm.DB
	Conf int64
	Task int64
	Type meta.TaskType
}

func NewReporter(db *gorm.DB, conf, task int64, type_ meta.TaskType) *Reporter {
	return &Reporter{
		DB:   db,
		Conf: conf,
		Task: task,
		Type: type_,
	}
}

func (re *Reporter) Str() string {
	return fmt.Sprintf("<Reporter(Conf=%v, Task=%v, Type=%v)>", re.Conf, re.Task, re.Type)
}

func (re *Reporter) ReportInfo(format string, a ...interface{}) (err error) {
	return re.info("", format, true, a...)
}

func (re *Reporter) ReportError(format string, a ...interface{}) (err error) {
	return re.error("", format, true, a...)
}

func (re *Reporter) ReportInfoWithoutLogWithKey(key, format string, a ...interface{}) (err error) {
	return re.info(key, format, false, a...)
}

func (re *Reporter) ReportErrWithoutLogWithKey(key, format string, a ...interface{}) (err error) {
	return re.error(key, format, false, a...)
}

func (re *Reporter) info(key, format string, log bool, a ...interface{}) (err error) {
	message := fmt.Sprintf(format, a...)
	if log {
		logger.Fmt.Infof("%s.info >>>>>> %s", re.Str(), message)
	}
	return models.NewLog(
		re.DB, re.Conf, re.Task,
		string(re.Type), key, LogStatusRun, message, "{}", LogLevelInfo)
}

func (re *Reporter) error(key, format string, log bool, a ...interface{}) (err error) {
	message := fmt.Sprintf(format, a...)
	if log {
		logger.Fmt.Infof("%s.error >>>>>> %s", re.Str(), message)
	}
	return models.NewLog(
		re.DB, re.Conf, re.Task,
		string(re.Type), key, LogStatusRun, message, "{}", LogLevelInfo)
}
