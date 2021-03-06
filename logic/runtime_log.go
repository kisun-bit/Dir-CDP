package logic

import (
	"fmt"
	"github.com/thoas/go-funk"
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

var AllLevels = []int{LogLevelTrace, LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError, LogLevelFatal}

func DescribeLogLevel(level int) string {
	switch level {
	case LogLevelTrace:
		return "trace"
	case LogLevelDebug:
		return "debug"
	case LogLevelInfo:
		return "info"
	case LogLevelWarn:
		return "warn"
	case LogLevelError:
		return "error"
	case LogLevelFatal:
		return "fatal"
	default:
		return "unknown"
	}
}

// 日志任务状态
const (
	LogStatusRun string = "R"
	// TODO 后续支持日志中提现任务的执行状态
	//LogStatusSuccess string = "S"
	//LogStatusFailed  string = "F"
	//LogStatusWarn    string = "W"
	//LogStatusCancel  string = "C"
	//LogStatusUnknown string = "O"
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
	StepFileScanUpS                = "基于%v事件，文件%s完成同步"
	StepFileEventUpS               = "基于%v事件，文件%s完成同步"
	StepFileScanUpF                = "基于%v事件，文件%s同步失败，原因：%v"
	StepFileEventUpF               = "基于%v事件，文件%s同步失败，原因：%v"
	StepFileEventDel               = "基于%v事件，远程文件%s完成删除"
	StepFileEventDelF              = "基于%v事件，源文件%v的远程文件删除失败，原因：%v"
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
	StepStartSnapCreator           = "启用定时快照例程"
	StepScanDel                    = "检测中断期间的删除事件"
	StepCreateSnapshot             = "对驱动器%v创建快照%v成功"
	StepCreateSnapshotErr          = "对驱动器%v创建快照失败，原因：%v"
	StepCreateVersion              = "整合快照，创建副本点%v完成"
	StepCreateVersionErr           = "整合快照，创建副本点%v失败，原因：%v"
	StepDeleteSnapshot             = "对驱动器%v删除快照%v成功"
	StepDeleteSnapshotErr          = "对驱动器%v删除快照失败，原因：%v"
	StepRecycleVersion             = "销毁快照，回收副本点%v完成"
	StepRecycleVersionErr          = "销毁快照，回收副本点%v失败，原因：%v"
	StepReceiveRestoreInstruct     = "接收到版本(%v)恢复指令"
	StepLoadAllSnapshotsConfig     = "载入版本(%v)下所有卷快照集"
	StepMatchDirFromSnapshot       = "处理源目录集与快照集映射关系"
	StepLoadSnapshot               = "导出快照数据至临时挂载点(%v)"
	StepRevertShadowCopy           = "正在回滚快照%v至%v:"
	StepRevertOneDir               = "正在从临时挂载点%v回滚目录%v，处理差异及增量数据"
	StepFinishRestore              = "完成版本(%v)恢复"
	StepRevertErr                  = "版本(%v)恢复失败，原因：%v"
	StepHandleSnapshot             = "正在处理快照%v(SnapID=%v)"
	StepStartExposeLocal           = "开始导出快照%v至本地挂载点"
	StepEndExposeLocal             = "导出快照%v至本地挂载点%v完成"
	StepCreateShare                = "基于快照挂载点%v创建网络共享\\\\%v\\%v"
	StepLackLettersNetConn         = "恢复完成，使用\\\\%v\\%v地址访问恢复文件(用户名：%v 密码：%v)"
)

type Reporter struct {
	DB     *gorm.DB
	Conf   int64
	Task   int64
	Type   meta.TaskType
	Levels []int
}

func NewReporter(db *gorm.DB, conf, task int64, type_ meta.TaskType, levels []int) *Reporter {
	return &Reporter{
		DB:     db,
		Conf:   conf,
		Task:   task,
		Type:   type_,
		Levels: levels,
	}
}

func (re *Reporter) Str() string {
	return fmt.Sprintf("<Reporter(Conf=%v, Task=%v, Type=%v)>", re.Conf, re.Task, re.Type)
}

func (re *Reporter) isValid(level int) bool {
	return funk.InInts(re.Levels, level)
}

func (re *Reporter) ReportTrace(format string, a ...interface{}) (err error) {
	return re.trace("", format, true, a...)
}

func (re *Reporter) ReportInfo(format string, a ...interface{}) (err error) {
	return re.info("", format, true, a...)
}

func (re *Reporter) ReportWarn(format string, a ...interface{}) (err error) {
	return re.warn("", format, true, a...)
}

func (re *Reporter) ReportError(format string, a ...interface{}) (err error) {
	return re.error("", format, true, a...)
}

func (re *Reporter) ReportFatal(format string, a ...interface{}) (err error) {
	return re.fatal("", format, true, a...)
}

func (re *Reporter) ReportTraceWithoutLogWithKey(key, format string, a ...interface{}) (err error) {
	return re.trace(key, format, false, a...)
}

func (re *Reporter) ReportDebugWithoutLogWithKey(key, format string, a ...interface{}) (err error) {
	return re.debug(key, format, false, a...)
}

func (re *Reporter) ReportInfoWithoutLogWithKey(key, format string, a ...interface{}) (err error) {
	return re.info(key, format, false, a...)
}

func (re *Reporter) ReportWarnWithoutLogWithKey(key, format string, a ...interface{}) (err error) {
	return re.warn(key, format, false, a...)
}

func (re *Reporter) ReportErrorWithoutLogWithKey(key, format string, a ...interface{}) (err error) {
	return re.error(key, format, false, a...)
}

func (re *Reporter) ReportFatalWithoutLogWithKey(key, format string, a ...interface{}) (err error) {
	return re.fatal(key, format, false, a...)
}

func (re *Reporter) trace(key, format string, log bool, a ...interface{}) (err error) {
	return re._log(LogLevelTrace, key, format, log, a...)
}

func (re *Reporter) debug(key, format string, log bool, a ...interface{}) (err error) {
	return re._log(LogLevelDebug, key, format, log, a...)
}

func (re *Reporter) info(key, format string, log bool, a ...interface{}) (err error) {
	return re._log(LogLevelInfo, key, format, log, a...)
}

func (re *Reporter) warn(key, format string, log bool, a ...interface{}) (err error) {
	return re._log(LogLevelWarn, key, format, log, a...)
}

func (re *Reporter) error(key, format string, log bool, a ...interface{}) (err error) {
	return re._log(LogLevelError, key, format, log, a...)
}

func (re *Reporter) fatal(key, format string, log bool, a ...interface{}) (err error) {
	return re._log(LogLevelFatal, key, format, log, a...)
}

func (re *Reporter) _log(level int, key, format string, log bool, a ...interface{}) (err error) {
	if !re.isValid(level) {
		return nil
	}
	message := fmt.Sprintf(format, a...)
	if log {
		logger.Fmt.Infof("%s.%s >>>>>> %s", re.Str(), DescribeLogLevel(level), message)
	}
	return models.NewLog(
		re.DB, re.Conf, re.Task,
		string(re.Type), key, LogStatusRun, message, "{}", level)
}
