// @Title  file_cdp.go
// @Description  实现文件级CDP功能
// @Author  Kisun
// @Update  2022-6-13
package logic

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asmcos/requests"
	"github.com/boltdb/bolt"
	"github.com/gofrs/flock"
	"github.com/gofrs/uuid"
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/aws/session"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"github.com/journeymidnight/aws-sdk-go/service/s3/s3manager"
	gos3 "github.com/kisun-bit/go-s3"
	"github.com/kr/pretty"
	"github.com/panjf2000/ants/v2"
	"github.com/thoas/go-funk"
	"io"
	"io/ioutil"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/models"
	"jingrongshuan/rongan-fnotify/nt_notify"
	"jingrongshuan/rongan-fnotify/tools"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// fullBackupProxy 全量备份代理
// 支持基于扫描的多目录递归备份
type fullBackupProxy struct {
	walkers              []*Walker
	counter              *int32 // 计数器信号， 目的是等待所有目录枚举完毕后，关闭枚举通道
	walkerQueue          chan nt_notify.FileWatchingObj
	walkerQueueCloseOnce *sync.Once
	fullPool             *ants.Pool
	fullWG               *sync.WaitGroup
	fullQueue            chan models.EventFileModel // 全量数据通道，该通道在全量备份完成后关闭
	fullQueueCloseOnce   *sync.Once
}

func initFullBackupProxy(poolSize, fullQueueSize int) *fullBackupProxy {
	fbp := new(fullBackupProxy)
	fbp.fullPool, _ = ants.NewPool(poolSize)
	fbp.fullWG = new(sync.WaitGroup)
	fbp.fullQueue = make(chan models.EventFileModel, fullQueueSize)
	fbp.walkerQueue = make(chan nt_notify.FileWatchingObj, meta.DefaultEnumPathChannelSize)
	fbp.walkerQueueCloseOnce = new(sync.Once)
	fbp.counter = new(int32)
	fbp.fullQueueCloseOnce = new(sync.Once)
	return fbp
}

// incrBackupProxy 增量备份代理
type incrBackupProxy struct {
	watchers           []*watcherWrapper
	incrPool           *ants.Pool
	incrWG             *sync.WaitGroup
	incrQueue          chan models.EventFileModel // 增量数据通道，该通道永久开放
	incrQueueCloseOnce *sync.Once
}

type watcherWrapper struct {
	watcher      *nt_notify.Win32Watcher
	watcherPatch *eventDelayPusher // 用于对装载变更事件的通道WatcherQueue做相邻去重处理以及尾元素处理
	watcherQueue chan nt_notify.FileWatchingObj
}

func initIncrBackupProxy(poolSize, incrQueueSize int) *incrBackupProxy {
	ibp := new(incrBackupProxy)
	ibp.incrPool, _ = ants.NewPool(poolSize)
	ibp.incrWG = new(sync.WaitGroup)
	ibp.incrQueue = make(chan models.EventFileModel, incrQueueSize)
	ibp.incrQueueCloseOnce = new(sync.Once)
	return ibp
}

// storage 目标存储
type storage struct {
	// 以下属性仅在目标存储为主机时有效
	uploadSession string
	deleteSession string
	renameSession string
	// 一下属性仅在目标存储为对象存储时有效
	s3Client  gos3.S3Client
	s3Session *session.Session
}

// CDPExecutor 文件级CDP实现
type CDPExecutor struct {
	isReload      bool                    // 当服务重启时，此属性为True
	isRetry       bool                    // 当任务重试时，此属性为True
	startTs       int64                   // 本次备份的开始时间
	handle        string                  // 任务句柄
	taskLocker    *flock.Flock            // 任务句柄锁(文件锁)
	DBDriver      *models.DBProxy         // 数据库驱动器
	confObj       *models.ConfigModel     // 配置对象
	taskObj       *models.BackupTaskModel // 任务对象
	reporter      *Reporter               // 日志上报器
	snapCreator   *SnapCreator            // 快照定时创建
	logRecycle    *LogRecycle             // 日志清理
	progress      *Progress               // 数据量、文件数完成进度上报器
	listFilter    *Grep                   // 黑白名单过滤器
	fbp           *fullBackupProxy        // 全备代理
	ibp           *incrBackupProxy        // 增量代理
	storage       *storage                // 目标存储信息
	monitor       *hangEventMonitor       // 任务中断捕捉器
	hangSignal    *int32                  // 捕捉用户禁用/取消、任务异常终止的信号
	notifyErr     chan ErrorCode          // 异常记录
	notifyErrOnce *sync.Once              // 并发环境下仅报告一次异常记录
	localCtxDB    *bolt.DB                // TODO 本地用于记录服务关键信息的文件数据库
}

func initOrResetCDPExecutor(ce *CDPExecutor, config *models.ConfigModel, dp *models.DBProxy) (err error) {
	ce.confObj = config
	ce.DBDriver = dp

	numCores := int(config.Cores)
	if numCores < 0 {
		numCores = runtime.NumCPU()
	}

	ce.fbp = initFullBackupProxy(numCores, meta.DefaultDRQueueSize)
	ce.ibp = initIncrBackupProxy(numCores, meta.DefaultDRQueueSize)

	ce.storage = new(storage)
	ce.startTs = time.Now().Unix()
	ce.monitor = NewMonitorWithBackup(ce)

	ce.hangSignal = new(int32)
	ce.notifyErr = make(chan ErrorCode)
	ce.notifyErrOnce = new(sync.Once)
	return nil
}

func NewCDPExecutor(config *models.ConfigModel, dp *models.DBProxy) (ce *CDPExecutor, err error) {
	ce = new(CDPExecutor)
	if err = initOrResetCDPExecutor(ce, config, dp); err != nil {
		return
	}
	logger.Fmt.Infof("NewCDPExecutor. init CDPExecutor(Conf=%v) is ok", config.ID)
	return
}

func NewCDPExecutorWhenReload(config *models.ConfigModel, dp *models.DBProxy) (ce *CDPExecutor, err error) {
	task, err := models.QueryBackupTaskByConfID(dp.DB, config.ID)
	if err != nil {
		return nil, fmt.Errorf("reload conf(id=%v) is NULL", config.ID)
	}

	ce, err = NewCDPExecutor(config, dp)
	if err != nil {
		return
	}

	ce.taskObj = &task
	ce.isReload = true
	return
}

func LoadCDP(ip string, conf int64, reload bool) (err error) {
	var (
		dp     *models.DBProxy
		cdp    *CDPExecutor
		config models.ConfigModel
	)
	dp, err = models.NewDBProxyWithInit(ip)
	if err != nil {
		return
	}
	config, err = models.QueryConfigByID(dp.DB, conf)
	if err != nil {
		return err
	}
	logger.Fmt.Infof("LoadCDP IP(%s) ConfID(%v) matched", ip, conf)

	if !reload {
		if config.Enable {
			return errors.New("already enabled")
		}
		cdp, err = NewCDPExecutor(&config, dp)
	} else {
		cdp, err = NewCDPExecutorWhenReload(&config, dp)
	}
	if err != nil {
		return
	}

	if !cdp.isReload {
		if err = cdp.initTaskOnce(); err != nil {
			return
		}
		if err = models.EnableConfig(dp.DB, conf); err != nil {
			return
		}
	}

	if err = cdp.confObj.LoadsJsonFields(cdp.DBDriver.DB); err != nil {
		return
	}
	logger.Fmt.Infof("LoadCDP CONFIG is:\n%v", pretty.Sprint(cdp.confObj))

	cdp.reporter = NewReporter(
		cdp.DBDriver.DB,
		cdp.confObj.ID,
		cdp.taskObj.ID,
		meta.TaskTypeBackup,
		cdp.confObj.ExtInfoJson.LogKeepPolicy.Levels)
	cdp.snapCreator = &SnapCreator{
		config:   cdp.confObj,
		task:     cdp.taskObj,
		db:       cdp.DBDriver.DB,
		reporter: cdp.reporter,
		stop:     false,
	}

	_ = cdp.reporter.ReportInfo(StepConfigEnable)
	if !cdp.isReload {
		_ = cdp.reporter.ReportInfo(StepInitCDP)
	} else {
		_ = cdp.reporter.ReportInfo(StepReloadCDPFinish)
	}
	if err = cdp.registerShardingModel(); err != nil {
		return
	}
	if err = cdp.StartWithRetry(); err != nil {
		return
	}
	return nil
}

func (c *CDPExecutor) Str() string {
	return fmt.Sprintf("<CDP(conf=%v, task=%v, reload=%v, retry=%v)>",
		c.confObj.ID, c.taskObj.ID, c.isReload, c.isRetry)
}

func (c *CDPExecutor) registerShardingModel() (err error) {
	if err = c.DBDriver.RegisterEventFileModel(c.confObj.ID); err != nil {
		logger.Fmt.Errorf("%v.StartWithRetry RegisterEventFileModel ERR=%v", c.Str(), err)
		return
	}
	if err = c.DBDriver.RegisterEventDirModel(c.confObj.ID); err != nil {
		logger.Fmt.Errorf("%v.StartWithRetry RegisterEventDirModel ERR=%v", c.Str(), err)
		return
	}
	return nil
}

func (c *CDPExecutor) inRetryErr(ec ErrorCode) bool {
	return ec == ExitErrCodeUserCancel ||
		ec == ExitErrCodeTaskReplicate ||
		ec == ExitErrCodeLackHandler
}

func (c *CDPExecutor) StartWithRetry() (err error) {
	if err = c.moreInit(); err != nil {
		_ = c.reporter.ReportError(StepInitArgsF, err)
		logger.Fmt.Errorf("%v.StartWithRetry moreInit err=%v", c.Str(), err)
		return
	}
	_ = c.reporter.ReportInfo(StepInitArgs)

	// 无限重试策略
	go func() {
		for {
			if c.confObj.ExtInfoJson.ServerAddress == meta.UnsetStr {
				_ = c.reporter.ReportError(StepRetryCDPMatchSer)
				break
			}
			if c.DBDriver, err = models.NewDBProxyWithInit(c.confObj.ExtInfoJson.ServerAddress); err != nil {
				logger.Fmt.Infof("%v.StartWithRetry. will sleep 5s...", c.Str())
			} else {
				_ = c.reporter.ReportInfo(StepConnDB)
				if c.isRetry == true {
					_ = c.reporter.ReportInfo(StepRetryCDPFinish)
				}
				if c.inRetryErr(c.Start()) {
					_ = c.reporter.ReportInfo(StepExitNormal)
					break
				}
			}
			c.isRetry = true
			time.Sleep(meta.DefaultRetryTimeInterval)
		}
	}()
	return nil
}

func (c *CDPExecutor) Start() (ec ErrorCode) {
	if err := c.mallocHandle(); err != nil {
		logger.Fmt.Errorf("%v.mallocHandle ERR=%v", c.Str(), err)
		_ = c.reporter.ReportError(StepMallocHandleF, err)
		return ExitErrCodeLackHandler
	}
	_ = c.reporter.ReportInfo(StepMallocHandle)

	locked, err := c.taskLocker.TryLock()
	if err != nil || !locked {
		if err == nil {
			err = errors.New("locked err")
		}
		logger.Fmt.Errorf("%v.taskLocker.TryLock ERR=%v", c.Str(), err)
		_ = c.reporter.ReportError(StepLockHandleF, err)
		return ExitErrCodeTaskReplicate
	}

	// assert locked is true
	_ = c.reporter.ReportInfo(StepLockHandle)
	defer func() {
		if err = c.taskLocker.Unlock(); err != nil {
			logger.Fmt.Errorf("%v.taskLocker.Unlock ERR=%v", c.Str(), err)
			_ = c.reporter.ReportError(StepUnLockHandleF, err)
		} else {
			_ = c.reporter.ReportInfo(StepUnLockHandle)
		}
	}()
	c.progress = NewProgress(meta.DefaultReportProcessSecs,
		c.taskObj.ID, c.DBDriver.DB, c.confObj.ExtInfoJson.ServerAddress, meta.TaskTypeBackup)
	c.logRecycle = NewLogRecycle(c.confObj.ID, c.taskObj.ID,
		c.confObj.ExtInfoJson.LogKeepPolicy.Days, c.DBDriver.DB)

	go c.monitorStopNotify()
	go c.logRecycle.Start()
	go c.progress.Gather()
	go c.logic()

	// 阻塞
	ec = <-c.notifyErr
	_ = c.reporter.ReportInfo(StepExitOccur, ErrByCode(ec))
	if ec == ExitErrCodeUserCancel {
		_ = c.reporter.ReportInfo(StepConfigDisable)
		return
	}
	if ec == ExitErrCodeServerConn {
		_ = c.reporter.ReportInfo(StepServerConnErr)
	}
	if err := initOrResetCDPExecutor(c, c.confObj, c.DBDriver); err != nil {
		return
	}
	c.isReload = true
	_ = c.reporter.ReportInfo(StepResetCDP)
	return
}

func (c *CDPExecutor) mallocHandle() (err error) {
	if c.handle, err = c.createHandle(); err != nil {
		logger.Fmt.Errorf("%v.createHandle ERR=%v", c.Str(), err)
		return
	}
	c.taskLocker = flock.New(c.handlePath(c.handle))
	return nil
}

func (c *CDPExecutor) isStopped() bool {
	return atomic.LoadInt32(c.hangSignal) == 1
}

func (c *CDPExecutor) monitorStopNotify() {
	go func() {
		c.monitor.monitor()
		_ = c.reporter.ReportInfo(StepStartMonitor)
	}()
}

func (c *CDPExecutor) stop() {
	_ = c.reporter.ReportInfo(StepTaskHang)
	if c.isStopped() {
		return
	}
	atomic.StoreInt32(c.hangSignal, 1)
	c.stopBackupProxy()
	c.progress.Stop()
	c.snapCreator.SetStop()
	c.logRecycle.SetStop()
}

func (c *CDPExecutor) stopBackupProxy() {
	for _, w := range c.fbp.walkers {
		w.SetStop()
	}
	for _, w := range c.ibp.watchers {
		w.watcher.SetStop()
	}
	c.ibp.incrQueueCloseOnce.Do(func() {
		close(c.ibp.incrQueue)
		_ = c.reporter.ReportInfo(StepCloseIncrQueue)
	})
	_ = c.reporter.ReportInfo(StepEndWatchers)
}

func (c *CDPExecutor) logic() {
	var err error
	defer c.catchErr(err)

	_ = c.reporter.ReportInfo(StepStartLogic)
	if err = c.createStorageContainer(); err != nil {
		return
	}
	if err = models.DeleteNotUploadFileFlows(c.DBDriver.DB, c.confObj.ID); err != nil {
		return
	}
	if err = models.DeleteCDPStartLogsByTime(c.DBDriver.DB, c.confObj.ID, c.startTs); err != nil {
		return
	}
	_ = c.reporter.ReportInfo(StepClearFailedHistory)

	/*启动任务方式有两种：
	1. 重载未完成任务（断网、重启、宕机、重试）
	2. 执行新任务（HTTP调用）
	*/
	if c.isReload {
		if err = c.logicWhenReload(); err != nil {
			return
		}
	} else {
		if err = c.logicWhenNormal(); err != nil {
			return
		}
	}
	c.uploadQueue2Storage()
}

func (c *CDPExecutor) createStorageContainer() (err error) {
	if c.is2s3() {
		for _, b := range c.confObj.UniqBuckets() {
			if err = c.createBucket(b); err != nil {
				return err
			}
		}
	}
	if c.is2host() {
		// do nothing
		return nil
	}
	return errors.New("failed to create storage container with unsupported storage")
}

func (c *CDPExecutor) convertCDPing2Copying() (err error) {
	if c.taskObj.Status != meta.CDPCDPING {
		return errors.New("invalid task status, not equals to `CDPING`")
	}
	if err = models.UpdateBackupTaskStatusByConfID(c.DBDriver.DB, c.confObj.ID, meta.CDPCOPYING); err != nil {
		return
	}
	_ = c.reporter.ReportInfo(StepCDPing2Copying)
	c.taskObj.Status = meta.CDPCOPYING
	return nil
}

func (c *CDPExecutor) convertUnStart2Copying() (err error) {
	if c.taskObj.Status != meta.CDPUNSTART {
		return errors.New("invalid task status, not equals to `CDPUNSTART`")
	}
	if err = models.UpdateBackupTaskStatusByConfID(c.DBDriver.DB, c.confObj.ID, meta.CDPCOPYING); err != nil {
		return
	}
	_ = c.reporter.ReportInfo(StepUnStart2Copying)
	c.taskObj.Status = meta.CDPCOPYING
	return nil
}

func (c *CDPExecutor) convertCopying2CDPing() (err error) {
	if c.taskObj.Status != meta.CDPCOPYING {
		return errors.New("invalid task status, not equals to `COPYING`")
	}
	err = models.UpdateBackupTaskStatusByConfID(c.DBDriver.DB, c.confObj.ID, meta.CDPCDPING)
	if err != nil {
		return
	}
	c.taskObj.Status = meta.CDPCDPING
	_ = c.reporter.ReportInfo(StepCopying2CDPing)
	return nil
}

func (c *CDPExecutor) logicWhenReload() (err error) {
	_ = c.deleteWhenDisableVersion()
	_ = c.reporter.ReportInfo(StepStartFullAndIncrWhenReload)
	if c.taskObj.Status == meta.CDPUNSTART {
		if err = c.convertUnStart2Copying(); err != nil {
			return
		}
	} else if c.taskObj.Status == meta.CDPCDPING {
		if err = c.convertCDPing2Copying(); err != nil {
			return
		}
	}
	logger.Fmt.Infof("%v.logicWhenReload -> FULL + INCR", c.Str())
	c.full()
	c.incr()
	return nil
}

func (c *CDPExecutor) logicWhenNormal() (err error) {
	if c.taskObj.Status == meta.CDPUNSTART || c.taskObj.Status == meta.CDPCOPYING {
		_ = c.reporter.ReportInfo(StepStartFullAndIncrWhenNor)
		if c.taskObj.Status == meta.CDPUNSTART {
			if err = c.convertUnStart2Copying(); err != nil {
				return
			}
		}
		logger.Fmt.Infof("%v.logicWhenNormal in [UNSTART、COPYING] -> FULL + INCR", c.Str())
		c.full()
		c.incr()
		return nil
	}

	if c.taskObj.Status == meta.CDPCDPING && models.IsEmptyTable(c.DBDriver.DB, c.confObj.ID) {
		_ = c.reporter.ReportInfo(StepStartFullAndIncrWhenNor)
		if err = c.convertCDPing2Copying(); err != nil {
			return
		}
		logger.Fmt.Infof("%v.logicWhenNormal in [CDPING] and empty table --> FULL + INCR", c.Str())
		c.full()
		c.incr()
		return nil
	}

	if c.taskObj.Status == meta.CDPCDPING && !models.IsEmptyTable(c.DBDriver.DB, c.confObj.ID) {
		logger.Fmt.Infof("%v.logicWhenNormal in [CDPING] and not empty table --> INCR", c.Str())
		close(c.fbp.fullQueue)
		_ = c.reporter.ReportInfo(StepCloseFullQueue)
		_ = c.reporter.ReportInfo(StepStartIncrWhenNor)
		c.incr()
		return nil
	}

	logger.Fmt.Errorf("%v.logicWhenNormal error status=%v", c.Str(), c.taskObj.Status)
	panic("undefined taskObj status")
}

func (c *CDPExecutor) moreInit() (err error) {
	c.listFilter = NewGrep(c.confObj.Include, c.confObj.Exclude)
	logger.Fmt.Infof("%v.modeInit TASK is:\n%v", c.Str(), c.taskObj.String())

	if c.is2host() {
		c.storage.uploadSession = fmt.Sprintf(
			"http://%s:%v/api/v1/upload", c.confObj.TargetHostJson.IP, c.confObj.TargetHostJson.Port)
		c.storage.deleteSession = fmt.Sprintf(
			"http://%s:%v/api/v1/delete", c.confObj.TargetHostJson.IP, c.confObj.TargetHostJson.Port)
		c.storage.renameSession = fmt.Sprintf(
			"http://%s:%v/api/v1/rename", c.confObj.TargetHostJson.IP, c.confObj.TargetHostJson.Port)
		logger.Fmt.Infof("%v.moreInit 2h session ->%v", c.Str(), c.storage.uploadSession)

	} else if c.is2s3() {
		c.storage.s3Session, c.storage.s3Client = tools.NewS3Client(
			c.confObj.S3ConfJson.AK,
			c.confObj.S3ConfJson.SK,
			c.confObj.S3ConfJson.Endpoint,
			c.confObj.S3ConfJson.Region,
			c.confObj.S3ConfJson.SSL,
			c.confObj.S3ConfJson.Style == "path")
	} else {
		return fmt.Errorf("unsupported confObj-target(%v)", c.confObj.Target)
	}
	return nil
}

func (c *CDPExecutor) createBucket(bucket string) (err error) {

	_, err = c.storage.s3Client.Client.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err == nil { // existed
		logger.Fmt.Warnf("%v.createBucket bucket %s has already created", c.Str(), bucket)
		return
	}

	_, err = c.storage.s3Client.Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		logger.Fmt.Errorf("%v.createBucket failed to create bucket %s. err=%v", c.Str(), bucket, err)
		return err
	}

	err = c.storage.s3Client.Client.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		logger.Fmt.Errorf(
			"%v.createBucket WaitUntilBucketExists failed to create bucket %s. err=%v",
			c.Str(), bucket, err)
		return err
	}
	return nil
}

func (c *CDPExecutor) is2s3() bool {
	return c.confObj.TargetJson.TargetType == meta.WatchingConfTargetS3
}

func (c *CDPExecutor) is2host() bool {
	return c.confObj.TargetJson.TargetType == meta.WatchingConfTargetHost
}

func (c *CDPExecutor) uploadQueue2Storage() {
	c.uploadFullQueue()
	c.uploadIncrQueue()
}

func (c *CDPExecutor) uploadFullQueue() {
	go c.uploadDispatcher(true)
	_ = c.reporter.ReportInfo(StepConsumeFullQueue)
}

func (c *CDPExecutor) uploadIncrQueue() {
	go c.uploadDispatcher(false)
	_ = c.reporter.ReportInfo(StepConsumeIncrQueue)
}

func (c *CDPExecutor) uploadDispatcher(full bool) {
	var (
		err   error
		wg    = c.ibp.incrWG
		queue = c.ibp.incrQueue
		pool  = c.ibp.incrPool
	)

	if full {
		wg = c.fbp.fullWG
		queue = c.fbp.fullQueue
		pool = c.fbp.fullPool
		logger.Fmt.Infof("%v.uploadDispatcher consuming fullQueue", c.Str())
	} else {
		logger.Fmt.Infof("%v.uploadDispatcher consuming incrQueue", c.Str())
	}

	defer func() {
		c.catchErr(err)
		if !full {
			_ = c.reporter.ReportInfo(StepEndFullTransfer)
		}
	}()
	for fwo := range queue {
		if c.isStopped() {
			return
		}
		if meta.AppIsDebugMode {
			logger.Fmt.Debugf("%v | 上传文件`%v`", c.Str(), fwo.Path)
		}
		wg.Add(1)
		if err = c.uploadOneFile(pool, fwo, wg); err != nil {
			return
		}
	}
	if c.isStopped() {
		return
	}
	if !full {
		//c.incrWG.Wait()
		return
	} else {
		//c.fullWG.Wait()
		c.waitUntilNoSyncingFile()
		if !c.snapCreator.disable() {
			c.snapCreator.start() // 全量备份完毕后，随即创建快照
			_ = c.reporter.ReportInfo(StepStartSnapCreator)
		}
	}
	if c.taskObj.Status == meta.CDPCOPYING {
		err = c.convertCopying2CDPing()
	} else if c.taskObj.Status == meta.CDPCDPING {
		_ = c.reporter.ReportInfo(StepAlreadyCDPing)
	} else {
		err = errors.New("can't go from state `CDPUNSTART` to state `CDPCDPING`")
	}
	return
}

func (c *CDPExecutor) waitUntilNoSyncingFile() {
	for {
		if c.fbp.fullPool.Running() == 0 && c.fbp.fullPool.Waiting() == 0 {
			break
		}
		time.Sleep(meta.CycleQuerySecsWhenFullEnd)
	}
}

func (c *CDPExecutor) uploadOneFile(pool *ants.Pool, fwo models.EventFileModel, wg *sync.WaitGroup) (_ error) {
	return pool.Submit(func() {
		var err_ error
		defer func() {
			if err_ == nil && fwo.Event == meta.Win32EventFullScan.Str() {
				_ = c.reporter.ReportDebugWithoutLogWithKey(
					meta.RuntimeIOBackup, StepFileScanUpS, meta.EventDesc[fwo.Event], fwo.Path)
			} else if err_ == nil && fwo.Event != meta.Win32EventFullScan.Str() {
				_ = c.reporter.ReportDebugWithoutLogWithKey(
					meta.RuntimeIOBackup, StepFileEventUpS, meta.EventDesc[fwo.Event], fwo.Path)
			} else if err_ != nil && fwo.Event == meta.Win32EventFullScan.Str() {
				_ = c.reporter.ReportWarnWithoutLogWithKey(
					meta.RuntimeIOBackup, StepFileScanUpF, meta.EventDesc[fwo.Event], fwo.Path, err_)
			} else {
				_ = c.reporter.ReportWarnWithoutLogWithKey(
					meta.RuntimeIOBackup, StepFileEventUpF, meta.EventDesc[fwo.Event], fwo.Path, err_)
			}
		}()

		defer wg.Done()
		err_ = c.upload2DiffStorage(fwo)
		if err_ == nil {
			return
		}

		// 连接不上备份服务器或SQL错误（不对文件类型错误做处理）
		if strings.Contains(err_.Error(), "SQLSTATE") {
			logger.Fmt.Warnf("%v.uploadOneFile ERR=%v", c.Str(), err_)
			return
		} else {
			err_ = nil
		}
	})
}

func (c *CDPExecutor) upload2DiffStorage(ffm models.EventFileModel) (err error) {
	defer c.catchErr(err)

	defer func() {
		if err == nil {
			if e := models.UpdateFileFlowStatus(
				c.DBDriver.DB, c.confObj.ID, ffm.ID, meta.FFStatusFinished); e != nil {
				logger.Fmt.Warnf("%v.upload2DiffStorage UpdateFileFlowStatus (f%v) to `FINISHED` ERR=%v",
					c.Str(), ffm.Path, e)
			}
		} else {
			logger.Fmt.Warnf("%v.upload2DiffStorage upload `%v` ERR=%v", c.Str(), ffm.Path, err)
			if e := models.UpdateFileFlowStatus(c.DBDriver.DB, c.confObj.ID, ffm.ID, meta.FFStatusError); e != nil {
				logger.Fmt.Warnf(
					"%v.uploadOneFile UpdateFileFlowStatus (f%v) ERR=%v", c.Str(), ffm.ID, e)
			}
		}
	}()

	if err = models.UpdateFileFlowStatus(
		c.DBDriver.DB, c.confObj.ID, ffm.ID, meta.FFStatusSyncing); err != nil {
		logger.Fmt.Warnf("%v.upload2DiffStorage UpdateFileFlowStatus (f%v) to `SYNCING` ERR=%v",
			c.Str(), ffm.ID, err)
		return
	}
	err = c.uploadWithRetry(ffm, meta.DefaultTransferRetryTimes)
	return
}

func (c *CDPExecutor) uploadWithRetry(ffm models.EventFileModel, retry int) (err error) {
	fp, err := os.Open(ffm.Path)
	if err != nil {
		return err
	}
	defer fp.Close()

	for i := 0; i < retry; i++ {
		_, _ = fp.Seek(0, io.SeekStart)
		err = nil
		if c.is2host() {
			err = c.upload2host(ffm, fp)
		} else if c.is2s3() {
			err = c.upload2s3(ffm, fp)
		} else {
			err = fmt.Errorf("unsupported confObj-target(%v)", c.confObj.Target)
		}
		if err == nil {
			if c.confObj.ExtInfoJson.LocalDeleteAfterBackup {
				_ = os.Remove(ffm.Path)
			}
			return nil
		}
	}

	return err
}

func (c *CDPExecutor) upload2host(ffm models.EventFileModel, fp io.Reader) (err error) {
	r, w := io.Pipe()
	writer := multipart.NewWriter(w)
	defer r.Close()

	// FIX: 优化大文件传输时内存占用过大的问题
	go func() {
		defer w.Close()
		defer writer.Close()

		var part io.Writer
		part, err = writer.CreateFormFile("filename", ffm.Storage)
		if err != nil {
			logger.Fmt.Warnf("%v.upload2host writer.CreateFormFile ERR=%v", c.Str(), err)
			return
		}
		if _, err = io.Copy(part, fp); err != nil {
			logger.Fmt.Warnf("%v.upload2host io.Copy ERR=%v", c.Str(), err)
			return
		}
	}()

	req, err := http.NewRequest(http.MethodPost, c.storage.uploadSession, r)
	if err != nil {
		logger.Fmt.Warnf("%v.upload2host http.NewRequest ERR=%v", c.Str(), err)
		return err
	}
	req.Header.Add("Content-Type", writer.FormDataContentType())
	client := new(http.Client)
	req.Close = true
	resp, err := client.Do(req)
	if err != nil {
		if strings.Contains(err.Error(), "EOF") {
			logger.Fmt.Warnf("%v.upload2host upload `%v` POST EOF", c.Str(), ffm.Path)
			return nil
		}
		logger.Fmt.Errorf("%v.upload2host client.Do ERR=%v", c.Str(), err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		c.progress.AddNum(1)
		c.progress.AddSize(ffm.Size)
		return nil
	}
	bb, _ := ioutil.ReadAll(resp.Body)
	return fmt.Errorf("invalid status(%v) reason(%v)", resp.StatusCode, string(bb))
}

func (c *CDPExecutor) upload2s3(ffm models.EventFileModel, fp io.Reader) (err error) {
	uploader := s3manager.NewUploader(
		c.storage.s3Session,
		func(u *s3manager.Uploader) {
			u.MaxUploadParts = s3manager.MaxUploadParts
		},
	)

	_, bucket, _, _, err := c.confObj.SpecifyTarget(ffm.Path)
	if err != nil {
		return err
	}
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(ffm.Storage),
		Body:   fp,
	})
	if err != nil {
		logger.Fmt.Warnf("%v.upload2s3 Unable to upload file %s | %v", c.Str(), ffm.Path, err)
	}

	c.progress.AddNum(1)
	c.progress.AddSize(ffm.Size)
	return
}

func (c *CDPExecutor) full() {
	go func() {
		c.scanNotify()
	}()
}

func (c *CDPExecutor) incr() {
	go func() {
		c.fsNotify()
	}()
}

func (c *CDPExecutor) deleteWhenDisableVersion() (err error) {
	if c.confObj.EnableVersion {
		logger.Fmt.Infof("%v.deleteWhenDisableVersion skip...", c.Str())
		return nil
	}

	_ = c.reporter.ReportInfo(StepScanDel)

	var (
		row *sql.Rows
		ffm models.EventFileModel
	)

	if row, err = models.QueryFileIteratorByConfig(
		c.DBDriver.DB, c.taskObj.ConfID); err != nil {
		logger.Fmt.Errorf("%v.deleteWhenDisableVersion Error=%v", c.Str(), err)
		return err
	}
	defer row.Close()

	for row.Next() {
		if c.isStopped() {
			return nil
		}
		if err = c.DBDriver.DB.ScanRows(row, &ffm); err != nil {
			logger.Fmt.Errorf("%v.deleteWhenDisableVersion. ScanRows-Err=%v", c.Str(), err)
			return err
		}
		path := filepath.Join(ffm.Parent, ffm.Name)
		if _, e := os.Stat(path); e != nil {
			_ = c.deleteFiles(ffm.Path, ffm.Name, meta.EventDesc[meta.Win32EventFullScan.Str()])
		}
	}
	return nil
}

func (c *CDPExecutor) scanNotify() {
	var err error
	defer func() {
		c.catchErr(err)
		c.fbp.fullQueueCloseOnce.Do(func() {
			close(c.fbp.fullQueue)
			_ = c.reporter.ReportInfo(StepCloseFullQueue)
		})
	}()

	if err = c.startWalkers(); err != nil {
		return
	}

	for fwo := range c.fbp.walkerQueue {
		if fwo.Type == meta.FTDir {
			if err = models.CreateDirIfNotExists(c.DBDriver.DB, c.confObj.ID, fwo.Path, ""); err != nil {
				return
			}
			continue
		}
		if c.isStopped() {
			return
		}
		if err = c.putFile2FullOrIncrQueue(nil, fwo, true); err != nil {
			return
		}
	}

	// 两段式日志, 提示线程终止
	if c.isStopped() {
		return
	}
}

// 由于连续变更的文件集，将其压入incrQueue时，必须是有新的不同的变更记录来驱动last其压入incrQueue
// 所以，针对最后一个文件变更，由于没有c.WatcherQueue驱动上传，就需要另开一个线程来收尾
type eventDelayPusher struct {
	m *sync.Map
}

func (r *eventDelayPusher) Store(v interface{}) {
	r.m.Store("last", v)
	r.m.Store("update_time", time.Now().Unix())
}

func (r *eventDelayPusher) Del() {
	r.m.Delete("last")
	r.m.Delete("update_time")
}

func (r *eventDelayPusher) Load() (nt_notify.FileWatchingObj, int64, bool) {
	last, ok := r.m.Load("last")
	if !ok {
		return nt_notify.FileWatchingObj{}, 0, false
	}
	update, ok := r.m.Load("update_time")
	if !ok {
		return last.(nt_notify.FileWatchingObj), 0, false
	}
	return last.(nt_notify.FileWatchingObj), update.(int64), true
}

func (c *CDPExecutor) fsNotify() {
	var err error
	defer func() {
		c.catchErr(err)
	}()

	if err = c.startWatchers(); err != nil {
		return
	}
}

func (c *CDPExecutor) notifyOneDir(w *watcherWrapper) {
	var err error

	go c.putFileEventWhenTimeout(w)
	for fwo := range w.watcherQueue {
		if c.isStopped() {
			return
		}
		/*连续文件变更记录的去重
		补充：
		1. 若一个文件处于持续写状态时，其ModTime会一直更新为最新时间

		*****************去重思路****************
		由于通道cs中取出的记录可能为相邻相同的连续记录，如下：
		A, A, B, B, B, B, C, D, D, D..........
		上传至incrQueue的条件如下：
		1. 5s内文件没有写入操作；
		2. 捕捉到新的不同名文件的变更记录就上传last
		*/
		switch fwo.Event {
		case meta.Win32EventDelete:
			fallthrough
		case meta.Win32EventRenameFrom:
			if !c.confObj.EnableVersion {
				_ = c.deleteFiles(fwo.Path, fwo.Name, meta.EventCode[fwo.Event])
			}
			continue
		case meta.Win32EventCreate:
			fallthrough
		case meta.Win32EventRenameTo:
			fallthrough
		case meta.Win32EventUpdate:
			if last, _, ok := w.watcherPatch.Load(); !ok {
				last = fwo
				w.watcherPatch.Store(fwo)
				break
			} else {
				// 捕捉到新的不同名文件的变更记录
				if fwo.Path != last.Path {
					err = c.putFile2FullOrIncrQueue(w, last, false)
					if err != nil {
						return
					}
					w.watcherPatch.Store(fwo) // 用新文件覆盖掉旧文件记录
				} else {
					// CTRL+V拷贝文件会产生create、update事件集
					// - 若本次到来的是同名文件的update事件且上一次事件是create事件
					//   则忽略本次的create、update事件集
					// - 否则更新Event
					if last.Event == meta.Win32EventCreate && fwo.Event == meta.Win32EventUpdate {
						if meta.AppIsDebugMode {
							logger.Fmt.Infof("拷贝未结束事件， file=`%v`", fwo.Path)
						}
						w.watcherPatch.Del()
						continue
					} else {
						w.watcherPatch.Store(fwo) // 更新Event
					}
				}
			}
		}
	}
	if c.isStopped() {
		return
	}
	logger.Fmt.Infof("%v.notifyOneDir stop notify `%v`", c.Str(), w.watcher.Str())
}

func (c *CDPExecutor) startWalkers() (err error) {
	logger.Fmt.Infof("%v.startWalkers start walkers...", c.Str())
	for _, v := range c.confObj.DirsMappingJson {
		w, e := NewWalker(v.LocalOrigin, v.LocalEnabledDepth, meta.DefaultWalkerCores, c.fbp.walkerQueue, c.fbp.counter)
		if e != nil {
			continue
		}
		c.fbp.walkers = append(c.fbp.walkers, w)
	}
	for _, w := range c.fbp.walkers {
		_ = w.Walk()
	}
	_ = c.reporter.ReportInfo(StepStartWalkers)

	go func() {
		ticker := time.NewTicker(meta.DefaultRetryTimeInterval)
		for range ticker.C {
			if c.isWalkersCompleted() {
				c.fbp.walkerQueueCloseOnce.Do(func() {
					close(c.fbp.walkerQueue)
					ticker.Stop()
					_ = c.reporter.ReportInfo(StepEndWalkers)
				})
			}
			time.Sleep(meta.DefaultCloseWalkerInterval)
		}
	}()
	return
}

func (c *CDPExecutor) startWatchers() (err error) {
	logger.Fmt.Infof("%v.startWatchers start watchers...", c.Str())

	watched := make([]string, 0)
	for _, v := range c.confObj.DirsMappingJson {
		if funk.InStrings(watched, v.LocalOrigin) {
			continue
		}
		w, e := nt_notify.NewWatcher(v.LocalOrigin, v.LocalEnableRecursion, v.LocalEnabledDepth)
		if e != nil {
			watched = append(watched, v.LocalOrigin)
			continue
		}
		wq, e := w.Start()
		if e != nil {
			watched = append(watched, v.LocalOrigin)
			continue
		}
		c.ibp.watchers = append(c.ibp.watchers, &watcherWrapper{
			watcher:      w,
			watcherPatch: &eventDelayPusher{m: new(sync.Map)},
			watcherQueue: wq,
		})
	}

	for _, w := range c.ibp.watchers {
		logger.Fmt.Infof("%v.startWatchers start `%v` is ok", c.Str(), w.watcher.Str())
		go c.notifyOneDir(w)
	}
	_ = c.reporter.ReportInfo(StepStartWatchers)
	return
}

func (c *CDPExecutor) deleteFiles(path, name, eventDesc string) (err error) {
	if c.is2host() {
		// 如果备机存在则删除
		if meta.AppIsDebugMode {
			logger.Fmt.Debugf("file=`%v`, will delete in remote host", path)
		}
		if e := c.deleteFilesInRemote(path, name); e == nil {
			_ = c.reporter.ReportDebugWithoutLogWithKey(
				meta.RuntimeIOBackup, StepFileEventDel, eventDesc, path)
		} else {
			_ = c.reporter.ReportWarnWithoutLogWithKey(
				meta.RuntimeIOBackup, StepFileEventDelF, eventDesc, path, e)
		}
	} else if c.is2s3() {
		if meta.AppIsDebugMode {
			logger.Fmt.Debugf("file=`%v`, will delete in s3", path)
		}
		// 如果对象存储存在则删除
		if e := c.deleteFilesInS3(path, name); e == nil {
			_ = c.reporter.ReportDebugWithoutLogWithKey(
				meta.RuntimeIOBackup, StepFileEventDel, eventDesc, path)
		} else {
			_ = c.reporter.ReportWarnWithoutLogWithKey(
				meta.RuntimeIOBackup, StepFileEventDelF, eventDesc, path, e)
		}
	}
	return nil
}

func (c *CDPExecutor) deleteFilesInRemote(path, name string) (err error) {
	if c.storage.deleteSession == meta.UnsetStr {
		return errors.New("lack address of delete session")
	}

	fs, e := models.QueryNoVersionFilesByPath(c.DBDriver.DB, c.confObj.ID, path, name)
	if e != nil {
		logger.Fmt.Errorf("%v.QueryNoVersionFilesByPath ERR=%v", c.Str(), e)
	}

	keys := make([]string, 0)
	for _, f := range fs {
		keys = append(keys, f.Storage)
	}

	for _, file := range funk.UniqString(keys) {
		logger.Fmt.Infof("%v.deleteFilesInRemote delete %v related %v", c.Str(), file, path)
		data := requests.Datas{"b64": file}
		resp, e := requests.Post(c.storage.deleteSession, data)
		if e != nil {
			logger.Fmt.Errorf("%v.Post(%v) ERR=%v", c.Str(), c.storage.deleteSession, e)
			err = e
			continue
		}
		if resp.R.StatusCode != http.StatusOK {
			err = errors.New("delete failed with 400 code")
			continue
		}
	}

	return models.DeleteNoVersionFilesByPath(c.DBDriver.DB, c.confObj.ID, path)
}

func (c *CDPExecutor) deleteFilesInS3(path, name string) (err error) {
	fs, e := models.QueryNoVersionFilesByPath(c.DBDriver.DB, c.confObj.ID, path, name)
	if e != nil {
		logger.Fmt.Errorf("%v.QueryNoVersionFilesByPath ERR=%v", c.Str(), e)
	}

	keys := make([]string, 0)
	for _, f := range fs {
		keys = append(keys, f.Storage)
	}

	_, bucket, _, _, err := c.confObj.SpecifyTarget(path)
	if err != nil {
		logger.Fmt.Warnf("%v.deleteFilesInS3 SpecifyTarget ERR=%v", c.Str(), err)
		return err
	}

	for _, file := range funk.UniqString(keys) {
		logger.Fmt.Infof("%v.deleteFilesInS3 delete %v related %v", c.Str(), file, path)
		e := c.storage.s3Client.DeleteObject(bucket, file)
		if e != nil {
			logger.Fmt.Errorf("%v.deleteFilesInS3 DeleteObject ERR=%v", c.Str(), e)
		}
	}

	return models.DeleteNoVersionFilesByPath(c.DBDriver.DB, c.confObj.ID, path)
}

func (c *CDPExecutor) putFile2FullOrIncrQueue(w *watcherWrapper, fwo nt_notify.FileWatchingObj, full bool) (err error) {
	if strings.Contains(fwo.Name, meta.IgnoreFlag) {
		return
	}
	fh, err := models.QueryLastSameNameFile(c.DBDriver.DB, c.confObj.ID, fwo.Path)
	if err != nil {
		return err
	}

	/*上传条件说明：
	情况1：存在同名文件、重载任务、同名文件状态为FINISHED，当前文件与同名文件的mtime相同
	      不做处理
	情况2：存在同名文件、重载任务、同名文件状态为FINISHED，当前文件与同名文件的mtime不相同
	      处理
	情况3：存在同名文件、重载任务、同名文件状态为非FINISHED(WATCHED, SYNCING, ERROR)
	      处理
	情况4：存在同名文件、普通任务、同名文件状态为FINISHED，当前文件与同名文件的mtime相同
	      处理
	情况5：存在同名文件、普通任务、同名文件状态为FINISHED，当前文件与同名文件的mtime不相同
	      处理
	情况6：存在同名文件、普通任务、同名文件状态为WATCHED或SYNCING
	      不做处理
	情况7：存在同名文件、普通任务、同名文件状态为ERROR
	      处理
	情况8：不存在同名文件
	      处理
	*/
	if fh.ID != 0 && c.isReload && fh.Status == meta.FFStatusFinished && fh.Time == fwo.Time ||
		fh.ID != 0 && !c.isReload && fh.Status == meta.FFStatusFinished && fh.Time == fwo.Time ||
		fh.ID != 0 && !c.isReload && (fh.Status == meta.FFStatusWatched || fh.Status == meta.FFStatusSyncing) {
		// do nothing
		return nil
	} else if fh.ID != 0 && c.isReload && fh.Status == meta.FFStatusFinished && fh.Time != fwo.Time ||
		fh.ID != 0 && c.isReload && fh.Status != meta.FFStatusFinished ||
		fh.ID != 0 && !c.isReload && fh.Status == meta.FFStatusFinished && fh.Time != fwo.Time ||
		fh.ID != 0 && !c.isReload && fh.Status == meta.FFStatusError ||
		fh.ID == 0 {
		/*如何处理？？？
		1. 若fh.Create >= c.startTs, 则表示文件对象fh是在本次CDP启动之后才捕捉到的
		   分支一： ERROR状态 ---> 处理
		   分支二： 非ERROR状态且fh.timestamp与fwo.timestamp不相同 ---> 处理
		   分支三： 非ERROR状态且fh.timestamp与fwo.timestamp相同 ---> 不处理
		2. 若fh.Create < c.startTs, 则表示文件对象fh是在本次CDP启动之前才捕捉到的
		   分支四： 处理
		*/
		if fh.ID != 0 && fh.Create >= c.startTs && fh.Status != meta.FFStatusError && fh.Time == fwo.Time {
			return
		}
	} else {
		logger.Fmt.Errorf("%v.uploadDispatcher \nfwo:%v\nfh:%v",
			c.Str(), pretty.Sprint(fwo), pretty.Sprint(fh))
		err = errors.New("invalid upload condition")
	}

	/*特殊情况说明：
	某一个文件更改很频繁，在上一个文件还没有传输完毕的情况下，新的变更又被此服务捕捉到了

	解决办法：
	每次上传文件，均需要等待历史最近一次的同名文件上传完毕
	*/
	if fh.ID != 0 && (fh.Status == meta.FFStatusSyncing || fh.Status == meta.FFStatusWatched) {
		fi, err_ := os.Stat(fwo.Path)
		if err_ != nil {
			return nil
		}
		// 说明新的变更事件已经产生，且已存在于c.watcherQueue中，所以忽略本次事件
		if fi.ModTime().Unix() > fwo.Time {
			//logger.Fmt.Debugf("忽略过期事件 %v", fwo.Flag)
			return nil
		}
		//logger.Fmt.Debugf("回溯到原监控队列 %v", fwo.Flag)
		if w != nil {
			w.watcherQueue <- fwo
		}
	}

	if !c.isValidPath(fh.Path, fh.Time) {
		return nil
	}

	// 记录事件至DB和Queue
	if err = models.CreateDirIfNotExists(c.DBDriver.DB, c.confObj.ID, filepath.Dir(fwo.Path), ""); err != nil {
		logger.Fmt.Errorf("%v.fsNotify CreateDirIfNotExists Error=%s", c.Str(), err.Error())
		return
	}
	fm, err := c.notifyOneFileEvent(fwo)
	if err != nil {
		logger.Fmt.Errorf("%v.fsNotify notifyOneFileEvent Error=%s", c.Str(), err.Error())
		return err
	}
	if !full {
		c.ibp.incrQueue <- fm
	} else {
		c.fbp.fullQueue <- fm
	}
	return
}

func (c *CDPExecutor) isValidPath(path string, mtime int64) bool {
	if !c.listFilter.IsValidByGrep(path) {
		return false
	}

	now := time.Now()
	mti := time.Unix(mtime, 0)

	switch c.confObj.TimeStrategyJson.Type {
	case meta.TimeStrategyNoLimit:
		return true
	case meta.TimeStrategyPre:
		// 最近指定时间之前的数据， 例如3年以前的数据
		if mti.Add(meta.OneDay * time.Duration(c.confObj.TimeStrategyJson.Pre)).Before(now) {
			return true
		}
	case meta.TimeStrategyNext:
		// 最近指定时间之后的数据， 例如3年以后至现在的数据
		if mti.Add(meta.OneDay * time.Duration(c.confObj.TimeStrategyJson.Pre)).After(now) {
			return true
		}
	case meta.TimeStrategyCenter:
		// 最近指定时间之间的数据， 例如3年前以后（TStart）至2年前以前(TEnd)的数据
		if mti.Add(meta.OneDay*time.Duration(c.confObj.TimeStrategyJson.After)).Before(now) &&
			mti.Add(meta.OneDay*time.Duration(c.confObj.TimeStrategyJson.Pre)).After(now) {
			return true
		}
	}
	return false
}

func (c *CDPExecutor) initTaskOnce() (err error) {
	var hb uuid.UUID
	defer func() {
		if err == nil && hb.String() != "00000000-0000-0000-0000-000000000000" {
			_, err = os.Create(filepath.Join(meta.HandlerBaseDir, hb.String()))
		}
	}()

	c_, err := models.QueryBackupTaskByConfID(c.DBDriver.DB, c.confObj.ID)
	if err == nil {
		c.taskObj = &c_
		return nil
	}

	_time := time.Now()
	c.taskObj = new(models.BackupTaskModel)
	c.taskObj.Start = &_time
	c.taskObj.Trigger = meta.TriggerMan
	c.taskObj.ConfID = c.confObj.ID
	c.taskObj.Status = meta.CDPUNSTART

	hb, err = uuid.NewV4()
	if err != nil {
		return err
	}
	tex := new(models.BackupExt)
	tex.Handler = hb.String()
	tes, err := json.Marshal(tex)
	if err != nil {
		return err
	}
	c.taskObj.ExtInfo = string(tes)
	return models.CreateBackupTaskModel(c.DBDriver.DB, c.taskObj)
}

func (c *CDPExecutor) notifyOneFileEvent(e nt_notify.FileWatchingObj) (fm models.EventFileModel, err error) {
	ff := new(models.EventFileModel)
	ff.Event = meta.EventCode[e.Event]
	ff.Create = time.Now().Unix()
	ff.ConfID = c.confObj.ID
	ff.Time = e.Time
	ff.Mode = int(e.Mode)
	ff.Size = e.Size
	ff.Path, _ = filepath.Abs(e.Path)
	ff.Name = filepath.Base(e.Path)
	ff.Version = nt_notify.GenerateVersion(c.confObj.ID, e)
	ff.Type = int(e.Type)
	ff.Status = meta.FFStatusWatched
	ff.Parent = tools.CorrectDirWithPlatform(filepath.Dir(e.Path), meta.IsWin)

	existed := models.ExistedHistoryVersionFile(c.DBDriver.DB, c.confObj.ID, ff.Path)
	if existed && c.confObj.EnableVersion {
		// 已备份文件中存在同名文件，且本次任务开启了版本控制
		// 则生成版本文件
		ff.Tag = tools.ConvertModTime2VersionFlag(e.Time)
		ff.Name = fmt.Sprintf("%s_%s", ff.Tag, ff.Name)
	} else if existed && !c.confObj.EnableVersion {
		// 已备份文件中存在同名文件，且本次任务未开启版本控制
		// 则删除同名文件记录，并以此新版本文件重新创建
		if err = models.DeleteByPath(c.DBDriver.DB, c.confObj.ID, ff.Path); err != nil {
			return
		}
	} else {
		// 已备份文件中不存在同名文件
		// do nothing
	}

	storagePath := filepath.Join(ff.Parent, ff.Name)
	if c.is2host() {
		origin, remote, err := c.confObj.SpecifyTargetWhenHost(ff.Path)
		if err != nil {
			return fm, err
		}
		ff.Storage = tools.GenerateRemoteHostKey(origin, storagePath, remote, tools.IsWin(c.confObj.TargetHostJson.OS))
	} else if c.is2s3() {
		origin, _, _, prefix, err := c.confObj.SpecifyTarget(ff.Path)
		if err != nil {
			return fm, err
		}
		ff.Storage = tools.GenerateS3Key(c.confObj.ID, origin, storagePath, prefix)
	} else {
		return fm, errors.New("unsupported target storage")
	}

	err = models.CreateFileFlowModel(c.DBDriver.DB, c.confObj.ID, ff)
	return *ff, err
}

func (c *CDPExecutor) putFileEventWhenTimeout(w *watcherWrapper) {
	logger.Fmt.Infof("%v.putFileEventWhenTimeout `%v` start...", c.Str(), w.watcher.Str())

	go func() {
		var (
			err error
			fwo nt_notify.FileWatchingObj
			t   int64
			ok  bool
		)

		defer func() {
			logger.Fmt.Infof("%v.putFileEventWhenTimeout `%v` exit...", c.Str(), w.watcher.Str())
			c.catchErr(err)
		}()

		for {
			if c.isStopped() {
				return
			}
			time.Sleep(meta.DefaultTailEventHandleSecs)
			if fwo, t, ok = w.watcherPatch.Load(); !ok {
				continue
			}

			if time.Now().Sub(time.Unix(t, 0)) > meta.DefaultTailEventHandleSecs {
				if err = c.putFile2FullOrIncrQueue(w, fwo, false); err != nil {
					logger.Fmt.Errorf("%v.putFile2FullOrIncrQueue `%v` ERR=%v",
						c.Str(), w.watcher.Str(), err)
					return
				}
				w.watcherPatch.Del()
			}
		}
	}()
}

func (c *CDPExecutor) exitWhenErr(code ErrorCode, err error) {
	if err == nil {
		return
	}
	c.notifyErrOnce.Do(func() {
		logger.Fmt.Errorf("%v.exitWhenErr err=%v, will exit...",
			c.Str(), ExtendErr(code, err.Error()))
		c.notifyErr <- code
		c.stop()
	})
}

func (c *CDPExecutor) catchErr(err error) {
	if err != nil {
		c.exitWhenErr(ExitErrCodeOriginErr, err)
	}
}

func (c *CDPExecutor) isWalkersCompleted() bool {
	if int(*c.fbp.counter) >= len(c.fbp.walkers) {
		return true
	}
	return false
}

func (c *CDPExecutor) createHandle() (handle string, err error) {
	tex, err := c.taskObj.BackupExtInfos()
	if err != nil {
		return handle, err
	}

	if _, e := os.Stat(meta.HandlerBaseDir); e != nil {
		logger.Fmt.Infof("%v.createHandle will create handlers base dir `%v`", c.Str(), meta.HandlerBaseDir)
		_ = os.MkdirAll(meta.HandlerBaseDir, meta.DefaultFileMode)
	}

	hp := c.handlePath(tex.Handler)
	if _, err = os.Stat(hp); err != nil {
		if fp, err := os.Create(hp); err != nil {
			return tex.Handler, err
		} else {
			fp.Close()
		}
	}
	return tex.Handler, nil
}

func (c *CDPExecutor) handlePath(handle string) string {
	return filepath.Join(meta.HandlerBaseDir, handle)
}
