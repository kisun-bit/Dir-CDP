package logic

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
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

// 目标存储枚举量
type TargetType string

const (
	TTS3   TargetType = "s3"
	TTHost TargetType = "hostInfo"
)

type hosts struct {
	server,
	origin,
	target models.ClientInfo
}

// 全量备份器
type fullBackupProxy struct {
	walker      *Walker
	walkerQueue chan nt_notify.FileWatchingObj
	fullPool    *ants.Pool
	fullWG      *sync.WaitGroup
	fullQueue   chan models.EventFileModel // 全量数据通道，该通道在全量备份完成后关闭
}

func initFullBackupProxy(poolSize, fullQueueSize int) *fullBackupProxy {
	fbp := new(fullBackupProxy)
	fbp.fullPool, _ = ants.NewPool(poolSize)
	fbp.fullWG = new(sync.WaitGroup)
	fbp.fullQueue = make(chan models.EventFileModel, fullQueueSize)
	return fbp
}

// 增量备份器
type incrBackupProxy struct {
	watcher      *nt_notify.Win32Watcher
	watcherPatch *eventDelayPusher // 用于对装载变更事件的通道WatcherQueue做相邻去重处理以及尾元素处理
	watcherQueue chan nt_notify.FileWatchingObj
	incrPool     *ants.Pool
	incrWG       *sync.WaitGroup
	incrQueue    chan models.EventFileModel // 增量数据通道，该通道永久开放
}

func initIncrBackupProxy(poolSize, incrQueueSize int) *incrBackupProxy {
	ibp := new(incrBackupProxy)
	ibp.incrPool, _ = ants.NewPool(poolSize)
	ibp.incrWG = new(sync.WaitGroup)
	ibp.incrQueue = make(chan models.EventFileModel, incrQueueSize)
	ibp.watcherPatch = new(eventDelayPusher)
	ibp.watcherPatch.m = new(sync.Map)
	return ibp
}

// 任务终止事件监视器
type hangEventMonitor struct {
	task  interface{}
	type_ meta.TaskType
}

func NewMonitorWithBackup(cdp *CDPExecutor) *hangEventMonitor {
	hem := new(hangEventMonitor)
	hem.task = cdp
	hem.type_ = meta.TaskTypeBackup
	return hem
}

func NewMonitorWithRestore(restore *RestoreTask) *hangEventMonitor {
	hem := new(hangEventMonitor)
	hem.task = restore
	hem.type_ = meta.TaskTypeRestore
	return hem
}

func (h *hangEventMonitor) monitor() {
	logger.Fmt.Infof("%v.monitor 任务监控器已启动, 正在捕捉中断事件...", h.Str())

	defer func() {
		logger.Fmt.Infof("%v.monitor【终止】", h.Str())
	}()

	var (
		ok  bool
		err error
	)

	for {
		ticker := time.NewTicker(meta.DefaultRetryTimeInterval)

		// TODO 可考虑用任务接口改写...
		for range ticker.C {
			if h.type_ == meta.TaskTypeBackup {
				ok, err = models.IsEnable(
					h.CDPExecutorObj().DBDriver.DB, h.CDPExecutorObj().confObj.ID)
			} else if h.type_ == meta.TaskTypeRestore {
				ok, err = models.IsRestoreCancel(
					h.RestoreObj().DBDriver.DB, h.RestoreObj().taskObj.ID)
			}

			if !ok && err == nil {
				logger.Fmt.Infof("%v.monitor !!!!!!!!!!!!!!!!! 【取消事件】", h.Str())
				if h.type_ == meta.TaskTypeBackup {
					h.CDPExecutorObj().exitWhenErr(
						ExitErrCodeUserCancel, ErrByCode(ExitErrCodeUserCancel))
				} else if h.type_ == meta.TaskTypeRestore {
					h.RestoreObj().exitWhenErr(
						ExitErrCodeUserCancel, ErrByCode(ExitErrCodeUserCancel))
				}
			} else if err != nil {
				logger.Fmt.Infof("%v.monitor !!!!!!!!!!!!!!!!! 【备份服务器连接失败】", h.Str())
				if h.type_ == meta.TaskTypeBackup {
					h.CDPExecutorObj().exitWhenErr(
						ExitErrCodeTargetConn, ErrByCode(ExitErrCodeTargetConn))
				} else if h.type_ == meta.TaskTypeRestore {
					h.RestoreObj().exitWhenErr(
						ExitErrCodeTargetConn, ErrByCode(ExitErrCodeTargetConn))
				}
			} else if ok {
				continue
			}
			ticker.Stop()
		}
	}
}

func (h *hangEventMonitor) CDPExecutorObj() *CDPExecutor {
	c, ok := h.task.(*CDPExecutor)
	if !ok {
		panic("failed to convert to CDPExecutorObj")
	}
	return c
}

func (h *hangEventMonitor) RestoreObj() *RestoreTask {
	r, ok := h.task.(*RestoreTask)
	if !ok {
		panic("failed to convert to RestoreTask")
	}
	return r
}

func (h *hangEventMonitor) Str() string {
	return fmt.Sprintf("%v.<hangEventMonitor-Thread>", h.CDPExecutorObj().Str())
}

// 目标存储
type storage struct {
	type_       TargetType
	hostInfo    models.TargetHost // 备份目标机存储信息
	s3Info      models.TargetS3   // 备份目标对象存储信息
	hostSession string
	s3Session   *session.Session
	s3Client    gos3.S3Client
}

// CDPExecutor 文件级CDP实现
type CDPExecutor struct {
	isReload      bool  // 当服务重启时，此属性为True
	startTs       int64 // 本次持续备份的开始时间
	handle        string
	hosts         *hosts
	taskLocker    *flock.Flock
	DBDriver      *models.DBProxy
	confObj       *models.ConfigModel
	taskObj       *models.BackupTaskModel
	filter        *Grep
	fbp           *fullBackupProxy
	ibp           *incrBackupProxy
	storage       *storage
	monitor       *hangEventMonitor
	hangSignal    *int32         // 用户禁用/取消信号、异常终止信号
	notifyErr     chan ErrorCode // 异常记录
	notifyErrOnce *sync.Once     // 多线程环境下仅报告一次异常记录
	localCtxDB    *bolt.DB       // TODO 本地用于记录服务关键信息的文件数据库
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

	ce.hosts = new(hosts)
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
	logger.Fmt.Infof("NewCDPExecutor. 成功初始化CDP执行器(conf=%v)", config.ID)
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
	logger.Fmt.Infof("NewCDPExecutor. 重启CDP执行器(conf=%v)完成，等待系统调度", config.ID)
	return
}

func AsyncStartCDPExecutor(ip string, conf int64, reload bool) (err error) {
	logger.Fmt.Infof("AsyncStartCDPExecutor IP(%v) ConfID(%v)", ip, conf)

	var (
		dp     *models.DBProxy
		cdp    *CDPExecutor
		config models.ConfigModel
	)

	dp, err = models.NewDBProxy(ip)
	if err != nil {
		return
	}
	logger.Fmt.Infof("AsyncStartCDPExecutor IP(%v) ConfID(%v) init DB is ok", ip, conf)

	config, err = models.QueryConfigByID(dp.DB, conf)
	if err != nil {
		return
	}
	logger.Fmt.Infof("AsyncStartCDPExecutor IP(%v) ConfID(%v) matched ConfigObject", ip, conf)

	if !reload {
		if config.Enable {
			logger.Fmt.Errorf("AsyncStartCDPExecutor IP(%v) ConfID(%v) is already enabled", ip, conf)
			return errors.New("already enabled")
		}

		if err = models.EnableConfig(dp.DB, conf); err != nil {
			return
		}
		cdp, err = NewCDPExecutor(&config, dp)
	} else {
		cdp, err = NewCDPExecutorWhenReload(&config, dp)
	}
	if err != nil {
		return
	}
	logger.Fmt.Infof("AsyncStartCDPExecutor IP(%v) ConfID(%v) new CDP executor", ip, conf)

	if err = cdp.StartWithRetry(); err != nil {
		return
	}
	logger.Fmt.Infof("AsyncStartCDPExecutor IP(%v) ConfID(%v) start", ip, conf)

	return nil
}

func (c *CDPExecutor) Str() string {
	return fmt.Sprintf("<CDP(conf=%v, taskObj=%v, dir=%v)>", c.confObj.ID, c.taskObj.ID, c.confObj.Dir)
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
	if !c.isReload {
		if err = c.initTaskOnce(); err != nil {
			return
		}
	}

	if err = c.registerShardingModel(); err != nil {
		return
	}

	// 无限重试
	go func() {
		for {
			ce, err := c.confObj.Ext()
			if err != nil {
				logger.Fmt.Warnf("%v retry failed. parse server-ip ERR=%v", c.Str(), err)
				break
			}
			if c.DBDriver, err = models.NewDBProxy(ce.ServerAddress); err != nil {
				logger.Fmt.Infof("%v.StartWithRetry failed to retry. will sleep 5s...", c.Str())
				// do nothing
			} else {
				logger.Fmt.Infof("%v.StartWithRetry 正在拉起任务...", c.Str())
				if c.inRetryErr(c.Start()) {
					break
				}
			}
			time.Sleep(meta.DefaultRetryTimeInterval)
		}
	}()
	return nil
}

func (c *CDPExecutor) Start() (ec ErrorCode) {
	if err := c.mallocHandle(); err != nil {
		return ExitErrCodeLackHandler
	}
	logger.Fmt.Infof("%v.Start 成功分配任务执行句柄%v", c.Str(), c.handle)

	locked, err := c.taskLocker.TryLock()
	if err != nil {
		logger.Fmt.Errorf("%v.StartWithRetry TryLock ERR=%v", c.Str(), err)
		return ExitErrCodeTaskReplicate
	}
	logger.Fmt.Infof("%v.Start 成功锁定句柄%v", c.Str(), c.handle)

	if locked {
		defer func() {
			if err = c.taskLocker.Unlock(); err != nil {
				logger.Fmt.Errorf("%v.StartWithRetry UnLock ERR=%v", c.Str(), err)
				logger.Fmt.Errorf("%v.Start 解锁句柄失败%v", c.Str(), c.handle)
			} else {
				logger.Fmt.Infof("%v.Start 成功解锁句柄%v", c.Str(), c.handle)
			}
		}()
	}

	go c.monitorStopNotify()
	go c.logic()

	// 阻塞到有退出信号为止
	ec = <-c.notifyErr
	logger.Fmt.Infof("%v.Start CDP执行器发生【%v】, 正在重试...请稍后", c.Str(), ErrByCode(ec))
	if ec == ExitErrCodeUserCancel {
		return
	}

	// 重置CDPExecutor
	address := c.hosts.server.Address
	if err := initOrResetCDPExecutor(c, c.confObj, c.DBDriver); err != nil {
		return
	}
	c.isReload = true
	c.hosts.server.Address = address
	logger.Fmt.Infof("%v.initOrResetCDPExecutor 重置完成", c.Str())
	return
}

func (c *CDPExecutor) mallocHandle() (err error) {
	if c.handle, err = c.createHandle(); err != nil {
		logger.Fmt.Errorf("%v.mallocHandle ERR=%v", c.Str(), err)
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
	}()
}

func (c *CDPExecutor) stop() {
	logger.Fmt.Infof("%v.stop 正在停止CDP执行器...", c.Str())
	if c.isStopped() {
		return
	}
	atomic.StoreInt32(c.hangSignal, 1)
	c.stopBackupProxy()
}

func (c *CDPExecutor) stopBackupProxy() {
	if c.fbp.walker != nil {
		c.fbp.walker.SetStop()
	}
	if c.ibp.watcher != nil {
		c.ibp.watcher.SetStop()
	}
}

func (c *CDPExecutor) logic() {
	logger.Fmt.Infof("%v.monitorStopNotify 开始执行核心逻辑", c.Str())

	var err error
	if err = c.moreInit(); err != nil {
		logger.Fmt.Errorf("%v.logic moreInit err=%v", c.Str(), err)
		return
	}
	logger.Fmt.Infof("%v.monitorStopNotify 初始化环境参数完成", c.Str())

	/*对于启动流程的说明如下：
	启动任务方式有两种
	1. 重载未完成任务（断网、重启、宕机），避免直接全量
	2. 执行新任务（远程调用）
	*/

	if err = models.DeleteNotUploadFileFlows(c.DBDriver.DB, c.confObj.ID); err != nil {
		return
	}
	logger.Fmt.Infof("%v.monitorStopNotify 清理过期事件完成", c.Str())

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

func (c *CDPExecutor) logicWhenReload() (err error) {
	logger.Fmt.Infof("%v.logicWhenReload【%v】重载任务 -> 全量(未完成同步的)+增量", c.Str(), c.taskObj.Status)
	c.full()
	c.incr()
	return nil
}

func (c *CDPExecutor) logicWhenNormal() (err error) {
	// 历史任务处于meta.CDPUNSTART或meta.CDPCOPYING状态时，需执行全量+增量
	if c.taskObj.Status == meta.CDPUNSTART || c.taskObj.Status == meta.CDPCOPYING {
		if err = models.UpdateBackupTaskStatusByConfID(c.DBDriver.DB, c.confObj.ID, meta.CDPCOPYING); err != nil {
			return
		}
		logger.Fmt.Infof("%v.logicWhenNormal【COPYING】-> 全量(未完成同步的)+增量", c.Str())
		c.full()
		c.incr()
		return nil
	}

	// 历史任务处于meta.CDPCDPING状态，且表记录为空时，需执行全量+增量（此分支仅用于调试环境做测试时出现）
	if c.taskObj.Status == meta.CDPCDPING && models.IsEmptyTable(c.DBDriver.DB, c.confObj.ID) {
		if err = models.UpdateBackupTaskStatusByConfID(c.DBDriver.DB, c.confObj.ID, meta.CDPCOPYING); err != nil {
			return
		}
		logger.Fmt.Warnf("%v.logicWhenNormal convert status from `%v` to `COPYING`", c.Str(), c.taskObj.Status)
		logger.Fmt.Infof("%v.logicWhenNormal【COPYING】--> 全量(未完成同步的)+增量", c.Str())
		c.full()
		c.incr()
		return nil
	}

	// 历历史任务处于meta.CDPCDPING状态，且表记录不为空时，仅执行增量即可
	if c.taskObj.Status == meta.CDPCDPING && !models.IsEmptyTable(c.DBDriver.DB, c.confObj.ID) {
		logger.Fmt.Infof("%v.logicWhenNormal【%v】--> 增量", c.Str(), c.taskObj.Status)
		close(c.fbp.fullQueue)
		logger.Fmt.Infof("%v.logicWhenNormal close FullQueue", c.Str())
		c.incr()
		return nil
	}

	logger.Fmt.Errorf("%v.logicWhenNormal error status=%v", c.Str(), c.taskObj.Status)
	panic("undefined taskObj status")
}

func (c *CDPExecutor) moreInit() (err error) {
	// 黑白名单过滤
	c.filter = NewGrep(c.confObj.Include, c.confObj.Exclude)

	// 备份服务器信息
	c.hosts.server, err = models.QueryHostInfoByHostID(c.DBDriver.DB, c.confObj.Server)
	if err != nil {
		return
	}
	logger.Fmt.Infof("%v.moreInit server ->%v", c.Str(), c.hosts.server)

	// 源机信息
	c.hosts.origin, err = models.QueryHostInfoByHostID(c.DBDriver.DB, c.confObj.Origin)
	if err != nil {
		return
	}
	logger.Fmt.Infof("%v.moreInit origin ->%v", c.Str(), c.hosts.origin)

	if models.Is2Host(*c.confObj) {
		// 目标机信息
		c.hosts.target, err = models.QueryTargetHostInfoByConf(c.DBDriver.DB, c.confObj.ID)
		if err != nil {
			return
		}
		logger.Fmt.Infof("%v.moreInit 2h TargetHost ->%v", c.Str(), c.hosts.target)

		// 目标机目标存储路径
		c.storage.hostInfo, err = models.QueryTargetConfHostByConf(c.DBDriver.DB, c.confObj.ID)
		if err != nil {
			return
		}
		logger.Fmt.Infof("%v.moreInit 2h TargetConf ->%v", c.Str(), c.storage.hostInfo)

		// 目标机上传接口地址
		c.storage.hostSession = fmt.Sprintf(
			"http://%s:%v/api/v1/upload", c.hosts.target.Address, meta.AppPort)
		logger.Fmt.Infof("%v.moreInit 2h session ->%v", c.Str(), c.storage.hostSession)
		c.storage.type_ = TTHost
	} else if models.Is2S3(*c.confObj) {

		// 目标对象存储相关配置
		c.storage.s3Info, err = models.QueryTargetConfS3ByConf(c.DBDriver.DB, c.confObj.ID)
		if err != nil {
			return
		}
		logger.Fmt.Infof("%v.moreInit 2s3 TargetS3Conf ->%v", c.Str(), c.storage.s3Info)

		// 创建目标对象存储目标桶
		c.storage.s3Session, c.storage.s3Client = tools.NewS3Client(
			c.storage.s3Info.TargetConfS3.AccessKey,
			c.storage.s3Info.TargetConfS3.SecretKey,
			c.storage.s3Info.TargetConfS3.Endpoint,
			c.storage.s3Info.TargetConfS3.Region,
			c.storage.s3Info.TargetConfS3.SSL,
			c.storage.s3Info.TargetConfS3.Path)
		c.storage.type_ = TTS3
		if err = c.createBucket(c.storage.s3Info.TargetConfS3.Bucket); err != nil {
			return
		}
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
	return c.storage.type_ == TTS3
}

func (c *CDPExecutor) is2host() bool {
	return c.storage.type_ == TTHost
}

func (c *CDPExecutor) uploadQueue2Storage() {
	c.uploadFullQueue()
	c.uploadIncrQueue()
}

func (c *CDPExecutor) uploadFullQueue() {
	go c.uploadDispatcher(true)
	logger.Fmt.Infof("%v.uploadFullQueue 开始全量扫描备份", c.Str())
}

func (c *CDPExecutor) uploadIncrQueue() {
	go c.uploadDispatcher(false)
	logger.Fmt.Infof("%v.uploadIncrQueue 开始持续增量备份", c.Str())
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
		logger.Fmt.Infof("%v.uploadDispatcher(FULL %v)【终止】", c.Str(), full)
	}()

	for fwo := range queue {
		if c.isStopped() {
			return
		}
		if !c.isValidPath(fwo.Path, fwo.Time) {
			continue
		}
		logger.Fmt.Debugf("%v | [监控捕捉] <<<<<<<<<<<<<<<<<<<<<<<<< %v", c.Str(), fwo.Path)
		wg.Add(1)
		if err = c.uploadOneFile(pool, fwo, wg); err != nil {
			return
		}
	}

	if c.isStopped() {
		return
	}

	if !full {
		//c.incrWG.Wait()  // TODO 不用阻塞在此
		logger.Fmt.Infof("%v.logic incrWG 退出阻塞状态...", c.Str())
		return
	} else {
		//c.fullWG.Wait() // TODO 不用阻塞在此
		logger.Fmt.Infof("%v.logic fullWG 退出阻塞状态...", c.Str())
	}

	err = models.UpdateBackupTaskStatusByConfID(c.DBDriver.DB, c.confObj.ID, meta.CDPCDPING)
	if err != nil {
		logger.Fmt.Warnf("%v.uploadDispatcher failed to convert CDPING status, err=%v", c.Str(), err)
		return
	}

	logger.Fmt.Infof("%v.uploadDispatcher exit，taskObj(%v) 进入【CDPING】状态", c.Str(), c.taskObj.ID)
}

func (c *CDPExecutor) uploadOneFile(pool *ants.Pool, fwo models.EventFileModel, wg *sync.WaitGroup) (err error) {
	return pool.Submit(func() {
		defer wg.Done()

		/* 存在同名文件的上传逻辑

		找到最近一次的同名文件记录:
		1. 处于FINISHED或ERROR状态：
		   直接上传
		2. 处于SYNCING或WATCHED状态：
		   TODO 暂时不做处理，后续优化
		未找到：
		   直接上传
		*/
		err = c.upload2DiffStorage(fwo)
		if err == nil {
			return
		}

		// 连接不上备份服务器或SQL错误（不对文件类型错误做处理）
		if strings.Contains(err.Error(), "SQLSTATE") {
			logger.Fmt.Warnf("%v.uploadOneFile ERR=%v", c.Str(), err)
			return
		} else {
			err = nil
		}
	})
}

func (c *CDPExecutor) upload2DiffStorage(ffm models.EventFileModel) (err error) {
	defer c.catchErr(err)

	defer func() {
		if err == nil {
			if err = models.UpdateFileFlowStatus(
				c.DBDriver.DB, c.confObj.ID, ffm.ID, meta.FFStatusFinished); err != nil {
				logger.Fmt.Warnf("%v.upload2DiffStorage UpdateFileFlowStatus (f%v) to `FINISHED` ERR=%v",
					c.Str(), ffm.Path, err)
			}
		} else {
			if err = models.UpdateFileFlowStatus(c.DBDriver.DB, c.confObj.ID, ffm.ID, meta.FFStatusError); err != nil {
				logger.Fmt.Warnf(
					"%v.uploadOneFile UpdateFileFlowStatus (f%v) ERR=%v", c.Str(), ffm.ID, err)
			}
		}
	}()

	if err = models.UpdateFileFlowStatus(
		c.DBDriver.DB, c.confObj.ID, ffm.ID, meta.FFStatusSyncing); err != nil {
		logger.Fmt.Warnf("%v.upload2DiffStorage UpdateFileFlowStatus (f%v) to `SYNCING` ERR=%v",
			c.Str(), ffm.ID, err)
		return
	}

	fp, err := os.Open(ffm.Path)
	if err != nil {
		return err
	}
	defer fp.Close()

	if models.Is2Host(*c.confObj) {
		return c.upload2host(ffm, fp)
	} else if models.Is2S3(*c.confObj) {
		return c.upload2s3(ffm, fp)
	} else {
		return fmt.Errorf("unsupported confObj-target(%v)", c.confObj.Target)
	}
}

func (c *CDPExecutor) upload2host(ffm models.EventFileModel, fp io.Reader) (err error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	part, _ := writer.CreateFormFile("filename", ffm.Storage)
	if _, err = io.Copy(part, fp); err != nil {
		logger.Fmt.Errorf("%v.upload2host io.Copy ERR=%v", c.Str(), err)
		return
	}
	if err = writer.Close(); err != nil {
		return
	}

	r, _ := http.NewRequest("POST", c.storage.hostSession, body)
	// TODO 在发生中断时，立即终止HTTP连接

	r.Header.Add("Content-Type", writer.FormDataContentType())
	client := &http.Client{}
	if resp, err := client.Do(r); err != nil {
		logger.Fmt.Errorf("%v.upload2host client.Do ERR=%v", c.Str(), err)
		return err
	} else {
		if resp.StatusCode == http.StatusOK {
			return nil
		} else {
			bb, _ := ioutil.ReadAll(resp.Body)
			logger.Fmt.Errorf("%v.upload2host status-err %v", string(bb))
		}
	}
	return nil
}

func (c *CDPExecutor) upload2s3(ffm models.EventFileModel, fp io.Reader) (err error) {
	uploader := s3manager.NewUploader(
		c.storage.s3Session,
		func(u *s3manager.Uploader) {
			u.MaxUploadParts = s3manager.MaxUploadParts
		},
	)

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(c.storage.s3Info.TargetConfS3.Bucket),
		Key:    aws.String(ffm.Storage),
		Body:   fp,
	})
	if err != nil {
		logger.Fmt.Warnf("%v.upload2s3 Unable to upload file %s | %v", c.Str(), ffm.Path, err)
	}
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

func (c *CDPExecutor) scanNotify() {
	var err error
	defer func() {
		logger.Fmt.Infof("%v.scanNotify【终止】", c.Str())
		c.catchErr(err)
		close(c.fbp.fullQueue)
	}()

	if err = c.startWalker(); err != nil {
		return
	}

	for fwo := range c.fbp.walkerQueue {
		if fwo.Type == meta.FTDir {
			if fwo.Path == c.confObj.Dir {
				continue
			}
			if err = models.CreateDirIfNotExists(c.DBDriver.DB, c.confObj.ID, fwo.Path, ""); err != nil {
				return
			}
			continue
		}
		if c.isStopped() {
			return
		}
		if err = c.putFile2FullOrIncrQueue(fwo, true); err != nil {
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
		logger.Fmt.Infof("%v.fsNotify【终止】", c.Str())
		c.catchErr(err)
		close(c.ibp.incrQueue)
	}()

	if err = c.startWatcher(); err != nil {
		return
	}

	go c.putFileEventWhenTimeout()

	for fwo := range c.ibp.watcherQueue {
		if c.isStopped() {
			//c.watcher.SetStop()
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
			// do nothing
		case meta.Win32EventCreate:
			fallthrough
		case meta.Win32EventRenameFrom:
			fallthrough
		case meta.Win32EventRenameTo:
			fallthrough
		case meta.Win32EventUpdate:
			if last, _, ok := c.ibp.watcherPatch.Load(); !ok {
				last = fwo
				c.ibp.watcherPatch.Store(fwo)
				break
			} else {
				// 捕捉到新的不同名文件的变更记录
				if fwo.Path != last.Path {
					err = c.putFile2FullOrIncrQueue(last, false)
					if err != nil {
						return
					}
					c.ibp.watcherPatch.Store(fwo) // 用新文件覆盖掉旧文件记录
				} else {
					c.ibp.watcherPatch.Store(fwo) // 更新Event
				}
			}
		}
	}

	if c.isStopped() {
		return
	}
}

func (c *CDPExecutor) startWalker() (err error) {
	logger.Fmt.Infof("%v.startWalker start walker...", c.Str())

	if err = models.UpdateBackupTaskStatusByConfID(c.DBDriver.DB, c.confObj.ID, meta.CDPCOPYING); err != nil {
		logger.Fmt.Errorf("%v.scanNotify UpdateBackupTaskStatusByConfID err=%v", c.Str(), err)
		return
	}

	c.fbp.walker = NewWalker(
		c.confObj.Dir,
		int(c.confObj.Depth),
		int(c.confObj.Cores),
		c.confObj.Include,
		c.confObj.Exclude,
		*c.taskObj.Start,
		int(c.confObj.ValidDays))

	c.fbp.walkerQueue, err = c.fbp.walker.Walk()
	if err != nil {
		logger.Fmt.Errorf("%v.scanNotify Walk Error=%s", c.Str(), err.Error())
		return
	}
	return
}

func (c *CDPExecutor) startWatcher() (err error) {
	logger.Fmt.Infof("%v.startWatcher start watcher...", c.Str())

	c.ibp.watcher, err = nt_notify.NewWatcher(c.confObj.Dir, c.confObj.Recursion)
	if err != nil {
		logger.Fmt.Errorf("%v.fsNotify NewWatcher Error=%s", c.Str(), err.Error())
		return
	}
	logger.Fmt.Infof("%v.fsNotify init watcher(%v) is ok", c.Str(), c.confObj.Dir)

	c.ibp.watcherQueue, err = c.ibp.watcher.Start()
	if err != nil {
		logger.Fmt.Errorf("%v.fsNotify StartWithRetry Error=%s", c.Str(), err.Error())
		return
	}
	logger.Fmt.Infof("%v.fsNotify start watcher(%v)...", c.Str(), c.confObj.Dir)
	return
}

func (c *CDPExecutor) putFile2FullOrIncrQueue(fwo nt_notify.FileWatchingObj, full bool) (err error) {
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
			logger.Fmt.Debugf("忽略过期事件 %v", fwo.Path)
			return nil
		}
		logger.Fmt.Debugf("回溯到原监控队列 %v", fwo.Path)
		c.ibp.watcherQueue <- fwo
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
	if c.confObj.Depth != -1 && int64(strings.Count(filepath.Dir(path), meta.Sep)) > c.confObj.Depth {
		return false
	}
	if !c.filter.IsValidByGrep(path) {
		return false
	}
	if c.confObj.ValidDays != -1 && mtime+24*60*60*c.confObj.ValidDays > c.taskObj.Start.Unix() {
		return false
	}
	return true
}

func (c *CDPExecutor) initTaskOnce() (err error) {
	var hb uuid.UUID
	defer func() {
		if err == nil && hb.String() != "" {
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

	// 捕捉到已存在文件的变更通知，则为其加入"版本标记"
	if models.ExistedHistoryVersionFile(c.DBDriver.DB, c.confObj.ID, ff.Path) {
		ff.Tag = tools.ConvertModTime2VersionFlag(e.Time)
		ff.Name += "." + ff.Tag
	}
	d, err := models.QueryDirByPath(c.DBDriver.DB, c.confObj.ID, ff.Parent)
	if err != nil {
		return fm, err
	}

	switch c.storage.type_ {
	case TTHost:
		ff.Storage = tools.GenerateRemoteHostKey(c.confObj.ID, d.ID,
			c.storage.hostInfo.TargetConfHost.RemotePath, tools.IsWin(c.hosts.target.Type))
	case TTS3:
		ff.Storage = tools.GenerateS3Key(c.confObj.ID, d.ID)
	}

	err = models.CreateFileFlowModel(c.DBDriver.DB, c.confObj.ID, ff)
	return *ff, err
}

func (c *CDPExecutor) putFileEventWhenTimeout() {
	logger.Fmt.Infof("%v.putFileEventWhenTimeout start...", c.Str())

	go func() {
		var (
			err error
			fwo nt_notify.FileWatchingObj
			t   int64
			ok  bool
		)

		defer func() {
			logger.Fmt.Infof("%v.putFileEventWhenTimeout【终止】", c.Str())
			c.catchErr(err)
		}()

		for {
			if c.isStopped() {
				return
			}
			time.Sleep(5 * time.Second)
			if fwo, t, ok = c.ibp.watcherPatch.Load(); !ok {
				continue
			}

			if time.Now().Unix()-t > 5 {
				if err = c.putFile2FullOrIncrQueue(fwo, false); err != nil {
					logger.Fmt.Errorf("%v.putFileEventWhenTimeout putFile2FullOrIncrQueue ERR=%v",
						c.Str(), err)
					return
				}
				c.ibp.watcherPatch.Del()
			}
		}
	}()
}

func (c *CDPExecutor) exitWhenErr(code ErrorCode, err error) {
	if err == nil {
		return
	}

	c.notifyErrOnce.Do(func() {
		logger.Fmt.Errorf("%v.exitWhenErr 捕捉到%v, CDP执行器等待退出...",
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

func (c *CDPExecutor) createHandle() (handle string, err error) {
	tex, err := c.taskObj.BackupExtInfos()
	if err != nil {
		return handle, err
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
