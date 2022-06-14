// @Title  file_cdp.go
// @Description  实现文件级CDP的核心逻辑
// @Author  Kisun
// @Update  2022-6-13
package logic

import (
	"bytes"
	"encoding/base64"
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
type fullBackupProxy struct {
	walkers            []*Walker
	walkerQueue        chan nt_notify.FileWatchingObj
	fullPool           *ants.Pool
	fullWG             *sync.WaitGroup
	fullQueue          chan models.EventFileModel // 全量数据通道，该通道在全量备份完成后关闭
	fullQueueCloseOnce *sync.Once
	counter            *int32 // 计数器信号， 目的是等待所有目录枚举完毕后，关闭枚举通道
}

// @title    initFullBackupProxy
// @description    初始化并返回一个多路径且基于扫描的全量备份代理器
// @param    poolSize        int         "并发上传的池大小"
// @param    fullQueueSize   int         "枚举文件路径的通道的缓存大小"
// @return   fbp             *fullBackupProxy
func initFullBackupProxy(poolSize, fullQueueSize int) *fullBackupProxy {
	fbp := new(fullBackupProxy)
	fbp.fullPool, _ = ants.NewPool(poolSize)
	fbp.fullWG = new(sync.WaitGroup)
	fbp.fullQueue = make(chan models.EventFileModel, fullQueueSize)
	fbp.walkerQueue = make(chan nt_notify.FileWatchingObj, meta.DefaultEnumPathChannelSize)
	fbp.counter = new(int32)
	fbp.fullQueueCloseOnce = new(sync.Once)
	return fbp
}

// incrBackupProxy 增量备份代理
type incrBackupProxy struct {
	watchers           []*nt_notify.Win32Watcher
	watcherPatch       *eventDelayPusher // 用于对装载变更事件的通道WatcherQueue做相邻去重处理以及尾元素处理
	watcherQueue       chan nt_notify.FileWatchingObj
	incrPool           *ants.Pool
	incrWG             *sync.WaitGroup
	incrQueue          chan models.EventFileModel // 增量数据通道，该通道永久开放
	incrQueueCloseOnce *sync.Once
}

// @title    initIncrBackupProxy
// @description    初始化并返回一个多路径且基于事件通知的全量备份代理器
// @param    poolSize        int         "并发上传的池大小"
// @param    incrQueueSize   int         "枚举文件路径的通道的缓存大小"
// @return   fbp             *incrBackupProxy
func initIncrBackupProxy(poolSize, incrQueueSize int) *incrBackupProxy {
	ibp := new(incrBackupProxy)
	ibp.incrPool, _ = ants.NewPool(poolSize)
	ibp.incrWG = new(sync.WaitGroup)
	ibp.incrQueue = make(chan models.EventFileModel, incrQueueSize)
	ibp.watcherQueue = make(chan nt_notify.FileWatchingObj, nt_notify.WatchingQueueSize)
	ibp.watcherPatch = new(eventDelayPusher)
	ibp.watcherPatch.m = new(sync.Map)
	ibp.incrQueueCloseOnce = new(sync.Once)
	return ibp
}

// storage 目标存储
type storage struct {
	uploadSession string
	deleteSession string
	s3Client      gos3.S3Client
	s3Session     *session.Session
}

// CDPExecutor 文件级CDP实现
type CDPExecutor struct {
	isReload      bool  // 当服务重启时，此属性为True
	startTs       int64 // 本次持续备份的开始时间
	handle        string
	taskLocker    *flock.Flock
	DBDriver      *models.DBProxy
	confObj       *models.ConfigModel
	taskObj       *models.BackupTaskModel
	listFilter    *Grep
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

	logger.Fmt.Infof("NewCDPExecutor. 重启CDP执行器(conf=%v)完成，等待系统调度中", config.ID)
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
		return err
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
	return fmt.Sprintf("<CDP(conf=%v, task=%v)>", c.confObj.ID, c.taskObj.ID)
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

	if err = c.moreInit(); err != nil {
		logger.Fmt.Errorf("%v.logic moreInit err=%v", c.Str(), err)
		return
	}

	logger.Fmt.Infof("%v.monitorStopNotify 初始化环境参数完成", c.Str())

	// 无限重试
	go func() {
		for {
			if c.confObj.ExtInfoJson.ServerAddress == meta.UnsetStr {
				logger.Fmt.Warnf("%v.StartWithRetry 重试失败, 未匹配到备份服务器IP, 正在退出...", c.Str())
				break
			}
			if c.DBDriver, err = models.NewDBProxy(c.confObj.ExtInfoJson.ServerAddress); err != nil {
				logger.Fmt.Infof("%v.StartWithRetry failed to retry. will sleep 5s...", c.Str())
				// do nothing
			} else {
				logger.Fmt.Infof("%v.StartWithRetry 正在拉起任务...", c.Str())
				if c.inRetryErr(c.Start()) {
					logger.Fmt.Infof("%v.StartWithRetry 不予重试", c.Str())
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
	logger.Fmt.Infof("%v.Start CDP执行器发生【%v】，已退出...", c.Str(), ErrByCode(ec))
	if ec == ExitErrCodeUserCancel {
		return
	}

	// 重置CDPExecutor
	if err := initOrResetCDPExecutor(c, c.confObj, c.DBDriver); err != nil {
		return
	}
	c.isReload = true
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
	for _, w := range c.fbp.walkers {
		w.SetStop()
	}
	for _, w := range c.ibp.watchers {
		w.SetStop()
	}
	close(c.ibp.watcherQueue)
}

func (c *CDPExecutor) logic() {
	var err error
	defer c.catchErr(err)

	logger.Fmt.Infof("%v.monitorStopNotify 开始执行核心逻辑", c.Str())
	if err = c.createStorageContainer(); err != nil {
		return
	}

	if err = models.DeleteNotUploadFileFlows(c.DBDriver.DB, c.confObj.ID); err != nil {
		return
	}
	logger.Fmt.Infof("%v.monitorStopNotify 清理过期事件完成", c.Str())

	/*启动任务方式有两种：
	1. 重载未完成任务（断网、重启、宕机），避免直接全量
	2. 执行新任务（远程调用）
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
		for _, b := range c.confObj.Buckets() {
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
	c.listFilter = NewGrep(c.confObj.Include, c.confObj.Exclude)

	if err = c.confObj.LoadsJsonFields(c.DBDriver.DB); err != nil {
		return
	}
	logger.Fmt.Infof("%v.moreInit CONFIG is:%v\n", c.Str(), pretty.Sprint(c.confObj))
	logger.Fmt.Infof("%v.modeInit TASK is:%v\n", c.Str(), c.taskObj.String())

	if c.is2host() {
		c.storage.uploadSession = fmt.Sprintf(
			"http://%s:%v/api/v1/upload", c.confObj.TargetHostJson.Address, meta.AppPort)
		c.storage.deleteSession = fmt.Sprintf(
			"http://%s:%v/api/v1/delete", c.confObj.TargetHostJson.Address, meta.AppPort)
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
	logger.Fmt.Infof("%v.uploadFullQueue 开始全量扫描备份", c.Str())
}

func (c *CDPExecutor) uploadIncrQueue() {
	go c.uploadDispatcher(false)
	logger.Fmt.Infof("%v.uploadIncrQueue 开始增量持续备份", c.Str())
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

	logger.Fmt.Infof("%v.uploadDispatcher exit，完成全量数据同步，进入【CDPING】保护状态", c.Str())
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

	if c.is2host() {
		return c.upload2host(ffm, fp)
	} else if c.is2s3() {
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

	r, _ := http.NewRequest("POST", c.storage.uploadSession, body)
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
		Bucket: aws.String(ffm.Bucket),
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
		c.fbp.fullQueueCloseOnce.Do(func() {
			close(c.fbp.fullQueue)
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
		c.ibp.incrQueueCloseOnce.Do(func() {
			close(c.ibp.incrQueue)
		})
	}()

	if err = c.startWatchers(); err != nil {
		return
	}

	go c.putFileEventWhenTimeout()

	for fwo := range c.ibp.watcherQueue {
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
			if !c.confObj.EnableVersion && c.is2host() {
				// 如果备机存在则删除
				_ = c.deleteFilesInRemote(fwo)
			}
			fallthrough
		case meta.Win32EventCreate:
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

func (c *CDPExecutor) startWalkers() (err error) {
	logger.Fmt.Infof("%v.startWalkers start walkers...", c.Str())

	if err = models.UpdateBackupTaskStatusByConfID(c.DBDriver.DB, c.confObj.ID, meta.CDPCOPYING); err != nil {
		logger.Fmt.Errorf("%v.scanNotify UpdateBackupTaskStatusByConfID err=%v", c.Str(), err)
		return
	}

	for _, v := range c.confObj.DirsMappingJson {
		w, e := NewWalker(v.Origin, v.Depth, 1, c.fbp.walkerQueue, c.fbp.counter)
		if e != nil {
			continue
		}
		c.fbp.walkers = append(c.fbp.walkers, w)
	}

	for _, w := range c.fbp.walkers {
		_ = w.Walk()
	}

	go func() {
		ticker := time.NewTicker(meta.DefaultRetryTimeInterval)
		for range ticker.C {
			if c.isWalkersCompleted() {
				close(c.fbp.walkerQueue)
				ticker.Stop()
			}
			time.Sleep(5 * time.Second)
		}
	}()
	return
}

func (c *CDPExecutor) startWatchers() (err error) {
	logger.Fmt.Infof("%v.startWatchers start watchers...", c.Str())

	for _, v := range c.confObj.DirsMappingJson {
		w, e := nt_notify.NewWatcher(v.Origin, v.Recursion, c.ibp.watcherQueue)
		if e != nil {
			continue
		}
		c.ibp.watchers = append(c.ibp.watchers, w)
	}

	for _, w := range c.ibp.watchers {
		_ = w.Start()
	}
	return
}

func (c *CDPExecutor) deleteFilesInRemote(fwo nt_notify.FileWatchingObj) (err error) {
	fs, e := models.QueryFilesByPath(c.DBDriver.DB, c.confObj.ID, fwo.Path)
	if e != nil {
		logger.Fmt.Errorf("%v.deleteFilesInRemote QueryFilesByPath ERR=%v", c.Str(), e)
	}

	keys := make([]string, 0)
	for _, f := range fs {
		keys = append(keys, f.Storage)
	}

	for _, file := range funk.UniqString(keys) {
		b64 := base64.StdEncoding.EncodeToString([]byte(file))
		url := fmt.Sprintf("%v/%v", c.storage.deleteSession, b64)
		resp, e := requests.Post(url)
		if e != nil {
			logger.Fmt.Errorf("%v.deleteFilesInRemote Post(%v) ERR=%v", c.Str(), url, e)
			err = e
		}
		if resp.R.StatusCode != http.StatusOK {
			err = errors.New("delete failed with 400 code")
		}
	}

	_ = models.DeleteByPath(c.DBDriver.DB, c.confObj.ID, fwo.Path)
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

	existed := models.ExistedHistoryVersionFile(c.DBDriver.DB, c.confObj.ID, ff.Path)
	if existed && c.confObj.EnableVersion {
		ff.Tag = tools.ConvertModTime2VersionFlag(e.Time)
		ff.Name = fmt.Sprintf("%s.%s", ff.Name, ff.Tag)
	} else if existed && !c.confObj.EnableVersion {
		if err = models.DeleteByPath(c.DBDriver.DB, c.confObj.ID, ff.Path); err != nil {
			return
		}
	} else {
		// do nothing
	}

	if c.is2host() {
		origin, remote, err := c.confObj.SpecifyTargetWhenHost(ff.Path)
		if err != nil {
			return fm, err
		}
		ff.Storage = tools.GenerateRemoteHostKey(origin, ff.Path, remote, tools.IsWin(c.confObj.TargetHostJson.Type))
	} else if c.is2s3() {
		origin, bucket, prefix, err := c.confObj.SpecifyTarget(ff.Path)
		if err != nil {
			return fm, err
		}
		ff.Bucket = bucket
		ff.Storage = tools.GenerateS3Key(c.confObj.ID, origin, ff.Path, prefix)
	} else {
		return fm, errors.New("unsupported target storage")
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
		_ = os.MkdirAll(meta.HandlerBaseDir, 0666)
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
