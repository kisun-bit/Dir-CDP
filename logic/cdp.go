package logic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/aws/session"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"github.com/journeymidnight/aws-sdk-go/service/s3/s3manager"
	gos3 "github.com/kisun-bit/go-s3"
	"github.com/kr/pretty"
	"github.com/panjf2000/ants/v2"
	"io"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/models"
	"jingrongshuan/rongan-fnotify/nt_notify"
	"jingrongshuan/rongan-fnotify/tools"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TargetType string

const (
	TTS3   = "s3"
	TTHost = "host"
	// TODO more...
)

// CDPExecutor 文件级CDP实现
// 首先文件级CDP的启动流程分为了下述3个状态
// 状态1：UNSTART（未启动状态）
// 状态2：COPYING（全量拷贝状态）
// 状态3：CDPING（持续保护状态）
//
// ****UNSTART****
// 条件:
// ConfigModel配置中没有对该目录做监控配置或对配置已禁用
// ****COPYING****
// 条件:
// ConfigModel配置中没有对该目录做监控配置，则先进行拷贝目录下所有文件
// 逻辑说明：
// 全量拷贝时，该目录下每完成拷贝一个文件，便向FileFlow中写入一个文件记录，
// 若此时，底层上报有新的文件发生变更，则判断，若该文件还没有完成全量拷贝，则不记录到FileFlow，否则，记录下来
// ****CDPING****
// 条件:
// 将上报的变更的文件上传，并记录到FileFlow
//
//
// 注意！！！！！！！！！！！！！！
// 在CDPING状态下执行恢复操作，如何处理？？？
// 恢复时，为恢复文件加上统一的恢复标志，这样一来，即使恢复至原路径，
// CDPExecutor也能在捕捉变更文件时，忽略掉恢复文件
type CDPExecutor struct {
	config       *models.ConfigModel
	dp           *models.DBProxy
	task         *models.BackupTaskModel
	incrQueue    chan models.FileFlowModel // 增量数据通道，该通道永久开放
	fullQueue    chan models.FileFlowModel // 全量数据通道，该通道在全量备份完成后关闭
	ctx          context.Context
	cancel       context.CancelFunc // 取消事件
	wg           *sync.WaitGroup
	isReload     bool // 当服务重启时，此属性为True
	targetType   TargetType
	Server       models.ClientInfo // 备份服务器信息
	Origin       models.ClientInfo // 备份源机信息
	Target       models.ClientInfo // 备份目标机
	StorageHost  models.TargetHost // 备份目标机存储信息
	StorageS3    models.TargetS3   // 备份目标对象存储信息
	HostSession  string
	Grep         *Grep
	S3Session    *session.Session
	S3Client     gos3.S3Client
	Watcher      *nt_notify.Win32Watcher
	WatcherQueue chan nt_notify.FileWatchingObj
	Walker       *Walker
	WalkerQueue  chan nt_notify.FileWatchingObj
	Pool         *ants.Pool
	StartTs      int64
}

func NewCDPExecutor(config *models.ConfigModel, dp *models.DBProxy) (ce *CDPExecutor, err error) {
	ce = new(CDPExecutor)
	ce.config = config
	ce.dp = dp
	ce.incrQueue = make(chan models.FileFlowModel, meta.BackupChanSize)
	ce.fullQueue = make(chan models.FileFlowModel, meta.BackupChanSize)
	ce.ctx, ce.cancel = context.WithCancel(context.Background())
	ce.wg = &sync.WaitGroup{}
	ce.StartTs = time.Now().Unix()
	logger.Fmt.Infof("init CDPExecutor is ok")
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

	ce.task = &task
	ce.isReload = true
	logger.Fmt.Infof("NewCDPExecutorWhenReload reload cdp executor(conf=%v) is ok.", config.ID)
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

	if err = cdp.Start(); err != nil {
		return
	}
	logger.Fmt.Infof("AsyncStartCDPExecutor IP(%v) ConfID(%v) start", ip, conf)

	return nil
}

func (c *CDPExecutor) Str() string {
	return fmt.Sprintf("<CDP(conf=%v, task=%v, dir=%v)>", c.config.ID, c.task.ID, c.config.Dir)
}

func (c *CDPExecutor) Start() (err error) {
	if !c.isReload {
		if err = c.initTaskOnce(); err != nil {
			return
		}
	}
	if err = c.dp.RegisterTable(c.config.ID); err != nil {
		logger.Fmt.Errorf("%v.Start RegisterTable ERR=%v", c.Str(), err)
		return
	}
	go func() {
		c.logic()
	}()
	return
}

func (c *CDPExecutor) logic() {
	var err error
	defer c.stopExecutorByError(err)

	if err = c.initArgs(); err != nil {
		logger.Fmt.Errorf("%v.logic initArgs err=%v", c.Str(), err)
		return
	}
	InitRecorder()

	/*对于启动流程的说明如下：
	启动任务方式有两种
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
	c.wg.Wait()
}

func (c *CDPExecutor) logicWhenReload() (err error) {
	logger.Fmt.Infof("%v.logicWhenReload【%v】重载任务 -> 全量(未完成同步的)+增量", c.Str(), c.task.Status)
	c.full()
	c.incr()
	return nil
}

func (c *CDPExecutor) logicWhenNormal() (err error) {
	// 历史任务处于meta.CDPUNSTART或meta.CDPCOPYING状态时，需执行全量+增量
	if c.task.Status == meta.CDPUNSTART || c.task.Status == meta.CDPCOPYING {
		if err = models.UpdateBackupTaskStatusByConfID(c.dp.DB, c.config.ID, meta.CDPCOPYING); err != nil {
			return
		}
		logger.Fmt.Infof("%v.logicWhenNormal【COPYING】-> 全量(未完成同步的)+增量", c.Str())
		c.full()
		c.incr()
		return nil
	}

	// 历史任务处于meta.CDPCDPING状态，且表记录为空时，需执行全量+增量（此分支仅用于调试环境做测试时出现）
	if c.task.Status == meta.CDPCDPING && models.IsEmptyTable(c.dp.DB, c.config.ID) {
		if err = models.UpdateBackupTaskStatusByConfID(c.dp.DB, c.config.ID, meta.CDPCOPYING); err != nil {
			return
		}
		logger.Fmt.Warnf("%v.logicWhenNormal convert status from `%v` to `COPYING`", c.Str(), c.task.Status)
		logger.Fmt.Infof("%v.logicWhenNormal【COPYING】--> 全量(未完成同步的)+增量", c.Str())
		c.full()
		c.incr()
		return nil
	}

	// 历历史任务处于meta.CDPCDPING状态，且表记录不为空时，仅执行增量即可
	if c.task.Status == meta.CDPCDPING && !models.IsEmptyTable(c.dp.DB, c.config.ID) {
		logger.Fmt.Infof("%v.logicWhenNormal【%v】--> 增量", c.Str(), c.task.Status)
		close(c.fullQueue)
		logger.Fmt.Infof("%v.logicWhenNormal close FullQueue", c.Str())
		c.incr()
		return nil
	}

	logger.Fmt.Errorf("%v.logicWhenNormal error status=%v", c.Str(), c.task.Status)
	panic("undefined task status")
}

func (c *CDPExecutor) initArgs() (err error) {
	// 黑白名单过滤
	c.Grep = NewGrep(c.config.Include, c.config.Exclude)

	// 备份服务器信息
	if c.Server, err = models.QueryHostInfoByHostID(c.dp.DB, c.config.Server); err != nil {
		return
	}
	logger.Fmt.Infof("%v.initArgs Server ->%v", c.Str(), c.Server)

	// 源机信息
	if c.Origin, err = models.QueryHostInfoByHostID(c.dp.DB, c.config.Origin); err != nil {
		return
	}
	logger.Fmt.Infof("%v.initArgs Origin ->%v", c.Str(), c.Origin)

	if models.Is2Host(*c.config) {
		// 目标机信息
		if c.Target, err = models.QueryTargetHostInfoByConf(c.dp.DB, c.config.ID); err != nil {
			return
		}
		logger.Fmt.Infof("%v.initArgs 2h TargetHost ->%v", c.Str(), c.Target)

		// 目标机目标存储路径
		if c.StorageHost, err = models.QueryTargetConfHostByConf(c.dp.DB, c.config.ID); err != nil {
			return
		}
		logger.Fmt.Infof("%v.initArgs 2h TargetConf ->%v", c.Str(), c.StorageHost)

		// 目标机上传接口地址
		c.HostSession = fmt.Sprintf("http://%s:%v/api/v1/upload", c.Target.Address, meta.AppPort)
		logger.Fmt.Infof("%v.initArgs 2h UploadSession ->%v", c.Str(), c.HostSession)
		c.targetType = TTHost

	} else if models.Is2S3(*c.config) {
		// 目标对象存储相关配置
		if c.StorageS3, err = models.QueryTargetConfS3ByConf(c.dp.DB, c.config.ID); err != nil {
			return
		}
		logger.Fmt.Infof("%v.initArgs 2s3 TargetS3Conf ->%v", c.Str(), c.StorageS3)

		// 创建目标对象存储目标桶
		c.S3Session, c.S3Client = tools.NewS3Client(
			c.StorageS3.TargetConfS3.AccessKey,
			c.StorageS3.TargetConfS3.SecretKey,
			c.StorageS3.TargetConfS3.Endpoint,
			c.StorageS3.TargetConfS3.Region,
			c.StorageS3.TargetConfS3.SSL,
			c.StorageS3.TargetConfS3.Path)
		c.targetType = TTS3
		if err = c.createBucket(c.StorageS3.TargetConfS3.Bucket); err != nil {
			return
		}

	} else {
		return fmt.Errorf("unsupported config-target(%v)", c.config.Target)
	}

	// 初始化备份线程池
	numCores := int(c.config.Cores)
	if numCores < 0 {
		numCores = runtime.NumCPU()
	}
	if c.Pool, err = ants.NewPool(numCores); err != nil {
		logger.Fmt.Errorf("%v.initArgs NewPool ERR=%v", c.Str(), err)
		return
	}

	// TODO more... 更多的初始化参数操作
	return nil
}

func (c *CDPExecutor) createBucket(bucket string) (err error) {

	_, err = c.S3Client.Client.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err == nil { // existed
		logger.Fmt.Warnf("%v.createBucket bucket %s has already created", c.Str(), bucket)
		return
	}

	_, err = c.S3Client.Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		logger.Fmt.Errorf("%v.createBucket failed to create bucket %s. err=%v", c.Str(), bucket, err)
		return err
	}

	err = c.S3Client.Client.WaitUntilBucketExists(&s3.HeadBucketInput{
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
	return c.targetType == TTS3
}

func (c *CDPExecutor) is2host() bool {
	return c.targetType == TTHost
}

func (c *CDPExecutor) uploadQueue2Storage() {
	c.uploadFullQueue()
	c.uploadIncrQueue()
}

func (c *CDPExecutor) uploadFullQueue() {
	go c.uploadDispatcher(true)
}

func (c *CDPExecutor) uploadIncrQueue() {
	go c.uploadDispatcher(false)
}

func (c *CDPExecutor) uploadDispatcher(full bool) {
	var (
		err   error
		queue = c.incrQueue
	)

	defer c.stopExecutorByError(err)
	if full {
		queue = c.fullQueue
		logger.Fmt.Infof("%v.uploadDispatcher consuming fullQueue", c.Str())
	} else {
		logger.Fmt.Infof("%v.uploadDispatcher consuming incrQueue", c.Str())
	}

	for fwo := range queue {
		if !c.isValidPath(fwo.Path, fwo.Time) {
			continue
		}
		logger.Fmt.Debugf("[上传] <<<<<<<<<<<<<<<<<<<<<<<<< %v", fwo.Path)
		select {
		case <-c.ctx.Done():
			logger.Fmt.Warnf("%v.uploadDispatcher !!!!!!!!!!!!!!!!!!!! 取消事件", c.Str())
			return
		default:
			c.wg.Add(1)
			if err = c.uploadOneFile(fwo); err != nil {
				return
			}
		}
	}

	if !full {
		return
	}

	err = models.UpdateBackupTaskStatusByConfID(c.dp.DB, c.config.ID, meta.CDPCDPING)
	if err != nil {
		logger.Fmt.Errorf("%v.uploadDispatcher failed to convert CDPING status, err=%v", c.Str(), err)
		return
	}
	logger.Fmt.Infof("%v.uploadDispatcher exit and task(%v) in CDPING status", c.Str(), c.task.ID)
}

func (c *CDPExecutor) uploadOneFile(fwo models.FileFlowModel) (err error) {
	return c.Pool.Submit(func() {
		var (
			err  error
			last models.FileFlowModel
		)

		defer c.wg.Done()
		defer c.stopExecutorByError(err)

		/* 存在同名文件的上传逻辑

		找到最近一次的同名文件记录:
		1. 处于FINISHED或ERROR状态：
		   直接上传
		2. 处于SYNCING或WATCHED状态：
		   TODO 暂时不做处理，后续优化
		未找到：
		   直接上传
		*/
		last, err = models.QueryLastSameNameFileExcludeSelf(c.dp.DB, c.config.ID, fwo.Path)
		if err != nil {
			logger.Fmt.Errorf("%v.uploadOneFile QueryLastSameNameFile ERR=%v", c.Str(), err)
			return
		}
		if last.ID != 0 && (last.Status == meta.FFStatusSyncing || last.Status == meta.FFStatusWatched) {
			logger.Fmt.Debugf("%v.uploadOneFile waiting last(%v) syncing until finished", c.Str(), last.ID)
			// TODO 是返回还是阻塞等待历史文件传输完毕呢？
			return
		}
		if !models.IsEnable(c.dp.DB, c.config.ID) {
			return
		}
		if err := c.upload2DiffStorage(fwo); err != nil {
			logger.Fmt.Warnf("%v.uploadOneFile _handleFileFlow ERR=%v", c.Str(), err)
			// TODO 删除远端文件
			err_ := models.UpdateFileFlowStatus(c.dp.DB, c.config.ID, fwo.ID, meta.FFStatusError)
			if err_ != nil {
				logger.Fmt.Errorf("%v.uploadOneFile UpdateFileFlowStatus (f%v) ERR=%v", c.Str(), fwo.ID, err_)
			}
			return
		}
	})
}

func (c *CDPExecutor) upload2DiffStorage(ffm models.FileFlowModel) (err error) {
	defer func() {
		if err == nil {
			if err = models.UpdateFileFlowStatus(
				c.dp.DB, c.config.ID, ffm.ID, meta.FFStatusFinished); err != nil {
				logger.Fmt.Errorf("%v.upload2DiffStorage UpdateFileFlowStatus (f%v) to `FINISHED` ERR=%v", c.Str(), err)
				return
			}
		}
	}()

	if err = models.UpdateFileFlowStatus(
		c.dp.DB, c.config.ID, ffm.ID, meta.FFStatusSyncing); err != nil {
		logger.Fmt.Errorf("%v.upload2DiffStorage UpdateFileFlowStatus (f%v) to `SYNCING` ERR=%v", c.Str(), ffm.ID, err)
		return
	}

	fp, err := os.Open(ffm.Path)
	if err != nil {
		return err
	}
	defer fp.Close()

	if models.Is2Host(*c.config) {
		return c.upload2host(ffm, fp)
	} else if models.Is2S3(*c.config) {
		return c.upload2s3(ffm, fp)
	} else {
		c.cancel()
		return fmt.Errorf("unsupported config-target(%v)", c.config.Target)
	}
}

func (c *CDPExecutor) upload2host(ffm models.FileFlowModel, fp io.Reader) (err error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, _ := writer.CreateFormFile("filename", filepath.Join(ffm.Parent, ffm.Name+ffm.Tag))
	if _, err = io.Copy(part, fp); err != nil {
		return
	}
	if err = writer.Close(); err != nil {
		return
	}

	r, _ := http.NewRequest("POST", c.HostSession, body)
	r.Header.Add("Content-Type", writer.FormDataContentType())
	client := &http.Client{}
	if _, err = client.Do(r); err != nil {
		return err
	}
	return nil
}

func (c *CDPExecutor) upload2s3(ffm models.FileFlowModel, fp io.Reader) (err error) {
	uploader := s3manager.NewUploader(
		c.S3Session,
		func(u *s3manager.Uploader) {
			u.MaxUploadParts = s3manager.MaxUploadParts
		},
	)

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(c.StorageS3.TargetConfS3.Bucket),
		Key:    aws.String(tools.GenerateS3Key(filepath.Join(ffm.Parent, ffm.Name+ffm.Tag), c.config.ID)),
		Body:   fp,
	})
	if err != nil {
		logger.Fmt.Errorf("%v.upload2s3 Unable to upload file %s | %v", c.Str(), ffm.Path, err)
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
	defer c.stopExecutorByError(err)
	defer close(c.fullQueue)

	if err = c.startWalker(); err != nil {
		return
	}

	for fwo := range c.WalkerQueue {
		select {
		case <-c.ctx.Done():
			logger.Fmt.Warnf("%v.scanNotify !!!!!!!!!!!!!!!!!!!! 取消事件", c.Str())
			return
		default:
			if err = c.putFile2FullOrIncrQueue(fwo, true); err != nil {
				return
			}
		}
	}

	logger.Fmt.Infof("%v.scanNotify close FullQueue", c.Str())
}

// 由于连续变更的文件集，将其压入incrQueue时，必须是有新的不同的变更记录来驱动last其压入incrQueue
// 所以，针对最后一个文件变更，由于没有c.WatcherQueue驱动上传，就需要另开一个线程来收尾
type Recorder struct {
	m *sync.Map
}

func (r *Recorder) Store(v interface{}) {
	r.m.Store("last", v)
	r.m.Store("update_time", time.Now().Unix())
}

func (r *Recorder) Del() {
	r.m.Delete("last")
	r.m.Delete("update_time")
}

func (r *Recorder) Load() (nt_notify.FileWatchingObj, int64, bool) {
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

var rd *Recorder
var locker_ sync.Once

func InitRecorder() {
	locker_.Do(func() {
		rd = new(Recorder)
		rd.m = new(sync.Map)
	})
}

func (c *CDPExecutor) fsNotify() {
	var (
		err error
	)

	defer c.stopExecutorByError(err)
	if err = c.startWatcher(); err != nil {
		return
	}

	go c.putFileEventWhenTimeout()

	for {
		select {
		case <-c.ctx.Done():
			logger.Fmt.Warnf("%v.fsNotify !!!!!!!!!!!!!!!!!!!! 取消事件", c.Str())
			return
		case fwo := <-c.WatcherQueue:
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
				//logger.Fmt.Debugf("[捕捉] >>>>>>>>>>>>>>>>>>> %v | %v", fwo.Path, fwo.Event)
				if last, _, ok := rd.Load(); !ok {
					last = fwo
					rd.Store(fwo)
					break
				} else {
					// 捕捉到新的不同名文件的变更记录
					if fwo.Path != last.Path {
						err = c.putFile2FullOrIncrQueue(last, false)
						if err != nil {
							return
						}
						rd.Store(fwo) // 用新文件覆盖掉旧文件记录
					} else {
						rd.Store(fwo) // 必须对同名记录做一次更新Event，否则会影响文件的mtime属性
					}
				}
			}
		}
	}
}

func (c *CDPExecutor) startWalker() (err error) {
	logger.Fmt.Infof("%v.startWalker start walker...", c.Str())

	if err = models.UpdateBackupTaskStatusByConfID(c.dp.DB, c.config.ID, meta.CDPCOPYING); err != nil {
		logger.Fmt.Errorf("%v.scanNotify UpdateBackupTaskStatusByConfID err=%v", c.Str(), err)
		return
	}

	c.Walker = NewWalker(
		c.config.Dir,
		int(c.config.Depth),
		int(c.config.Cores),
		c.config.Include,
		c.config.Exclude,
		*c.task.Start,
		int(c.config.ValidDays))

	c.WalkerQueue, err = c.Walker.Walk()
	if err != nil {
		logger.Fmt.Errorf("%v.scanNotify Walk Error=%s", c.Str(), err.Error())
		return
	}
	return
}

func (c *CDPExecutor) startWatcher() (err error) {
	logger.Fmt.Infof("%v.startWatcher start watcher...", c.Str())

	c.Watcher, err = nt_notify.NewWatcher(c.config.Dir, c.config.Recursion)
	if err != nil {
		logger.Fmt.Errorf("%v.fsNotify NewWatcher Error=%s", c.Str(), err.Error())
		return
	}
	logger.Fmt.Infof("%v.fsNotify init watcher(%v) is ok", c.Str(), c.config.Dir)

	c.WatcherQueue, err = c.Watcher.Start()
	if err != nil {
		logger.Fmt.Errorf("%v.fsNotify Start Error=%s", c.Str(), err.Error())
		return
	}
	logger.Fmt.Infof("%v.fsNotify start watcher(%v)...", c.Str(), c.config.Dir)
	return
}

func (c *CDPExecutor) putFile2FullOrIncrQueue(fwo nt_notify.FileWatchingObj, full bool) (err error) {
	defer func() {
		if err != nil {
			logger.Fmt.Errorf("putFile2FullOrIncrQueue err=%v", err)
			return
		}
	}()
	if strings.Contains(fwo.Name, meta.RestoreSuffix) {
		return
	}
	fh, err := models.QueryLastSameNameFile(c.dp.DB, c.config.ID, fwo.Path)
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
		1. 若fh.Create >= c.StartTs, 则表示文件对象fh是在本次CDP启动之后才捕捉到的
		   分支一： ERROR状态 ---> 处理
		   分支二： 非ERROR状态且fh.timestamp与fwo.timestamp不相同 ---> 处理
		   分支三： 非ERROR状态且fh.timestamp与fwo.timestamp相同 ---> 不处理
		2. 若fh.Create < c.StartTs, 则表示文件对象fh是在本次CDP启动之前才捕捉到的
		   分支四： 处理
		*/
		if fh.ID != 0 && fh.Create >= c.StartTs && fh.Status != meta.FFStatusError && fh.Time == fwo.Time {
			return
		}
	} else {
		logger.Fmt.Errorf("%v.uploadDispatcher \nfwo:%v\nfh:%v",
			c.Str(), pretty.Sprint(fwo), pretty.Sprint(fh))
		err = errors.New("invalid upload condition")
	}

	fm, err := c.notifyOneFileEvent(fwo)
	if err != nil {
		logger.Fmt.Errorf("%v.fsNotify notifyOneFileEvent Error=%s", c.Str(), err.Error())
		return err
	}
	if !full {
		c.incrQueue <- fm
	} else {
		c.fullQueue <- fm
	}
	return
}

func (c *CDPExecutor) isValidPath(path string, mtime int64) bool {
	if c.config.Depth != -1 && int64(strings.Count(filepath.Dir(path), meta.Sep)) > c.config.Depth {
		return false
	}
	if !c.Grep.IsValidByGrep(path) {
		return false
	}
	if c.config.ValidDays != -1 && mtime+24*60*60*c.config.ValidDays > c.task.Start.Unix() {
		return false
	}
	return true
}

func (c *CDPExecutor) initTaskOnce() (err error) {
	c_, err := models.QueryBackupTaskByConfID(c.dp.DB, c.config.ID)
	if err == nil {
		c.task = &c_
		return nil
	}

	_time := time.Now()
	c.task = new(models.BackupTaskModel)
	c.task.Start = &_time
	c.task.Trigger = meta.TriggerMan
	c.task.ConfID = c.config.ID
	c.task.Status = meta.CDPUNSTART
	return models.CreateBackupTaskModel(c.dp.DB, c.task)
}

func (c *CDPExecutor) notifyOneFileEvent(e nt_notify.FileWatchingObj) (fm models.FileFlowModel, err error) {
	ff := new(models.FileFlowModel)
	ff.Event = meta.EventCode[e.Event]
	ff.Create = time.Now().Unix()
	ff.ConfID = c.config.ID
	ff.Time = e.Time
	ff.Mode = int(e.Mode)
	ff.Size = e.Size
	ff.Path, _ = filepath.Abs(e.Path)
	ff.Name = filepath.Base(e.Path)
	ff.Version = nt_notify.GenerateVersion(c.config.ID, e)
	ff.Type = int(e.Type)
	ff.Status = meta.FFStatusWatched
	ff.Parent = tools.CorrectDirWithPlatform(filepath.Dir(e.Path), meta.IsWin)

	// 捕捉到已存在文件的变更通知，则为其加入"版本标记"
	if models.ExistedHistoryVersionFile(c.dp.DB, c.config.ID, ff.Path) {
		ff.Tag = tools.ConvertModTime2VersionFlag(e.Time)
		ff.Name += "." + ff.Tag
	}

	switch c.targetType {
	case TTHost:
		ff.Storage = filepath.Join(
			c.StorageHost.TargetConfHost.RemotePath,
			strconv.FormatInt(c.config.ID, 0xa),
			strings.Replace(ff.Parent, c.config.Dir, "", 1),
			ff.Name,
		)
		ff.Storage = tools.CorrectPathWithPlatform(ff.Storage, tools.IsWin(c.Target.Type))
	case TTS3:
		ff.Storage = tools.GenerateS3Key(ff.Path, c.config.ID)
	}

	err = models.CreateFileFlowModel(c.dp.DB, c.config.ID, ff)
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
		defer c.stopExecutorByError(err)

		for {
			time.Sleep(5 * time.Second)
			if fwo, t, ok = rd.Load(); !ok {
				continue
			}

			if time.Now().Unix()-t > 5 {
				if err = c.putFile2FullOrIncrQueue(fwo, false); err != nil {
					return
				}
				rd.Del()
			}
		}
	}()
}

func (c *CDPExecutor) stopExecutorByError(err error) {
	if err != nil {
		c.cancel()
	}
}
