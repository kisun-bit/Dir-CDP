package logic

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/aws/session"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"github.com/journeymidnight/aws-sdk-go/service/s3/s3manager"
	"github.com/panjf2000/ants/v2"
	"io"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/models"
	"jingrongshuan/rongan-fnotify/tools"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 恢复任务
type RestoreTask struct {
	confObj        *models.ConfigModel
	taskObj        *models.RestoreTaskModel
	DBDriver       *models.DBProxy
	keyArgs        *models.RestoreExtInfo
	filter         *Grep
	queue          chan models.EventFileModel
	pool           *ants.Pool
	monitor        *hangEventMonitor
	exitNotify     chan ErrorCode
	exitNotifyOnce *sync.Once
	stopSignal     *int32
}

func NewRestoreTask(task *models.RestoreTaskModel, dp *models.DBProxy) (r *RestoreTask, err error) {
	r = new(RestoreTask)

	r.DBDriver = dp
	r.taskObj = task
	r.confObj = new(models.ConfigModel)
	r.keyArgs = new(models.RestoreExtInfo)

	r.exitNotifyOnce = new(sync.Once)
	r.stopSignal = new(int32)
	r.monitor = NewMonitorWithRestore(r)

	*r.keyArgs, err = r.taskObj.ExtInfos()
	if err != nil {
		return r, err
	}

	r.queue = make(chan models.EventFileModel, meta.DefaultDRQueueSize)
	if r.pool, err = NewPoolWithCores(r.keyArgs.Threads); err != nil {
		return
	}
	return r, nil
}

func (r *RestoreTask) Str() string {
	return fmt.Sprintf("<RestoreTask(ID=%v)>", r.taskObj.ID)
}

func (r *RestoreTask) isStopped() bool {
	return atomic.LoadInt32(r.stopSignal) == 1
}

func (r *RestoreTask) stop() {
	logger.Fmt.Infof("%v.stop 正在停止恢复", r.Str())

	if r.isStopped() {
		return
	}
	atomic.StoreInt32(r.stopSignal, 1)
}

func (r *RestoreTask) paramFileset() (fs []string) {
	if r.keyArgs.Fileset == "" {
		return
	}
	return strings.Split(r.keyArgs.Fileset, meta.SplitFlag)
}

func (r *RestoreTask) paramStartTime() *time.Time {
	if r.keyArgs.Starttime == "" {
		return nil
	}
	t_ := tools.String2Time(r.keyArgs.Starttime)
	return &t_
}

func (r *RestoreTask) paramEndTime() *time.Time {
	if r.keyArgs.Endtime == "" {
		return nil
	}
	t_ := tools.String2Time(r.keyArgs.Endtime)
	return &t_
}

func (r *RestoreTask) Start() (err error) {
	go func() {
		r.logic()
	}()
	return nil
}

func (r *RestoreTask) logic() {
	logger.Fmt.Infof("%v.logic 开始执行恢复逻辑", r.Str())
	go r.monitorStopNotify()

	var err error
	defer r.exitWhenErr(ExitErrCodeOriginErr, err)

	if err = r.initArgs(); err != nil {
		return
	}

	if len(r.paramFileset()) != 0 {
		err = r.restoreByFileset()
	} else {
		err = r.restoreByTime()
	}
	r.restore()
}

func (r *RestoreTask) initArgs() (err error) {
	r.filter = NewGrep(r.keyArgs.Include, r.keyArgs.Exclude)

	if *r.confObj, err = models.QueryConfig(r.DBDriver.DB, r.taskObj.ConfID); err != nil {
		return
	}
	return nil
}

func (r *RestoreTask) monitorStopNotify() {
	go func() {
		r.monitor.monitor()
	}()
}

func (r *RestoreTask) restoreByFileset() (err error) {
	for _, f := range r.paramFileset() {
		if err = r.restoreOneSet(f); err != nil {
			return err
		}
	}
	return nil
}

func (r *RestoreTask) restoreOneSet(path_ string) (err error) {
	var (
		dir bool
		row *sql.Rows
		ffm models.EventFileModel
	)

	dir = strings.HasSuffix(path_, meta.Sep)
	if dir {
		if row, err = models.QueryRecursiveFilesIteratorInDir(r.DBDriver.DB, r.taskObj.ConfID, path_); err != nil {
			logger.Fmt.Errorf("RestoreTask.restoreOneSet invalid dir %s", path_)
			return err
		}
		defer row.Close()
		for row.Next() {
			if err = r.DBDriver.DB.ScanRows(row, &ffm); err != nil {
				logger.Fmt.Errorf("RestoreTask.restoreOneSet File=%v ScanRows-Err=%v", ffm.Path, err)
				return err
			}
			r.queue <- ffm
		}
		return
	}

	if ffm, err = models.QueryFileByName(r.DBDriver.DB, r.taskObj.ConfID, path_); err != nil {
		logger.Fmt.Errorf("RestoreTask.restoreOneSet. QueryFileByName-Err=%v", ffm)
		return err
	}

	if !r.isStopped() {
		r.queue <- ffm
	}
	return nil
}

func (r *RestoreTask) restoreByTime() (err error) {
	var (
		row *sql.Rows
		ffm models.EventFileModel
	)

	if row, err = models.QueryFileIteratorByTime(
		r.DBDriver.DB, r.taskObj.ConfID, r.paramStartTime(), r.paramEndTime()); err != nil {
		logger.Fmt.Errorf("RestoreTask.restoreByTime QueryFileIteratorByTime-Error=%v", err)
		return err
	}
	defer row.Close()

	for row.Next() {
		if err = r.DBDriver.DB.ScanRows(row, &ffm); err != nil {
			logger.Fmt.Errorf("RestoreTask RestoreByFilterArgs. ScanRows-Err=%v", ffm)
			return err
		}
		r.queue <- ffm
	}
	return nil
}

func (r *RestoreTask) restore() {
	go func() {
		r.download()
	}()
}

func (r *RestoreTask) download() {
	var (
		err error
		tcs models.TargetS3
		s3s *session.Session
		thi models.ClientInfo
		url string
	)

	defer func() {
		if err != nil {
			// TODO 任务状态
		} else {
			// TODO 任务状态
		}
	}()

	if models.Is2S3(*r.confObj) {
		if tcs, err = models.QueryTargetConfS3ByConf(r.DBDriver.DB, r.taskObj.ConfID); err != nil {
			return
		}
		s3s, _ = tools.NewS3Client(
			tcs.TargetConfS3.AccessKey,
			tcs.TargetConfS3.SecretKey,
			tcs.TargetConfS3.Endpoint,
			tcs.TargetConfS3.Region,
			tcs.TargetConfS3.SSL,
			tcs.TargetConfS3.Path)
	} else {
		if thi, err = models.QueryTargetHostInfoByConf(r.DBDriver.DB, r.taskObj.ConfID); err != nil {
			return
		}
		url = fmt.Sprintf("http://%v:%v/api/v1/download/", thi.Address, meta.AppPort)
	}

	for ffm := range r.queue {
		_ = r.pool.Submit(func() {
			r.downloadOneFile(ffm, tcs, s3s, thi, url)
		})
	}

	<-r.exitNotify
}

func (r *RestoreTask) downloadOneFile(ffm models.EventFileModel, tcs models.TargetS3, s3s *session.Session,
	thi models.ClientInfo, url string) {
	var err error
	if !r.filter.IsValidByGrep(ffm.Path) {
		return
	}

	local := strings.Replace(ffm.Path, r.confObj.Dir, r.keyArgs.RestoreDir, 1) + meta.IgnoreFlag
	target, err := os.OpenFile(local, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		logger.Fmt.Errorf("RestoreTask DownloadOneFile. OpenFile-Error=%v", err)
		return
	}

	if s3s != nil {
		downloader := s3manager.NewDownloader(
			s3s,
			func(downloader_ *s3manager.Downloader) {
				downloader_.Concurrency = s3manager.DefaultDownloadConcurrency
			},
		)
		if err = r._downloadFromS3(ffm, target, downloader, tcs.TargetConfS3.Bucket); err != nil {
			return
		}
	} else {
		if err = r._downloadFromHost(ffm, target, url); err != nil {
			return
		}
	}

	truePath := strings.TrimSuffix(local, meta.IgnoreFlag)
	if err = os.Rename(local, truePath); err != nil {
		return
	}
	if err = os.Chtimes(truePath, time.Now(), time.Unix(ffm.Time, 0)); err != nil {
		return
	}
}

func (r *RestoreTask) _downloadFromS3(
	ffm models.EventFileModel, local *os.File, downloader *s3manager.Downloader, bucket string) (err error) {
	defer local.Close()

	_, err = downloader.Download(local,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(ffm.Storage),
		})
	return err
}

func (r *RestoreTask) _downloadFromHost(
	ffm models.EventFileModel, local *os.File, url string) (err error) {
	defer local.Close()

	url += base64.StdEncoding.EncodeToString([]byte(ffm.Storage))
	var r_ *http.Response
	if r_, err = http.Get(url); err != nil {
		return
	}

	if _, err = io.Copy(local, r_.Body); err != nil {
		return err
	}
	return nil
}

func (r *RestoreTask) exitWhenErr(code ErrorCode, err error) {
	if err == nil {
		return
	}

	r.exitNotifyOnce.Do(func() {
		logger.Fmt.Errorf("%v.exitWhenErr 捕捉到%v, 恢复任务等待退出...", r.Str())
		close(r.queue)
		for {
			if err = models.UpdateRestoreTask(r.DBDriver.DB, r.taskObj.ID, meta.RESTORESERROR); err != nil {
				logger.Fmt.Warnf("%v.UpdateRestoreTask Err=%v, wait 10s...", r.Str(), err)
				time.Sleep(10 * time.Second)
			}
			break
		}
		r.exitNotify <- code
	})
}
