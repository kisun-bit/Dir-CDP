package logic

import (
	"database/sql"
	"fmt"
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/aws/session"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"github.com/journeymidnight/aws-sdk-go/service/s3/s3manager"
	"github.com/kr/pretty"
	"github.com/panjf2000/ants/v2"
	"io"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/models"
	"jingrongshuan/rongan-fnotify/tools"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
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
	filter         *Grep
	wg             *sync.WaitGroup
	reporter       *Reporter
	progress       *Progress
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
	r.wg = new(sync.WaitGroup)
	r.confObj = new(models.ConfigModel)
	if *r.confObj, err = models.QueryConfig(r.DBDriver.DB, r.taskObj.ConfID); err != nil {
		return
	}

	r.exitNotifyOnce = new(sync.Once)
	r.stopSignal = new(int32)
	r.monitor = NewMonitorWithRestore(r)
	r.reporter = NewReporter(dp.DB, r.confObj.ID, r.taskObj.ID, meta.TaskTypeRestore)
	_ = r.reporter.ReportInfo(StepInitRestoreTask)

	if err = r.taskObj.LoadJsonFields(); err != nil {
		_ = r.reporter.ReportError(StepLoadArgsF, err)
		return
	}
	if err = r.confObj.LoadsJsonFields(dp.DB); err != nil {
		_ = r.reporter.ReportError(StepLoadArgsF, err)
		return
	}
	_ = r.reporter.ReportInfo(StepLoadArgs)

	r.progress = NewProgress(meta.DefaultReportProcessSecs,
		r.taskObj.ID, r.DBDriver.DB, r.confObj.ExtInfoJson.ServerAddress, meta.TaskTypeRestore)
	r.queue = make(chan models.EventFileModel, meta.DefaultDRQueueSize)
	if r.taskObj.ExtInfoJson.Threads <= 0 {
		r.taskObj.ExtInfoJson.Threads = runtime.NumCPU()
	}
	if r.pool, err = NewPoolWithCores(r.taskObj.ExtInfoJson.Threads); err != nil {
		_ = r.reporter.ReportError(StepInitRestorePoolF, err)
		return
	}
	_ = r.reporter.ReportInfo(StepInitRestorePool)
	return r, nil
}

func (r *RestoreTask) Str() string {
	return fmt.Sprintf("<RestoreTask(ID=%v)>", r.taskObj.ID)
}

func (r *RestoreTask) isStopped() bool {
	return atomic.LoadInt32(r.stopSignal) == 1
}

func (r *RestoreTask) stop() {
	if r.isStopped() {
		return
	}
	atomic.StoreInt32(r.stopSignal, 1)
}

func (r *RestoreTask) paramFileset() (fs []string) {
	if r.taskObj.ExtInfoJson.Fileset == "" {
		return
	}
	return strings.Split(r.taskObj.ExtInfoJson.Fileset, meta.SplitFlag)
}

func (r *RestoreTask) paramStartTime() *time.Time {
	if r.taskObj.ExtInfoJson.Starttime == "" {
		return nil
	}
	t_ := tools.String2Time(r.taskObj.ExtInfoJson.Starttime)
	return &t_
}

func (r *RestoreTask) paramEndTime() *time.Time {
	if r.taskObj.ExtInfoJson.Endtime == "" {
		return nil
	}
	t_ := tools.String2Time(r.taskObj.ExtInfoJson.Endtime)
	return &t_
}

func (r *RestoreTask) Start() (err error) {
	go func() {
		r.logic()
	}()
	return nil
}

func (r *RestoreTask) logic() {
	logger.Fmt.Infof("%v.logic TASK_EXT: %v", r.Str(), pretty.Sprint(r.taskObj.ExtInfoJson))
	defer close(r.queue)

	_ = r.reporter.ReportInfo(StepStartMonitor)
	go r.monitorStopNotify()

	var err error
	defer r.exitWhenErr(ExitErrCodeOriginErr, err)

	if err = r.initArgs(); err != nil {
		return
	}

	r.restore()

	if len(r.paramFileset()) != 0 {
		_ = r.reporter.ReportInfo(StepRestoreByFileset)
		err = r.restoreByFileset()
	} else {
		_ = r.reporter.ReportInfo(StepRestoreByTime)
		err = r.restoreByTime()
	}
}

func (r *RestoreTask) initArgs() (err error) {
	r.filter = NewGrep(r.taskObj.ExtInfoJson.Include, r.taskObj.ExtInfoJson.Exclude)
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

	if r.isStopped() {
		return nil
	}

	dir = strings.HasSuffix(path_, meta.Sep)
	if dir {
		if row, err = models.QueryRecursiveFilesIteratorInDir(r.DBDriver.DB, r.taskObj.ConfID, path_); err != nil {
			logger.Fmt.Errorf("RestoreTask.restoreOneSet invalid dir %s", path_)
			return err
		}
		defer row.Close()
		for row.Next() {
			if r.isStopped() {
				return nil
			}
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

	r.queue <- ffm
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
		if r.isStopped() {
			return nil
		}
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
		s3s *session.Session
		url string
		err error
	)

	defer func() {
		if err != nil {
			// TODO 任务状态
		} else {
			// TODO 任务状态
		}
	}()

	if r.confObj.TargetJson.TargetType == meta.WatchingConfTargetS3 {
		s3s, _ = tools.NewS3Client(
			r.confObj.S3ConfJson.AK,
			r.confObj.S3ConfJson.SK,
			r.confObj.S3ConfJson.Endpoint,
			r.confObj.S3ConfJson.Region,
			r.confObj.S3ConfJson.SSL,
			r.confObj.S3ConfJson.Style == "path")
	} else {
		url = fmt.Sprintf("http://%v:%v/api/v1/download", r.confObj.TargetHostJson.Address, meta.ConfigSettings.ServicePort)
	}

	_ = r.reporter.ReportInfo(StepStartTransfer)
	for ffm := range r.queue {
		if r.isStopped() {
			break
		}
		_ = r.pool.Submit(func() {
			r.downloadOneFile(ffm, s3s, url)
		})
	}

	for {
		if r.pool.Running() != 0 {
			time.Sleep(meta.DefaultMonitorRestoreHang)
		} else {
			break
		}
	}
	r.pool.Release()
	_ = r.reporter.ReportInfo(StepEndTransfer)

	if r.isStopped() {
		_ = models.EndRestoreTask(r.DBDriver.DB, r.taskObj.ID, false)
	} else {
		_ = models.EndRestoreTask(r.DBDriver.DB, r.taskObj.ID, true)
	}
	_ = r.reporter.ReportInfo(StepClearTask)
}

func (r *RestoreTask) downloadOneFile(ffm models.EventFileModel, s3s *session.Session, url string) {
	if !r.filter.IsValidByGrep(ffm.Path) {
		return
	}

	var err error
	defer r.exitWhenErr(ExitErrCodeOriginErr, err)

	if err = r._downloadWithRetry(ffm, meta.DefaultTransferRetryTimes, s3s, url); err != nil {
		logger.Fmt.Warnf("%v._downloadWithRetry ERR=%v", r.Str(), err)
	}
}

func (r *RestoreTask) _downloadWithRetry(ffm models.EventFileModel, retry int, s3s *session.Session, url string) (err error) {
	var restoreDir string
	var backupDir string
	var storageBucket string

	restoreDir, _, err = r.taskObj.SpecifyLocalDirAndBucket(ffm.Storage)
	if err != nil {
		return
	}
	backupDir, storageBucket, _, _, err = r.confObj.SpecifyTarget(ffm.Path)
	if err != nil {
		return
	}
	restorePath := strings.Replace(ffm.Path, backupDir, restoreDir, 1)
	restorePath = tools.CorrectPathWithPlatform(restorePath, meta.IsWin)
	parent := filepath.Dir(restorePath)
	if _, e := os.Stat(parent); e != nil {
		if err = os.MkdirAll(parent, meta.DefaultFileMode); err != nil {
			return err
		}
	}

	for i := 0; i < retry; i++ {
		err = nil
		if s3s != nil {
			downloader := s3manager.NewDownloader(
				s3s,
				func(downloader_ *s3manager.Downloader) {
					downloader_.Concurrency = s3manager.DefaultDownloadConcurrency
				},
			)
			err = r._downloadFromS3(ffm, restorePath, downloader, storageBucket)
		} else if url != meta.UnsetStr {
			err = r._downloadFromHost(ffm, restorePath, url)
		}
		if err == nil {
			_ = os.Chtimes(restorePath, time.Now(), time.Unix(ffm.Time, 0))
		}
	}
	return err
}

func (r *RestoreTask) _downloadFromS3(
	ffm models.EventFileModel, local string, downloader *s3manager.Downloader, bucket string) (err error) {
	target, err := os.OpenFile(local, os.O_CREATE|os.O_WRONLY, meta.DefaultFileMode)
	if err != nil {
		logger.Fmt.Errorf("RestoreTask DownloadOneFile. OpenFile-Error=%v", err)
		return
	}
	defer target.Close()

	_, err = downloader.Download(target,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(ffm.Storage),
		})
	return err
}

func (r *RestoreTask) _downloadFromHost(
	ffm models.EventFileModel, local string, url string) (err error) {
	target, err := os.OpenFile(local, os.O_CREATE|os.O_WRONLY, meta.DefaultFileMode)
	if err != nil {
		logger.Fmt.Errorf("RestoreTask DownloadOneFile. OpenFile-Error=%v", err)
		return
	}
	defer target.Close()

	var form http.Request
	if err = form.ParseForm(); err != nil {
		return
	}
	form.Form.Add("file", ffm.Storage)
	// TODO 后续添加“卷”的支持
	reqBody := strings.TrimSpace(form.Form.Encode())

	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(reqBody))
	if err != nil {
		logger.Fmt.Warnf("%v NewRequest err=%v", r.Str(), err)
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Connection", "Keep-Alive")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Fmt.Warnf("%v Do err=%v", r.Str(), err)
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(target, resp.Body)
	if err != nil {
		logger.Fmt.Warnf("%v io.Copy err=%v", r.Str(), err)
		return
	}
	return nil
}

func (r *RestoreTask) exitWhenErr(code ErrorCode, err error) {
	if err == nil {
		return
	}

	r.exitNotifyOnce.Do(func() {
		logger.Fmt.Errorf("%v.exitWhenErr catch%v, waiting to stop...", r.Str(), err)
		for {
			if err = models.UpdateRestoreTask(r.DBDriver.DB, r.taskObj.ID, meta.RESTORESERROR); err != nil {
				logger.Fmt.Warnf("%v.UpdateRestoreTask Err=%v, wait 10s...", r.Str(), err)
				time.Sleep(meta.RestoreErrFixStatusRetrySecs)
			}
			break
		}
		r.stop()
		r.exitNotify <- code
	})
}
