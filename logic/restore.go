package logic

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/aws/session"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"github.com/journeymidnight/aws-sdk-go/service/s3/s3manager"
	"io"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/models"
	"jingrongshuan/rongan-fnotify/tools"
	"net/http"
	"os"
	"strings"
	"time"
)

// 恢复任务
type RestoreTask struct {
	task       *models.RestoreTaskModel
	dp         *models.DBProxy
	filterArgs models.RestoreFilter
	queue      chan models.EventFileModel
}

func NewRestoreTask(task *models.RestoreTaskModel, dp *models.DBProxy) (r *RestoreTask, err error) {
	r = new(RestoreTask)
	r.dp = dp
	r.task = task
	r.queue = make(chan models.EventFileModel, 100)
	return r, nil
}

func (r *RestoreTask) paramFileset() (fs []string) {
	if r.filterArgs.Fileset == "" {
		return
	}
	return strings.Split(r.filterArgs.Fileset, meta.SplitFlag)
}

func (r *RestoreTask) paramStartTime() *time.Time {
	if r.filterArgs.Starttime == "" {
		return nil
	}
	t_ := tools.String2Time(r.filterArgs.Starttime)
	return &t_
}

func (r *RestoreTask) paramEndTime() *time.Time {
	if r.filterArgs.Endtime == "" {
		return nil
	}
	t_ := tools.String2Time(r.filterArgs.Endtime)
	return &t_
}

func (r *RestoreTask) Start() (err error) {
	if r.filterArgs, err = models.QueryRestoreFilterArgs(r.dp.DB, r.task.ID); err != nil {
		return err
	}
	go func() {
		r.logic()
	}()
	return nil
}

func (r *RestoreTask) logic() {
	var err error
	defer func() {
		if err != nil {
			close(r.queue)
			// TODO 修正任务状态
		} else {
			// TODO 修正任务状态
		}
	}()
	if len(r.paramFileset()) != 0 {
		err = r.restoreByFileset()
	} else {
		err = r.restoreByTime()
	}
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
		if row, err = models.QueryRecursiveFilesIteratorInDir(r.dp.DB, r.task.ConfID, path_); err != nil {
			logger.Fmt.Errorf("RestoreTask.restoreOneSet invalid dir %s", path_)
			return err
		}
		defer row.Close()
		for row.Next() {
			if err = r.dp.DB.ScanRows(row, &ffm); err != nil {
				logger.Fmt.Errorf("RestoreTask.restoreOneSet File=%v ScanRows-Err=%v", ffm.Path, err)
				return err
			}
			r.queue <- ffm
		}
		return
	}

	if ffm, err = models.QueryFileByName(r.dp.DB, r.task.ConfID, path_); err != nil {
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
		r.dp.DB, r.task.ConfID, r.paramStartTime(), r.paramEndTime()); err != nil {
		logger.Fmt.Errorf("RestoreTask.restoreByTime QueryFileIteratorByTime-Error=%v", err)
		return err
	}
	defer row.Close()

	for row.Next() {
		if err = r.dp.DB.ScanRows(row, &ffm); err != nil {
			logger.Fmt.Errorf("RestoreTask RestoreByFilterArgs. ScanRows-Err=%v", ffm)
			return err
		}
		r.queue <- ffm
	}
	return nil
}

func (r *RestoreTask) consume() {
	go func() {
		r.download()
	}()
}

func (r *RestoreTask) download() {
	var (
		err    error
		grep   *Grep
		tcs    models.TargetS3
		config models.ConfigModel
		s3s    *session.Session
		thi    models.ClientInfo
		url    string
	)

	defer func() {
		if err != nil {
			// TODO 任务状态
		} else {
			// TODO 任务状态
		}
	}()

	if config, err = models.QueryConfig(r.dp.DB, r.task.ConfID); err != nil {
		return
	}
	grep = NewGrep(r.filterArgs.Include, r.filterArgs.Exclude)

	if models.Is2S3(config) {
		if tcs, err = models.QueryTargetConfS3ByConf(r.dp.DB, r.task.ConfID); err != nil {
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
		if thi, err = models.QueryTargetHostInfoByConf(r.dp.DB, r.task.ConfID); err != nil {
			return
		}
		url = fmt.Sprintf("http://%v:%v/api/v1/download/", thi.Address, meta.AppPort)
	}

	for ffm := range r.queue {
		if !grep.IsValidByGrep(ffm.Path) {
			continue
		}

		local := strings.Replace(ffm.Path, config.Dir, r.filterArgs.RestoreDir, 1) + meta.RestoreSuffix
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

		if err = os.Rename(local, strings.TrimSuffix(local, meta.RestoreSuffix)); err != nil {
			return
		}
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
