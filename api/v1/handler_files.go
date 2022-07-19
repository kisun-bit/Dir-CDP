package v1

import (
	"github.com/gin-gonic/gin"
	"io"
	"jingrongshuan/rongan-fnotify/api/app"
	"jingrongshuan/rongan-fnotify/api/statuscode"
	"jingrongshuan/rongan-fnotify/meta"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
)

func Upload(c *gin.Context) {
	appG := app.Gin{C: c}

	file, err := c.FormFile("filename")
	if err != nil {
		logger.Fmt.Warnf("Upload file=%v ERR=%v", file, err)
		appG.Response(http.StatusBadRequest, statuscode.LACKFILENAME, nil)
		return
	}
	mode, err := strconv.ParseInt(c.PostForm("mode"), 10, 64)
	if err != nil {
		logger.Fmt.Warnf("Upload parse mode err=%v", err)
		appG.Response(http.StatusBadRequest, statuscode.ERROR, nil)
		return
	}
	dir := filepath.Dir(file.Filename)
	if _, notExisted := os.Stat(dir); notExisted != nil {
		_ = os.MkdirAll(dir, meta.DefaultFileMode)
	}

	err = c.SaveUploadedFile(file, file.Filename)
	if err != nil {
		logger.Fmt.Warnf("Upload `%v` ERR=%v", file.Filename, err)
		appG.Response(http.StatusBadRequest, statuscode.SYNCFILEFAILED, nil)
		return
	}
	_ = os.Chmod(file.Filename, os.FileMode(mode))
	appG.Response(http.StatusOK, statuscode.SUCCESS, nil)
	return
}

func Download(c *gin.Context) {
	appG := app.Gin{C: c}
	file := c.PostForm("file")
	volume := c.PostForm("volume")

	_ = volume // TODO 支持从卷上恢复
	_, err := os.Stat(file)
	if err != nil {
		appG.Response(http.StatusBadRequest, statuscode.WRITEIOFAILED, nil)
		return
	}
	fileTmp, errByOpenFile := os.Open(file)
	if errByOpenFile != nil {
		appG.Response(http.StatusBadRequest, statuscode.WRITEIOFAILED, nil)
		return
	}
	defer fileTmp.Close()
	c.Header("Content-Type", "application/octet-stream")

	_, err = io.Copy(c.Writer, fileTmp)
	if err != nil {
		appG.Response(http.StatusBadRequest, statuscode.WRITEIOFAILED, nil)
		return
	}
	return
}

func Delete(c *gin.Context) {
	appG := app.Gin{C: c}
	file := c.PostForm("file")
	if file == meta.UnsetStr {
		appG.Response(http.StatusBadRequest, statuscode.LACKFILEPATH, nil)
		return
	}

	if _, err := os.Stat(file); err != nil {
		// do nothing
	} else {
		if err = os.Remove(file); err != nil {
			logger.Fmt.Warnf("remove `%v` failed ERR=%v", file, err)
			if err = os.RemoveAll(file); err != nil {
				logger.Fmt.Warnf("remove all `%v` failed ERR=%v", file, err)
			}
			if err != nil {
				appG.Response(http.StatusBadRequest, statuscode.DELFILEERROR, nil)
				return
			}
		}
	}

	appG.Response(http.StatusOK, statuscode.SUCCESS, nil)
	return
}

func Rename(c *gin.Context) {
	appG := app.Gin{C: c}

	old := c.PostForm("old")
	new_ := c.PostForm("new")

	if _, err := os.Stat(old); err != nil {
		appG.Response(http.StatusBadRequest, statuscode.NOTEXISTSFILE, nil)
		return
	}
	if err := os.Rename(old, new_); err != nil {
		appG.Response(http.StatusBadRequest, statuscode.RENAMEERROR, nil)
		return
	}
	appG.Response(http.StatusOK, statuscode.SUCCESS, nil)
	return
}

func ChangeAttrs(c *gin.Context) {
	appG := app.Gin{C: c}

	path := c.PostForm("path")
	attr, _ := strconv.ParseInt(c.PostForm("mode"), 10, 64)

	if err := os.Chmod(path, os.FileMode(attr)); err != nil {
		appG.Response(http.StatusBadRequest, statuscode.CHANGEMODEERR, nil)
		return
	}
	appG.Response(http.StatusOK, statuscode.SUCCESS, nil)
	return
}

func CreateOrUpdateDir(c *gin.Context) {
	appG := app.Gin{C: c}

	path := c.PostForm("path")
	mode, e := strconv.ParseInt(c.PostForm("mode"), 10, 64)

	if e != nil {
		logger.Fmt.Warnf("CreateOrUpdateDir ParseInt ERR=%v", e)
		appG.Response(http.StatusBadRequest, statuscode.CorrectDirFailed, nil)
		return
	}
	fi, e := os.Stat(path)
	if e != nil {
		if err := os.MkdirAll(path, os.FileMode(mode)); err != nil {
			logger.Fmt.Warnf("CreateOrUpdateDir MkdirAll ERR=%v", err)
			appG.Response(http.StatusBadRequest, statuscode.CorrectDirFailed, nil)
			return
		}
	} else {
		if int64(fi.Mode()) != mode {
			if err := os.Chmod(path, os.FileMode(mode)); err != nil {
				logger.Fmt.Warnf("CreateOrUpdateDir Chmod ERR=%v", err)
				appG.Response(http.StatusBadRequest, statuscode.CorrectDirFailed, nil)
				return
			}
		}
	}
	appG.Response(http.StatusOK, statuscode.SUCCESS, nil)
	return
}
