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
)

func Upload(c *gin.Context) {
	appG := app.Gin{C: c}

	reader, err := c.Request.MultipartReader()
	if err != nil {
		appG.Response(http.StatusBadRequest, statuscode.SYNCFILEFAILED, nil)
		return
	}

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		} else if err != nil {
			appG.Response(http.StatusBadRequest, statuscode.SYNCFILEFAILED, nil)
			return
		}

		filename := part.FileName()
		// 权限
		if filename == "" {
			appG.Response(http.StatusBadRequest, statuscode.LACKFILENAME, nil)
			return
		}
		if _, err_ := os.Stat(filepath.Dir(filename)); err_ != nil {
			_ = os.MkdirAll(filepath.Dir(filename), 0666)
		}
		dst, err := os.Create(filename)
		if err != nil {
			appG.Response(http.StatusBadRequest, statuscode.EXISTSSAMENAMEFILE, nil)
			return
		}
		_, err = io.Copy(dst, part)
		dst.Close()
		if err != nil {
			appG.Response(http.StatusBadRequest, statuscode.WRITEIOFAILED, nil)
			return
		}
	}

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
	file := c.PostForm("b64")
	if file == meta.UnsetStr {
		appG.Response(http.StatusBadRequest, statuscode.LACKFILEPATH, nil)
		return
	}

	if _, err := os.Stat(file); err != nil {
		// do nothing
	} else {
		if err = os.Remove(file); err != nil {
			logger.Fmt.Warnf("remove `%v` failed ERR=%v", file, err)
			appG.Response(http.StatusBadRequest, statuscode.DELFILEERROR, nil)
			return
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
