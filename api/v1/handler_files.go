package v1

import (
	"encoding/base64"
	"github.com/gin-gonic/gin"
	"github.com/unknwon/com"
	"io"
	"jingrongshuan/rongan-fnotify/api/app"
	"jingrongshuan/rongan-fnotify/api/statuscode"
	"jingrongshuan/rongan-fnotify/meta"
	"net/http"
	"os"
	"path"
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
		if err != nil {
			dst.Close()
			appG.Response(http.StatusBadRequest, statuscode.WRITEIOFAILED, nil)
			return
		}
	}

	appG.Response(http.StatusOK, statuscode.SUCCESS, nil)
	return
}

func Download(c *gin.Context) {
	appG := app.Gin{C: c}
	b64 := com.StrTo(c.Param("b64")).String()
	pb, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		appG.Response(http.StatusBadRequest, statuscode.WRITEIOFAILED, nil)
		return
	}
	path_ := string(pb)

	fileTmp, errByOpenFile := os.Open(path_)
	if errByOpenFile != nil {
		appG.Response(http.StatusBadRequest, statuscode.WRITEIOFAILED, nil)
		return
	}
	defer fileTmp.Close()

	fileName := path.Base(path_)
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Disposition", "attachment; filename="+fileName)
	c.Header("Content-Disposition", "inline;filename="+fileName)
	c.Header("Content-Transfer-Encoding", "binary")
	c.Header("Cache-Control", "no-cache")

	c.File(path_)
	appG.Response(http.StatusOK, statuscode.SUCCESS, nil)
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
			appG.Response(http.StatusOK, statuscode.DELFILEERROR, nil)
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
