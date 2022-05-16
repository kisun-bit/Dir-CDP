package v1

import (
	"encoding/base64"
	"github.com/gin-gonic/gin"
	"github.com/unknwon/com"
	"io"
	"jingrongshuan/rongan-fnotify/api/app"
	"jingrongshuan/rongan-fnotify/api/statuscode"
	"net/http"
	"os"
	"path"
)

// @Summary 上传文件
// @Produce json
// @Success 200 {object} app.Response
// @Failure 500 {object} app.Response
// @Router /api/v1/file/upload [post]
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
		if filename == "" {
			appG.Response(http.StatusBadRequest, statuscode.LACKFILENAME, nil)
			return
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

// @Summary 下载文件
// @Produce json
// @Success 200 {object} app.Response
// @Failure 500 {object} app.Response
// @Router /api/v1/download/:b64 [get]
func Download(c *gin.Context) {
	appG := app.Gin{C: c}
	b64 := com.StrTo(c.Param("b64")).String()
	pathb, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		appG.Response(http.StatusBadRequest, statuscode.WRITEIOFAILED, nil)
		return
	}
	path_ := string(pathb)

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
