package v1

import (
	"github.com/gin-gonic/gin"
	"github.com/unknwon/com"
	"jingrongshuan/rongan-fnotify/api/app"
	"jingrongshuan/rongan-fnotify/api/statuscode"
	"jingrongshuan/rongan-fnotify/models"
	"net/http"
)

func Init(c *gin.Context) {
	ip := com.StrTo(c.Param("ip")).String()
	logger.Fmt.Infof("[GET] Init | get ip `%v`", ip)

	_, err := models.NewDBProxy(ip)
	if err != nil {
		logger.Fmt.Errorf("[GET] Init | NewDBProxy err=`%v`", err)
		app.Resp(c, http.StatusBadRequest, statuscode.INITDBPROXYFAILED, nil)
		return
	}

	logger.Fmt.Infof("[GET] Init | init db from `%v` is ok", ip)
	app.Resp(c, http.StatusOK, statuscode.SUCCESS, nil)
}
