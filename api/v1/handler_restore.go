package v1

import (
	"github.com/gin-gonic/gin"
	"github.com/unknwon/com"
	"jingrongshuan/rongan-fnotify/api/app"
	"jingrongshuan/rongan-fnotify/api/statuscode"
	"jingrongshuan/rongan-fnotify/logic"
	"jingrongshuan/rongan-fnotify/models"
	"net/http"
)

func StartRestore(c *gin.Context) {
	appG := app.Gin{C: c}
	id := com.StrTo(c.Param("id")).MustInt64()
	ip := com.StrTo(c.Param("ip")).String()

	dp, err := models.NewDBProxyWithInit(ip)
	if err != nil {
		appG.Response(http.StatusBadRequest, statuscode.INITDBPROXYFAILED, nil)
		return
	}
	t, err := models.QueryRestoreTaskByID(dp.DB, id)
	if err != nil {
		appG.Response(http.StatusBadRequest, statuscode.QUERYRESTOREFAILED, nil)
		return
	}
	r, err := logic.NewRestoreTask(&t, dp)
	if err != nil {
		appG.Response(http.StatusBadRequest, statuscode.INITRESTOREFAILED, nil)
		return
	}
	if err = r.Start(); err != nil {
		appG.Response(http.StatusBadRequest, statuscode.STARTESTOREFAILED, nil)
		return
	}
	appG.Response(http.StatusOK, statuscode.SUCCESS, nil)
	return
}
