package v1

import (
	"github.com/gin-gonic/gin"
	"github.com/unknwon/com"
	"jingrongshuan/rongan-fnotify/api/app"
	"jingrongshuan/rongan-fnotify/api/statuscode"
	"jingrongshuan/rongan-fnotify/logic"
	"jingrongshuan/rongan-fnotify/models"
	"net"
	"net/http"
)

// StartRestore
// @Summary xxx
// @Description 开始执行恢复操作
// @Tags RestoreTask相关接口
// @Accept application/json
// @Produce application/json
// @Param id path int true "恢复任务ID"
// @Success 200 {object} app.Response
// @Router /restore/:id/start [get]
func StartRestore(c *gin.Context) {
	appG := app.Gin{C: c}
	id := com.StrTo(c.Param("id")).MustInt64()

	ip, _, err := net.SplitHostPort(c.Request.RemoteAddr)
	if err != nil {
		appG.Response(http.StatusBadRequest, statuscode.BACKUPSERVERIPFAILED, nil)
		return
	}
	dp, err := models.NewDBProxy(ip)
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
