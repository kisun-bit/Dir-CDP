package v1

import (
	"github.com/gin-gonic/gin"
	"github.com/unknwon/com"
	"jingrongshuan/rongan-fnotify/api/app"
	"jingrongshuan/rongan-fnotify/api/statuscode"
	"jingrongshuan/rongan-fnotify/logic"
	"net/http"
)

func EnableCDPConfig(c *gin.Context) {
	appG := app.Gin{C: c}
	id := com.StrTo(c.Param("id")).MustInt64()
	ip := com.StrTo(c.Param("ip")).String()
	logger.Fmt.Infof("[GET] EnableCDPConfig | backup-server(%v) config-id(%v) called...", ip, id)

	if err := logic.RegisterServerInfo(ip); err != nil {
		appG.Response(http.StatusBadRequest, statuscode.RECORDSERVERFAILED, nil)
		return
	}

	// 初始化DB连接
	if err := logic.LoadCDP(ip, id, false); err != nil {
		logger.Fmt.Errorf("EnableCDPConfig LoadCDP. ERR=%v", err)
		appG.Response(http.StatusBadRequest, statuscode.INITCDPEXEFAILED, nil)
		return
	}

	appG.Response(http.StatusOK, statuscode.SUCCESS, nil)
	return
}
