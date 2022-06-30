package v1

import (
	"github.com/gin-gonic/gin"
	"github.com/unknwon/com"
	"io/ioutil"
	"jingrongshuan/rongan-fnotify/api/app"
	"jingrongshuan/rongan-fnotify/api/statuscode"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/models"
	"net/http"
	"strings"
)

func Init(c *gin.Context) {
	ip := com.StrTo(c.Param("ip")).String()
	logger.Fmt.Infof("[GET] Init | get ip `%v`", ip)

	_, err := models.NewDBProxyWithInit(ip)
	if err != nil {
		logger.Fmt.Errorf("[GET] Init | NewDBProxyWithInit err=`%v`", err)
		app.Resp(c, http.StatusBadRequest, statuscode.INITDBPROXYFAILED, nil)
		return
	}

	logger.Fmt.Infof("[GET] Init | init db from `%v` is ok", ip)
	app.Resp(c, http.StatusOK, statuscode.SUCCESS, nil)
}

func ModifyServerIP(c *gin.Context) {
	old := com.StrTo(c.Param("old")).String()
	now := com.StrTo(c.Param("new")).String()

	input, err := ioutil.ReadFile(meta.ServerIPs)
	if err != nil {
		app.Resp(c, http.StatusOK, statuscode.READIPSCONFERROR, nil)
		return
	}
	ips := strings.ReplaceAll(string(input), old, now)
	err = ioutil.WriteFile(meta.ServerIPs, []byte(ips), 0644)
	if err != nil {
		app.Resp(c, http.StatusOK, statuscode.ModifyIPSCONFERROR, nil)
		return
	}
	app.Resp(c, http.StatusOK, statuscode.SUCCESS, nil)
}
