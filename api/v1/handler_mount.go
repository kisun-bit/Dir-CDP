package v1

import (
	"github.com/gin-gonic/gin"
	"jingrongshuan/rongan-fnotify/api/app"
	"jingrongshuan/rongan-fnotify/api/statuscode"
	"jingrongshuan/rongan-fnotify/logic"
	"net/http"
)

func SMBConnect(c *gin.Context) {
	appG := app.Gin{C: c}

	ip := c.PostForm("share_ip")
	name := c.PostForm("share_name")

	//letters, err := tools.FreeDriveLetters()
	//if err != nil {
	//	logger.Fmt.Warnf("SMBConnect FreeDriveLetters ERR=%v", err)
	//	appG.Response(http.StatusBadRequest, statuscode.MallocDriveERR, nil)
	//	return
	//}
	//
	//for _, letter := range letters {
	//	if err = logic.ConnectShareFolderAsDrive(ip, name, letter); err != nil {
	//		logger.Fmt.Warnf("SMBConnect ConnectShareFolderAsDrive Letter(%v) ERR=%v", letter, err)
	//		continue
	//	}
	//	appG.Response(http.StatusOK, statuscode.SUCCESS, letter)
	//	return
	//}

	if err := logic.ConnectShareFolder(ip, name); err != nil {
		logger.Fmt.Warnf("SMBConnect ConnectShareFolder ERR=%v", err)
		appG.Response(http.StatusBadRequest, statuscode.SMBConnectERR, nil)
		return
	}
	appG.Response(http.StatusOK, statuscode.SUCCESS, "")
	return
}

func SMBDisconnect(c *gin.Context) {
	appG := app.Gin{C: c}

	ip := c.PostForm("share_ip")
	name := c.PostForm("share_name")
	drive := c.PostForm("drive")

	if err := logic.DisconnectShareDrive(ip, name, drive); err != nil {
		logger.Fmt.Warnf("SMBConnect DisconnectShareDrive ERR=%v", err)
	}
	if err := logic.DisconnectShareFolder(ip, name); err != nil {
		logger.Fmt.Warnf("SMBConnect DisconnectShareFolder ERR=%v", err)
		appG.Response(http.StatusBadRequest, statuscode.SMBDisConnectERR, nil)
		return
	}
	appG.Response(http.StatusOK, statuscode.SUCCESS, drive)
	return
}

func SMBDelete(c *gin.Context) {
	appG := app.Gin{C: c}

	name := c.PostForm("share_name")
	if err := logic.DeleteShare(name); err != nil {
		logger.Fmt.Warnf("SMBDelete DeleteShare ERR=%v", err)
		appG.Response(http.StatusBadRequest, statuscode.SMBDelShareERR, nil)
		return
	}
	appG.Response(http.StatusOK, statuscode.SUCCESS, nil)
	return
}
