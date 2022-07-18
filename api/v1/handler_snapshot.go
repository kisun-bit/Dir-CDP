package v1

import (
	"github.com/gin-gonic/gin"
	"github.com/thoas/go-funk"
	"jingrongshuan/rongan-fnotify/api/app"
	"jingrongshuan/rongan-fnotify/api/statuscode"
	"jingrongshuan/rongan-fnotify/logic"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/models"
	"net/http"
	"strconv"
)

func CreateSnapshot(c *gin.Context) {
	appG := app.Gin{C: c}
	vol := c.PostForm("volume")
	if vol == meta.UnsetStr {
		appG.Response(http.StatusBadRequest, statuscode.LackVolume, nil)
		return
	}
	if !meta.IsWin {
		appG.Response(http.StatusBadRequest, statuscode.SysUnsupportedSnap, nil)
		return
	}
	sci, err := logic.CreateVSS(vol)
	if err != nil {
		logger.Fmt.Errorf("CreateSnapshot CreateVSS ERR=%v", err)
		appG.Response(http.StatusBadRequest, statuscode.CreateVssSnapFailed, nil)
		return
	}
	//sb, err := json.Marshal(sci)
	//if err != nil {
	//	logger.Fmt.Errorf("CreateSnapshot marshal ERR=%v", err)
	//	appG.Response(http.StatusBadRequest, statuscode.CreateVssSnapFailed, nil)
	//	return
	//}
	appG.Response(http.StatusOK, statuscode.SUCCESS, sci)
	return
}

func DetailSnapshot(c *gin.Context) {
	appG := app.Gin{C: c}
	_ = c.PostForm("type")     // vss/lvm
	snap := c.PostForm("snap") // vss: snap_id/lvm: snap_path
	if !meta.IsWin {
		appG.Response(http.StatusBadRequest, statuscode.SysUnsupportedSnap, nil)
		return
	}
	sci, err := logic.DetailVSS(snap)
	if err != nil {
		logger.Fmt.Errorf("DetailSnapshot DetailVSS ERR=%v", err)
		appG.Response(http.StatusBadRequest, statuscode.DetailVssSnapFailed, nil)
		return
	}
	appG.Response(http.StatusOK, statuscode.SUCCESS, sci)
	return
}

func DeleteSnapshot(c *gin.Context) {
	appG := app.Gin{C: c}
	_ = c.PostForm("type")     // vss/lvm
	snap := c.PostForm("snap") // vss: snap_id/lvm: snap_path
	if !meta.IsWin {
		appG.Response(http.StatusBadRequest, statuscode.SysUnsupportedSnap, nil)
		return
	}
	err := logic.DeleteVSS(snap)
	if err != nil {
		logger.Fmt.Warnf("DeleteSnapshot DeleteVSS ERR=%v", err)
		appG.Response(http.StatusBadRequest, statuscode.DeleteVssSnapFailed, nil)
		return
	}
	appG.Response(http.StatusOK, statuscode.SUCCESS, nil)
	return
}

// 回滚快照，由studio调用
func RevertSnapshot(c *gin.Context) {
	appG := app.Gin{C: c}
	vtype := c.PostForm("type") // vss/lvm
	version := c.PostForm("version")
	server := c.PostForm("server")
	task, _ := strconv.ParseInt(c.PostForm("restore_id"), 10, 64)
	node, _ := strconv.ParseInt(c.PostForm("restore_node"), 10, 64)

	restoreMethod := c.PostForm("restore_method")
	logger.Fmt.Infof("RevertSnapshot type(%v) version(%v) server(%v) restore_id(%v) type(%v)",
		vtype, version, server, task, restoreMethod)

	if !funk.InStrings(
		[]string{
			meta.RevertSnap2TargetNewDrive,
			meta.RevertSnap2TargetOldDrive,
			meta.RevertSnap2OriginOldDrive,
			meta.RevertSnap2OriginNewDrive,
		}, restoreMethod) {
		appG.Response(http.StatusBadRequest, statuscode.InvalidRevertType, nil)
		return
	}
	db, err := models.NewDBInstanceByIP(server)
	if err != nil {
		logger.Fmt.Warnf("RevertSnapshot NewDBInstanceByIP ERR=%v", err)
		appG.Response(http.StatusBadRequest, statuscode.InitDBDriverFailed, nil)
		return
	}
	s, err := models.QuerySnapshotByVersion(db, version)
	if err != nil {
		logger.Fmt.Warnf("RevertSnapshot QuerySnapshotByVersion ERR=%v", err)
		appG.Response(http.StatusBadRequest, statuscode.DetailVssSnapFailed, nil)
		return
	}
	config, err := models.QueryConfigByID(db, s.Config)
	if err != nil {
		logger.Fmt.Warnf("RevertSnapshot QueryConfigByID ERR=%v", err)
		appG.Response(http.StatusBadRequest, statuscode.QueryConfigFailed, nil)
		return
	}
	revert, err := models.QueryRestoreTaskByID(db, task)
	if err != nil {
		logger.Fmt.Warnf("RevertSnapshot QuerySnapshotByVersion ERR=%v", err)
		appG.Response(http.StatusBadRequest, statuscode.QUERYRESTOREFAILED, nil)
		return
	}
	t := logic.NewTargetMachineRevert(&models.DBProxy{DB: db}, &config, &revert, &s, restoreMethod, node)
	t.Start()
	appG.Response(http.StatusOK, statuscode.SUCCESS, nil)
	return
}

func AddShadowStorage(c *gin.Context) {
	// TODO
	_ = c
	return
}
