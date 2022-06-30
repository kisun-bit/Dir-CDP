package v1

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/unknwon/com"
	"jingrongshuan/rongan-fnotify/api/app"
	"jingrongshuan/rongan-fnotify/api/statuscode"
	"jingrongshuan/rongan-fnotify/logic"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/models"
	"net/http"
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
		logger.Fmt.Errorf("DeleteSnapshot DeleteVSS ERR=%v", err)
		appG.Response(http.StatusBadRequest, statuscode.DeleteVssSnapFailed, nil)
		return
	}
	appG.Response(http.StatusOK, statuscode.SUCCESS, nil)
	return
}

// 回滚快照，由studio调用
func RevertSnapshot(c *gin.Context) {
	appG := app.Gin{C: c}
	_ = c.PostForm("type") // vss/lvm
	snapshotSet := com.StrTo(c.PostForm("version")).MustInt64()
	Server := c.PostForm("server")

	db, err := models.NewDBInstanceByIP(Server)
	if err != nil {
		logger.Fmt.Warnf("RevertSnapshot NewDBInstanceByIP ERR=%v", err)
		appG.Response(http.StatusBadRequest, statuscode.InitDBDriverFailed, nil)
		return
	}
	s, err := models.QuerySnapshotByID(db, snapshotSet)
	if err != nil {
		logger.Fmt.Warnf("RevertSnapshot QuerySnapshotByID ERR=%v", err)
		appG.Response(http.StatusBadRequest, statuscode.DetailVssSnapFailed, nil)
		return
	}

	var ss []logic.ShadowCopyIns
	if err := json.Unmarshal([]byte(s.Snap), &ss); err != nil {
		appG.Response(http.StatusBadRequest, statuscode.FormatResponseErr, nil)
		return
	}
	if !meta.IsWin {
		appG.Response(http.StatusBadRequest, statuscode.SysUnsupportedSnap, nil)
		return
	}
	for _, snap := range ss {
		err := logic.RevertVSS(snap.SnapID)
		if err != nil {
			logger.Fmt.Errorf("RevertSnapshot RevertVSS ERR=%v", err)
			appG.Response(http.StatusBadRequest, statuscode.RevertVssSnapFailed, nil)
			return
		}
	}
	if err = models.DeleteSnapshotsAfterID(db, snapshotSet); err != nil {
		logger.Fmt.Warnf("RevertSnapshot DeleteSnapshotsAfterID ERR=%v", err)
	}
	appG.Response(http.StatusOK, statuscode.SUCCESS, nil)
	return
}

func AddShadowStorage(c *gin.Context) {
	// TODO
	_ = c
	return
}
