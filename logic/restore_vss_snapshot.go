package logic

import (
	"encoding/json"
	"fmt"
	"github.com/kr/pretty"
	"golang.org/x/sync/semaphore"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/models"
	"jingrongshuan/rongan-fnotify/tools"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// SnapshotRestore 目标机回滚卷影副本或挂载卷影副本到异机
type SnapshotRestore struct {
	DBDriver        *models.DBProxy // 数据库驱动器
	configObj       *models.ConfigModel
	restoreObj      *models.RestoreTaskModel
	versionObj      *models.Snapshot
	restoreClientID int64
	restoreClient   *models.ClientNode
	originClient    *models.ClientNode
	reporter        *Reporter
	pool            *semaphore.Weighted
	restoreType     string
	uploadSession   string
	dirSession      string
	curSnaps        []ShadowCopyIns
	fixedSnaps      []*ShadowCopyIns
}

func NewTargetMachineRevert(db *models.DBProxy,
	config *models.ConfigModel, revert *models.RestoreTaskModel, version *models.Snapshot, type_ string,
	restoreClient int64) *SnapshotRestore {
	return &SnapshotRestore{
		DBDriver:        db,
		configObj:       config,
		restoreObj:      revert,
		versionObj:      version,
		restoreType:     type_,
		restoreClientID: restoreClient,
		reporter:        NewReporter(db.DB, config.ID, revert.ID, meta.TaskTypeRestore, AllLevels),
		pool:            semaphore.NewWeighted(int64(runtime.GOMAXPROCS(0))),
		fixedSnaps:      make([]*ShadowCopyIns, 0),
	}
}

func (t *SnapshotRestore) Str() string {
	return fmt.Sprintf("<SnapshotRestore(config=%v, revert=%v, version=%v)>",
		t.configObj.ID, t.restoreObj.ID, t.versionObj.Ver)
}

func (t *SnapshotRestore) Start() {
	_ = t.reporter.ReportInfo(StepReceiveRestoreInstruct, t.versionObj.Ver)
	go t.logic()
}

func (t *SnapshotRestore) logic() {
	var err error
	defer func() {
		if err != nil {
			if e := models.UpdateRestoreTask(t.DBDriver.DB, t.restoreObj.ID, meta.RESTORESERROR); e != nil {
				logger.Fmt.Warnf("%v.logic defer UpdateRestoreTask ERR=%v", t.Str(), err)
			}
			if e := models.EndRestoreTask(t.DBDriver.DB, t.restoreObj.ID, false); e != nil {
				logger.Fmt.Warnf("%v.logic defer EndRestoreTask ERR=%v", t.Str(), err)
			}
			_ = t.reporter.ReportError(StepRevertErr, t.versionObj.Ver, err.Error())
		} else {
			if e := models.UpdateRestoreTask(t.DBDriver.DB, t.restoreObj.ID, meta.RESTOREFINISH); e != nil {
				logger.Fmt.Warnf("%v.logic defer UpdateRestoreTask ERR=%v", t.Str(), err)
			}
			if e := models.EndRestoreTask(t.DBDriver.DB, t.restoreObj.ID, true); e != nil {
				logger.Fmt.Warnf("%v.logic defer EndRestoreTask ERR=%v", t.Str(), err)
			}
			_ = t.reporter.ReportInfo(StepFinishRestore, t.versionObj.Ver)
		}
	}()
	if err = models.UpdateRestoreTask(t.DBDriver.DB, t.restoreObj.ID, meta.RESTORESTART); err != nil {
		logger.Fmt.Warnf("%v.logic UpdateRestoreTask ERR=%v", t.Str(), err)
		return
	}
	if err = t.configObj.LoadsJsonFields(t.DBDriver.DB); err != nil {
		logger.Fmt.Warnf("%v.logic LoadsJsonFields ERR=%v", t.Str(), err)
		return
	}
	if err = t.loadNodes(); err != nil {
		logger.Fmt.Warnf("%v.logic loadNodes ERR=%v", t.Str(), err)
		return
	}
	if err = t.loadCurSnaps(); err != nil {
		logger.Fmt.Warnf("%v.logic loadCurSnaps ERR=%v", t.Str(), err)
		return
	}
	_ = t.reporter.ReportInfo(StepLoadAllSnapshotsConfig, t.versionObj.Ver)
	if err = t.mapOriginDir2Snapshot(); err != nil {
		logger.Fmt.Warnf("%v.logic mapOriginDir2Snapshot ERR=%v", t.Str(), err)
		return
	}
	_ = t.reporter.ReportInfo(StepMatchDirFromSnapshot)
	//if t.restoreType == meta.RevertSnap2TargetOldDrive {
	//	defer t.recycleSnapshotAfterVersion()
	//}
	if err = models.UpdateRestoreTask(t.DBDriver.DB, t.restoreObj.ID, meta.RESTOREING); err != nil {
		logger.Fmt.Warnf("%v.logic UpdateRestoreTask ERR=%v", t.Str(), err)
		return
	}
	if err = t.revertAllOriginDirs(); err != nil {
		logger.Fmt.Warnf("%v.logic revertAllOriginDirs ERR=%v", t.Str(), err)
		return
	}
}

func (t *SnapshotRestore) loadNodes() (err error) {
	t.restoreClient = new(models.ClientNode)
	*t.restoreClient, err = models.QueryClientNodeByID(t.DBDriver.DB, t.restoreClientID)
	if err != nil {
		return
	}
	logger.Fmt.Infof("%v.loadNodes restore client is `%v`", t.Str(), t.restoreClient.String())
	t.originClient = new(models.ClientNode)
	*t.originClient, err = models.QueryClientNodeByID(t.DBDriver.DB, t.versionObj.Target)
	if err != nil {
		return
	}
	logger.Fmt.Infof("%v.loadNodes origin client is `%v`", t.Str(), t.restoreClient.String())
	return
}

func (t *SnapshotRestore) loadCurSnaps() (err error) {
	if err := json.Unmarshal([]byte(t.versionObj.Snap), &t.curSnaps); err != nil {
		return err
	}
	logger.Fmt.Infof("%v.loadCurSnaps snapshot => \n%v", t.Str(), pretty.Sprint(t.curSnaps))
	return nil
}

func (t *SnapshotRestore) mapOriginDir2Snapshot() (err error) {
	for _, sci := range t.curSnaps {
		sci.Dirs = make([]string, 0)
		for _, d := range t.configObj.DirsMappingJson {
			if strings.HasPrefix(d.StoragePrefixForBucketOrRemote, sci.VolLetter+":") {
				sci.Dirs = append(sci.Dirs, d.StoragePrefixForBucketOrRemote)
			}
		}
		sci.IsSysVol = sci.VolLetter == "C"
		sci.ClientAccess = strings.Contains(sci.Attribute, meta.ClientAccessible)
		if sci.ShareRemote == nil {
			sci.ShareRemote = make([]ShareRemoteInfo, 0)
		}
		t.fixedSnaps = append(t.fixedSnaps, &sci)
	}
	logger.Fmt.Infof("%v.mapOriginDir2Snapshot map => \n%v", t.Str(), pretty.Sprint(t.fixedSnaps))
	return nil
}

func (t *SnapshotRestore) revertAllOriginDirs() (err error) {
	for _, s := range t.fixedSnaps {
		_ = t.reporter.ReportInfo(StepHandleSnapshot, s.SCopyPath, s.SnapID)
		if err = t.revertOneSnapshot(s); err != nil {
			return err
		}
	}
	return nil
}

func (t *SnapshotRestore) revertOneSnapshot(s *ShadowCopyIns) (err error) {
	logger.Fmt.Infof("%v.revertOneSnapshot snapshot_map_dir => \n%v", t.Str(), s)
	if s.ClientAccess {
		// VSSAdmin强制回滚
		_ = t.reporter.ReportInfo(StepRevertShadowCopy, s.SCopyPath, s.VolLetter)
		return RevertSnapshotByVssAdmin(s.SnapID)
	} else {
		if s.IsShare && t.restoreType != meta.RevertSnap2TargetOldDrive {
			logger.Fmt.Infof("%v.revertOneSnapshot snap(%v) already shared", t.Str(), s.SnapID)
			if t.restoreClientID == t.originClient.ID {
				_ = t.reporter.ReportInfo(StepEndExposeLocal, s.SnapID, s.Mountpoint)
				return nil
			}
			_ = t.reporter.ReportInfo(StepLackLettersNetConn, t.originClient.IP, s.ShareName,
				meta.WinShareUser, meta.WinSharePwd)
			return nil
		}
		if t.restoreType == meta.RevertSnap2OriginNewDrive && t.restoreClientID == t.originClient.ID {
			_ = t.reporter.ReportInfo(StepStartExposeLocal, s.SnapID)
			logger.Fmt.Infof("%v.revertOneSnapshot share to local. expose directory", t.Str())
			if s.Mountpoint != meta.UnsetStr {
				// do nothing
			} else {
				letters, err := tools.FreeDriveLetters()
				if err != nil {
					return err
				}
				for _, letter := range letters {
					logger.Fmt.Infof("%v.revertOneSnapshot will try %v", t.Str(), s.Mountpoint)
					if err = MountSnapshotOnTmpPath(s.SnapID, letter); err != nil {
						logger.Fmt.Warnf("%v.revertOneSnapshot MountSnapshotOnTmpPath ERR=%v", t.Str(), err)
						continue
					}
					s.Mountpoint = letter
					_ = SetVolumeLabel(s.Mountpoint, t.generateShareName(s.VolLetter))
					if err = t.updateSnapshotStatus(); err != nil {
						return err
					}
				}
			}
			_ = t.reporter.ReportInfo(StepEndExposeLocal, s.SnapID, s.Mountpoint)
			return nil
		}
		// VShadow先挂载到临时目录、在使用robocopy镜像拷贝
		if s.Mountpoint == meta.UnsetStr {
			// 未挂载且未共享
			_ = t.reporter.ReportInfo(StepStartExposeLocal, s.SnapID)
			s.Mountpoint = filepath.Join(meta.MountPoints, s.SnapID)
			if err = os.Mkdir(s.Mountpoint, meta.DefaultFileMode); err != nil {
				logger.Fmt.Warnf("%v.revertOneSnapshot failed to create `%v`", t.Str(), s.Mountpoint)
				return err
			}
			_ = t.reporter.ReportInfo(StepEndExposeLocal, s.SnapID)
			if err = t.updateSnapshotStatus(); err != nil {
				return err
			}
			if err = MountSnapshotOnTmpPath(s.SnapID, s.Mountpoint); err != nil {
				logger.Fmt.Warnf("%v.revertOneSnapshot MountSnapshotOnTmpPath ERR=%v", t.Str(), err)
				return err
			}
			_ = t.reporter.ReportInfo(StepLoadSnapshot, s.Mountpoint)
		}
		if t.restoreType == meta.RevertSnap2TargetOldDrive {
			_ = t.reporter.ReportInfo(StepRevertShadowCopy, s.SCopyPath, s.VolLetter)
			logger.Fmt.Infof("%v.revertOneSnapshot restore to target-old-drive", t.Str())
			if err = t.copyFilesFromSnapshot2LocalOrigin(s); err != nil {
				logger.Fmt.Warnf("%v.revertOneSnapshot CopyFilesInSnapshot ERR=%v", t.Str(), err)
				return err
			}
		} else if t.restoreType == meta.RevertSnap2OriginNewDrive {
			logger.Fmt.Infof("%v.revertOneSnapshot restore to origin-new-drive", t.Str())
			s.ShareName = t.generateShareName(s.VolLetter)
			if err = CreateShareFolder(s.ShareName, s.Mountpoint); err != nil {
				logger.Fmt.Warnf("%v.revertOneSnapshot CreateShareFolder ERR=%v", t.Str(), err)
				return err
			}
			_ = t.reporter.ReportInfo(StepCreateShare, s.Mountpoint, t.originClient.IP, s.ShareName)
			s.IsShare = true
			if err = t.updateSnapshotStatus(); err != nil {
				return err
			}
			var drive string
			//if drive, err = t.connectShareFolder(s.ShareName); err != nil {
			//	logger.Fmt.Warnf("%v.revertOneSnapshot connectShareFolder ERR=%v", t.Str(), err)
			//	return err
			//}
			// TODO 此处仅暴露出网络文件夹，不去做自动连接，交由用户去自动连接
			if drive == meta.UnsetStr {
				_ = t.reporter.ReportInfo(StepLackLettersNetConn, t.originClient.IP, s.ShareName,
					meta.WinShareUser, meta.WinSharePwd)
			}
			s.ShareRemote = append(s.ShareRemote, ShareRemoteInfo{Node: t.restoreClientID, Drive: drive})
			if err = t.updateSnapshotStatus(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *SnapshotRestore) updateSnapshotStatus() (err error) {
	ss, err := json.Marshal(t.fixedSnaps)
	if err != nil {
		logger.Fmt.Warnf("%v.updateSnapshotStatus Marshal ERR=%v", t.Str(), err)
		return err
	}
	if err = models.UpdateSnapByID(t.DBDriver.DB, t.versionObj.ID, string(ss)); err != nil {
		logger.Fmt.Warnf("%v.updateSnapshotStatus UpdateSnapByID ERR=%v", t.Str(), err)
		return err
	}
	return nil
}

type ConnShareDrive struct {
	Code  int64  `json:"code"`
	Msg   string `json:"msg"`
	Drive string `json:"data"`
}

func (t *SnapshotRestore) connectShareFolder(sharename string) (drive string, err error) {
	url := fmt.Sprintf(`https://%s:%v/api/v1/smb/connect`, t.restoreClient.IP, t.restoreClient.Port)
	form := map[string]string{
		"share_ip":   t.originClient.IP,
		"share_name": sharename,
	}
	out, err := RequestUrl(http.MethodPost, url, form)
	logger.Fmt.Infof("%v.connectShareFolder | out(%v)", t.Str(), string(out))
	if err != nil {
		return "", err
	}
	var csd ConnShareDrive
	if err = json.Unmarshal(out, &csd); err != nil {
		logger.Fmt.Warnf("%v.connectShareFolder Unmarshal ERR=%v", t.Str(), err)
		return csd.Drive, err
	}
	return csd.Drive, nil
}

func (t *SnapshotRestore) generateShareName(drive string) string {
	// 卷+时间
	timeStr := t.versionObj.Time.Format("2006-01-02_15-04-05")
	version := strings.ReplaceAll(strings.ReplaceAll(timeStr, "-", ""), "_", "")
	return fmt.Sprintf("%s.%s", drive, version)
}

func (t *SnapshotRestore) copyFilesFromSnapshot2LocalOrigin(s *ShadowCopyIns) (err error) {
	logger.Fmt.Infof("%v.copyFilesFromSnapshot2LocalOrigin will handle snap `%v`", t.Str(), pretty.Sprint(s))
	for _, target := range s.Dirs {
		source := strings.Replace(target, s.VolLetter+":", s.Mountpoint, 1)
		logger.Fmt.Infof("%v.copyFilesFromSnapshot2LocalOrigin will copy `%v` to `%v`", t.Str(), source, target)
		_ = t.reporter.ReportInfo(StepRevertOneDir, s.Mountpoint, target)
		if err = MirrorDir(source, target); err != nil {
			return err
		}
	}
	return nil
}

func MountSnapshotOnTmpPath(snapshot, path string) (err error) {
	logger.Fmt.Infof("MountSnapshotOnTmpPath snapshot(%v) to path(%v)", snapshot, path)
	r, o, e := tools.Process(meta.VShadow, fmt.Sprintf(`-el=%s,%s`, snapshot, path))
	logger.Fmt.Infof("MountSnapshotOnTmpPath `%s` -> `%s` | r(%v) out(%v) e(%v)", snapshot, path, r, o, e)
	if r != 0 || e != nil {
		return fmt.Errorf("failed to expose %v to %v", snapshot, path)
	}
	return nil
}

func MirrorDir(source, target string) (err error) {
	// 命令示例：
	// robocopy <source> <destnation> /mir /copyall /r:0 /w:5 /np /ns /ndl /nfl /nc
	// 退出代码：
	// 0 未复制任何文件。 未遇到任何失败。 没有不匹配的文件。 目标目录中已存在这些文件;因此，跳过了复制操作。
	// 1 已成功复制所有文件。
	// 2 目标目录中存在一些其他文件，这些文件不在源目录中。 未复制任何文件。
	// 3 复制了一些文件。 存在其他文件。 未遇到任何失败。
	// 5 复制了一些文件。 某些文件不匹配。 未遇到任何失败。
	// 6 存在其他文件和不匹配的文件。 未复制任何文件，也不会遇到任何故障。 这意味着文件已存在于目标目录中。
	// 7 文件已复制，存在文件不匹配，并且存在其他文件。
	// 8 多个文件未复制。
	logger.Fmt.Infof("MirrorDir source<%v> target<%v>", source, target)
	r, o, e := tools.Process("robocopy",
		fmt.Sprintf(`%s %s /mir /copyall /r:0 /w:5 /np /ns /ndl /nfl /nc`, source, target))
	logger.Fmt.Infof("MirrorDir `%s` -> `%s` | r(%v) out(%v) e(%v)", source, target, r, o, e)
	if e != nil {
		return e
	}
	// TODO 忽略异常
	return nil
}

func (t *SnapshotRestore) recycleSnapshotAfterVersion() {
	// 删除该点时间以后的所有快照
	snaps, e := models.QuerySnapshotsAfterVersion(t.DBDriver.DB, t.configObj.ID, t.versionObj.ID)
	if e != nil {
		logger.Fmt.Warnf("%v.recycleSnapshotAfterVersion QuerySnapshotsAfterVersion ERR=%v", t.Str(), e)
		return
	}
	if e = models.DeleteSnapshotsAfterID(t.DBDriver.DB, t.configObj.ID, t.versionObj.ID); e != nil {
		logger.Fmt.Warnf("%v.recycleSnapshotAfterVersion DeleteSnapshotsAfterID ERR=%v", t.Str(), e)
		return
	}
	for _, snap := range snaps {
		var scis []ShadowCopyIns
		if e := json.Unmarshal([]byte(snap.Snap), &scis); e != nil {
			logger.Fmt.Warnf("%v.recycleSnapshotAfterVersion Unmarshal2ShadowCopyIns ERR=%v", t.Str(), e)
			continue
		}
		for _, sci_ := range scis {
			_ = DeleteShareInShadowCopy(t.DBDriver.DB, t.configObj.TargetHostJson, sci_)
			_ = DeleteVSS(sci_.SnapID)
		}
	}
}
