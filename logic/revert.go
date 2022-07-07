package logic

import (
	"encoding/json"
	"fmt"
	"github.com/kr/pretty"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/models"
	"jingrongshuan/rongan-fnotify/tools"
	"strings"
)

// TargetMachineRevert 目标机回滚卷影副本
type TargetMachineRevert struct {
	DBDriver       *models.DBProxy // 数据库驱动器
	configObj      *models.ConfigModel
	revertObj      *models.RestoreTaskModel
	versionObj     *models.Snapshot
	reporter       *Reporter
	curSnaps       []ShadowCopyIns
	curSnapsDirMap []SnapshotMappingTargetDirs
}

func NewTargetMachineRevert(db *models.DBProxy,
	config *models.ConfigModel, revert *models.RestoreTaskModel, version *models.Snapshot) *TargetMachineRevert {
	return &TargetMachineRevert{
		DBDriver:   db,
		configObj:  config,
		revertObj:  revert,
		versionObj: version,
		reporter:   NewReporter(db.DB, config.ID, revert.ID, meta.TaskTypeRestore, AllLevels),
	}
}

func (t *TargetMachineRevert) Str() string {
	return fmt.Sprintf("<TargetMachineRevert(config=%v, revert=%v, version=%v)>",
		t.configObj.ID, t.revertObj.ID, t.versionObj.Ver)
}

func (t *TargetMachineRevert) Start() {
	_ = t.reporter.ReportInfo(StepReceiveRevertInstruct, t.versionObj.Ver)
	go t.logic()
}

func (t *TargetMachineRevert) logic() {
	var err error
	defer func() {
		if err != nil {
			if e := models.UpdateRestoreTask(t.DBDriver.DB, t.revertObj.ID, meta.RESTORESERROR); e != nil {
				logger.Fmt.Warnf("%v.logic defer UpdateRestoreTask ERR=%v", t.Str(), err)
			}
			if e := models.EndRestoreTask(t.DBDriver.DB, t.revertObj.ID, false); e != nil {
				logger.Fmt.Warnf("%v.logic defer EndRestoreTask ERR=%v", t.Str(), err)
			}
			_ = t.reporter.ReportError(StepRevertErr, t.versionObj.Ver, err.Error())
		} else {
			if e := models.UpdateRestoreTask(t.DBDriver.DB, t.revertObj.ID, meta.RESTOREFINISH); e != nil {
				logger.Fmt.Warnf("%v.logic defer UpdateRestoreTask ERR=%v", t.Str(), err)
			}
			if e := models.EndRestoreTask(t.DBDriver.DB, t.revertObj.ID, true); e != nil {
				logger.Fmt.Warnf("%v.logic defer EndRestoreTask ERR=%v", t.Str(), err)
			}
			_ = t.reporter.ReportInfo(StepFinishRevert, t.versionObj.Ver)
		}
	}()
	if err = models.UpdateRestoreTask(t.DBDriver.DB, t.revertObj.ID, meta.RESTORESTART); err != nil {
		logger.Fmt.Warnf("%v.logic UpdateRestoreTask ERR=%v", t.Str(), err)
		return
	}
	if err = t.configObj.LoadsJsonFields(t.DBDriver.DB); err != nil {
		logger.Fmt.Warnf("%v.logic LoadsJsonFields ERR=%v", t.Str(), err)
		return
	}
	if err = t.loadCurSnaps(); err != nil {
		logger.Fmt.Warnf("%v.logic loadCurSnaps ERR=%v", t.Str(), err)
		return
	}
	_ = t.reporter.ReportInfo(StepLoadRevertConfig, t.versionObj.Ver)
	if err = t.mapOriginDir2Snapshot(); err != nil {
		logger.Fmt.Warnf("%v.logic mapOriginDir2Snapshot ERR=%v", t.Str(), err)
		return
	}
	_ = t.reporter.ReportInfo(StepMatchDirFromSnapshot)
	defer t.recycleSnapshotAfterVersion()
	if err = models.UpdateRestoreTask(t.DBDriver.DB, t.revertObj.ID, meta.RESTOREING); err != nil {
		logger.Fmt.Warnf("%v.logic UpdateRestoreTask ERR=%v", t.Str(), err)
		return
	}
	if err = t.revertAllOriginDirs(); err != nil {
		logger.Fmt.Warnf("%v.logic revertAllOriginDirs ERR=%v", t.Str(), err)
		return
	}
}

func (t *TargetMachineRevert) loadCurSnaps() (err error) {
	if err := json.Unmarshal([]byte(t.versionObj.Snap), &t.curSnaps); err != nil {
		return err
	}
	logger.Fmt.Infof("%v.loadCurSnaps snapshot => \n%v", t.Str(), pretty.Sprint(t.curSnaps))
	return nil
}

func (t *TargetMachineRevert) mapOriginDir2Snapshot() (err error) {
	for _, sci := range t.curSnaps {
		tmp := SnapshotMappingTargetDirs{}
		tmp.SCI = sci
		tmp.Dirs = make([]string, 0)
		for _, d := range t.configObj.DirsMappingJson {
			if strings.HasPrefix(d.StoragePrefixForBucketOrRemote, tmp.SCI.VolLetter+":") {
				tmp.Dirs = append(tmp.Dirs, d.StoragePrefixForBucketOrRemote)
			}
		}
		tmp.IsSysVol = tmp.SCI.VolLetter == "C"
		tmp.ClientAccess = strings.Contains(tmp.SCI.Attribute, meta.ClientAccessible)
		t.curSnapsDirMap = append(t.curSnapsDirMap, tmp)
	}
	logger.Fmt.Infof("%v.mapOriginDir2Snapshot map => \n%v", t.Str(), pretty.Sprint(t.curSnapsDirMap))
	return nil
}

func (t *TargetMachineRevert) revertAllOriginDirs() (err error) {
	for _, s := range t.curSnapsDirMap {
		if err = t.revertOneSnapshot(s); err != nil {
			return err
		}
	}
	return nil
}

func (t *TargetMachineRevert) revertOneSnapshot(s SnapshotMappingTargetDirs) (err error) {
	logger.Fmt.Infof("%v.revertOneSnapshot snapshot_map_dir => \n%v", t.Str(), pretty.Sprint(s))
	if s.ClientAccess {
		// VSSAdmin强制回滚
		_ = t.reporter.ReportInfo(StepRevertShadowCopy, s.SCI.SCopyPath, s.SCI.VolLetter)
		return RevertSnapshotByVssAdmin(s.SCI.SnapID)
	} else {
		// VShadow先挂载到临时卷、在使用robocopy镜像拷贝
		s.Mountpoint, err = tools.MallocDrive()
		if err != nil {
			logger.Fmt.Warnf("%v.revertOneSnapshot MallocDrive ERR=%v", t.Str(), err)
			return err
		}
		if err = MountSnapshotOnDrive(s.SCI.SnapID, s.Mountpoint); err != nil {
			logger.Fmt.Warnf("%v.revertOneSnapshot MountSnapshotOnDrive ERR=%v", t.Str(), err)
			return err
		}
		_ = t.reporter.ReportInfo(StepLoadSnapshot, s.Mountpoint)
		if err = t.copyFilesFromSnapshot2Origin(s); err != nil {
			logger.Fmt.Warnf("%v.revertOneSnapshot CopyFilesInSnapshot ERR=%v", err)
			return err
		}
	}
	return nil
}

func (t *TargetMachineRevert) copyFilesFromSnapshot2Origin(s SnapshotMappingTargetDirs) (err error) {
	logger.Fmt.Infof("%v.copyFilesFromSnapshot2Origin will handle snap `%v`", t.Str(), pretty.Sprint(s))
	for _, target := range s.Dirs {
		source := strings.Replace(target, s.SCI.VolLetter+":", s.Mountpoint, 1)
		logger.Fmt.Infof("%v.copyFilesFromSnapshot2Origin will copy `%v` to `%v`", t.Str(), source, target)
		_ = t.reporter.ReportInfo(StepRevertOneDir, s.Mountpoint, target)
		if err = MirrorDir(source, target); err != nil {
			return err
		}
	}
	return nil
}

func MountSnapshotOnDrive(snapshot, drive string) (err error) {
	logger.Fmt.Infof("MountSnapshotOnDrive snapshot(%v) to drive(%v)", snapshot, drive)
	r, o, e := tools.Process(VShadow, fmt.Sprintf(`-el=%s,%s`, snapshot, drive))
	logger.Fmt.Infof("MountSnapshotOnDrive `%s` -> `%s` | r(%v) out(%v) e(%v)", snapshot, drive, r, o, e)
	if r != 0 || e != nil {
		return e
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

func (t *TargetMachineRevert) recycleSnapshotAfterVersion() {
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
			_ = DeleteVSS(sci_.SnapID)
		}
	}
}
