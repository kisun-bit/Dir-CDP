package logic

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gofrs/uuid"
	"gorm.io/gorm"
	"io/ioutil"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/models"
	"jingrongshuan/rongan-fnotify/tools"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

const VShadow = `C:\rongan\cdp\vshadow.exe`
const PowerShell = "powershell.exe"
const DiskShadowScriptsDir = `C:\rongan\cdp\tmp`

func init() {
	if _, err := os.Stat(DiskShadowScriptsDir); err != nil {
		_ = os.MkdirAll(DiskShadowScriptsDir, meta.DefaultFileMode)
	}
}

//var regexShadowCopyVol = regexp.MustCompile(`.*?\((?P<VOL>\w):\)\\\\\?\\VolLetter{(?P<VID>.*?)}`)
var RegexOriginalVolume = regexp.MustCompile(`\\\\\?\\Volume{(?P<VID>.*?)}\\.*?\[(?P<VOL>\w+):\\\]`)

// Shadow copy storage instance
type ShadowCopyIns struct {
	VolLetter     string            `json:"vol_letter"`       // Disk letter
	VolID         string            `json:"vol_id"`           // Disk identification
	SnapID        string            `json:"snap_id"`          // shadow copy ID
	Valid         bool              `json:"existed"`          // Whether the creation is successful
	SCopyPath     string            `json:"shadow_copy_path"` // Shadow copy volume name, for example "\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy5"
	OriginMachine string            `json:"origin_machine"`   // Originating machine
	ServerMachine string            `json:"server_machine"`   // Service machine
	CreationTime  string            `json:"creation_time"`    // Creation time
	Provider      string            `json:"provider"`         // Provider
	Type          string            `json:"type"`             // Shadow copy type
	Attribute     string            `json:"attribute"`        // Attributes, such as “持续|无自动释放|差异”
	Size          uint64            `json:"size"`             // Shadow size
	SizeHuman     string            `json:"size_human"`       // readable size
	IsShare       bool              `json:"is_share"`         // 是否将快照共享
	ShareName     string            `json:"share_name"`       // 共享文件夹名称
	Mountpoint    string            `json:"mountpoint"`       // mount snapshot to local path
	ShareRemote   []ShareRemoteInfo `json:"share_remotes"`    // 共享到了哪些主机
	IsSysVol      bool              `json:"is_sys_vol"`       // 是否为系统卷
	Dirs          []string          `json:"dirs"`             // 下属的源机备份目录
	ClientAccess  bool              `json:"client_access"`    // 是否暴露给系统客户端
}

type ShareRemoteInfo struct {
	Node  int64  `json:"node"`
	Drive string `json:"drive"`
}

// SnapCreator 用于定时创建快照
// UNSTART阶段开始前，会创建一个快照
// COPYING阶段不会创建快照
// CDPING阶段会按照cycle、keep参数创建快照
type SnapCreator struct {
	config   *models.ConfigModel
	task     *models.BackupTaskModel
	db       *gorm.DB
	reporter *Reporter
	stop     bool
}

func (s *SnapCreator) Str() string {
	return fmt.Sprintf(`<SnapCreator(config=%v, task=%v)>`, s.config.ID, s.task.ID)
}

func (s *SnapCreator) SetStop() {
	if s.stop {
		return
	}
	s.stop = true
}

func (s *SnapCreator) start() {
	go s.logic()
}

func (s *SnapCreator) logic() {
	for {
		if s.stop {
			logger.Fmt.Infof("%v.logic stop", s.Str())
			return
		}
		if err := s.loadConfig(); err != nil {
			logger.Fmt.Warnf("%v.logic loadConfig ERR=%v", s.Str(), err)
			return
		}
		if s.disable() {
			return
		}
		if s.config.TargetJson.TargetType != meta.WatchingConfTargetHost {
			logger.Fmt.Infof("%v.logic unsupported target type `%v`", s.Str(), s.config.TargetJson.TargetType)
			return
		}
		ss, err := s.create()
		if err != nil {
			logger.Fmt.Warnf("%v.logic create ERR=%v", s.Str(), err)
			goto SLEEP
		}
		if err = s.record(ss); err != nil {
			logger.Fmt.Warnf("%v.logic record ERR=%v", s.Str(), err)
			goto SLEEP
		}
		if e := s.recycle(); e != nil {
			logger.Fmt.Warnf("%v.logic recycle ERR=%v", s.Str(), e)
			goto SLEEP
		}
	SLEEP:
		time.Sleep(time.Duration(s.config.ExtInfoJson.SnapshotPolicy.CycleSecs) * time.Second)
	}
}

func (s *SnapCreator) generateVersion() string {
	u, _ := uuid.NewV4()
	return u.String()
}

func (s *SnapCreator) disable() bool {
	return s.config.ExtInfoJson.SnapshotPolicy.CycleSecs == 0 || s.config.ExtInfoJson.SnapshotPolicy.CycleSecs == -1
}

func (s *SnapCreator) create() (ss []ShadowCopyIns, err error) {
	for _, letter := range s.config.UniqTargetDriveLetters() {
		sci, e := s.createInRemote(letter)
		if e != nil {
			_ = s.reporter.ReportErrorWithoutLogWithKey(meta.RuntimeSnapshot, StepCreateSnapshotErr, letter, e)
			return ss, e
		}
		_ = s.reporter.ReportInfoWithoutLogWithKey(meta.RuntimeSnapshot, StepCreateSnapshot, letter, sci.SnapID)
		ss = append(ss, sci)
	}
	return
}

func (s *SnapCreator) record(ss []ShadowCopyIns) (err error) {
	version := s.generateVersion()
	defer func() {
		if err == nil {
			_ = s.reporter.ReportInfoWithoutLogWithKey(meta.RuntimeSnapshot, StepCreateVersion, version)
		} else {
			_ = s.reporter.ReportErrorWithoutLogWithKey(meta.RuntimeSnapshot, StepCreateVersionErr, version, err)
		}
	}()
	now := time.Now()
	sb, err := json.Marshal(ss)
	if err != nil {
		return err
	}
	return models.CreateSnapshot(s.db, models.Snapshot{
		Ver:    version,
		Time:   &now,
		Snap:   string(sb),
		Type:   meta.SnapTypeVSS,
		Target: s.config.TargetHostJson.ID,
		Origin: s.config.Origin,
		Config: s.config.ID,
		Task:   s.task.ID,
	})
}

func (s *SnapCreator) recycle() (err error) {
	count, err := models.CountSnapshots(s.db, s.config.ID, s.task.ID)
	if err != nil {
		return err
	}
	if count < s.config.ExtInfoJson.SnapshotPolicy.Keep {
		return nil
	}
	vs, err := models.QueryNeedlessSnapshots(s.db, s.config.ID, s.task.ID, s.config.ExtInfoJson.SnapshotPolicy.Keep)
	if err != nil {
		return err
	}
	for _, s_ := range vs {
		var scis []ShadowCopyIns
		if err = json.Unmarshal([]byte(s_.Snap), &scis); err != nil {
			// do nothing
			_ = models.DeleteSnapshotByID(s.db, s_.ID)
			continue
		}
		for _, sci := range scis {
			if err = s.deleteInRemote(sci.SnapID); err == nil {
				_ = s.reporter.ReportInfoWithoutLogWithKey(
					meta.RuntimeSnapshot, StepDeleteSnapshot, sci.VolLetter+":", sci.SnapID)
			} else {
				_ = s.reporter.ReportErrorWithoutLogWithKey(
					meta.RuntimeSnapshot, StepDeleteSnapshotErr, sci.VolLetter+":", sci.SnapID, err)
			}
			_ = DeleteShareInShadowCopy(s.db, s.config.TargetHostJson, sci)
		}
		if err != nil {
			_ = s.reporter.ReportErrorWithoutLogWithKey(
				meta.RuntimeSnapshot, StepRecycleVersionErr, s_.Ver, err)
		} else {
			_ = s.reporter.ReportInfoWithoutLogWithKey(
				meta.RuntimeSnapshot, StepRecycleVersion, s_.Ver)
		}
		_ = models.DeleteSnapshotByID(s.db, s_.ID)
	}
	return
}

func (s *SnapCreator) loadConfig() (err error) {
	if err := s.config.LoadsJsonFields(s.db); err != nil {
		return err
	}
	return nil
}

type SnapCreateHTTPRet struct {
	Code    int64         `json:"code"`
	Message string        `json:"msg"`
	Data    ShadowCopyIns `json:"data"`
}

func (s *SnapCreator) createInRemote(letter string) (sci ShadowCopyIns, err error) {
	var form http.Request
	if err = form.ParseForm(); err != nil {
		return
	}
	form.Form.Add("volume", letter)
	reqBody := strings.TrimSpace(form.Form.Encode())
	url := fmt.Sprintf(`%s://%s:%v/api/v1/create_snapshot`,
		"https", s.config.TargetHostJson.IP, s.config.TargetHostJson.Port)
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(reqBody))
	if err != nil {
		logger.Fmt.Warnf("%v.createInRemote NewRequest letter=%v, ERR=%v", s.Str(), letter, err)
		return sci, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Close = true

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: meta.Pool, InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	resp, err := client.Do(req)
	if err != nil {
		logger.Fmt.Warnf("%v.createInRemote Do letter=%v, ERR=%v", s.Str(), letter, err)
		return sci, err
	}
	defer resp.Body.Close()
	bs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Fmt.Warnf("%v.createInRemote ReadAll letter=%v, ERR=%v", s.Str(), letter, err)
		return sci, err
	}
	if resp.StatusCode != http.StatusOK {
		return sci, fmt.Errorf("bad request for creating vss in `%v`", letter)
	}
	var ret SnapCreateHTTPRet
	err = json.Unmarshal(bs, &ret)
	if err != nil {
		return sci, err
	}
	return ret.Data, nil
}

func (s *SnapCreator) deleteInRemote(snap string) (err error) {
	var form http.Request
	if err = form.ParseForm(); err != nil {
		return
	}

	form.Form.Add("type", meta.SnapTypeVSS)
	form.Form.Add("snap", snap)
	reqBody := strings.TrimSpace(form.Form.Encode())
	url := fmt.Sprintf(`%s://%s:%v/api/v1/delete_snapshot`,
		"https", s.config.TargetHostJson.IP, s.config.TargetHostJson.Port)
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(reqBody))
	if err != nil {
		logger.Fmt.Warnf("%v.createInRemote deleteInRemote ID=%v, ERR=%v", s.Str(), snap, err)
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Close = true

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: meta.Pool, InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	resp, err := client.Do(req)
	if err != nil {
		logger.Fmt.Warnf("%v.deleteInRemote Do ID=%v, ERR=%v", s.Str(), snap, err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad request for delete vss in `%v`", snap)
	}
	return nil
}

func IsSysVol(letter string) bool {
	letter = strings.ToUpper(letter)
	return strings.HasPrefix(letter, "C")
}

func CreateVSS(letter string) (sci ShadowCopyIns, err error) {
	/*说明
	现象：
	1. 系统盘的卷影副本回滚会因为句柄占用导致回滚失败，但是又不能卸载句柄
	2. win版本vssadmin的回滚功能不可用
	3. 非持久性卷影副本和ClientAccessible卷影副本不能在本地公开。
	解决方案：
	1. Server版本、系统盘 --》创建DataVolumeRollback类型的卷影副本
	2. Server版本、数据盘 --》创建ClientAccessible类型的卷影副本
	3. win版本、系统盘、数据盘 --》创建DataVolumeRollback类型的卷影副本
	*/
	var args string
	var r int
	var o string
	var type_ string

	if !strings.HasSuffix(letter, ":") {
		letter += ":"
	}

	//logger.Fmt.Infof("CreateVSS is_win_server(%v) is_sys_vol(%v)", tools.IsWinServer, IsSysVol(letter))
	if tools.IsWinServer && IsSysVol(letter) {
		args = fmt.Sprintf(`-p -nw %v`, letter)
		type_ = meta.DataVolumeRollback
	} else if tools.IsWinServer && !IsSysVol(letter) {
		//args = fmt.Sprintf(`-scsf -p -nw %v`, letter)
		//type_ = meta.ClientAccessible
		args = fmt.Sprintf(`-p -nw %v`, letter)
		type_ = meta.DataVolumeRollback
	} else {
		args = fmt.Sprintf(`-p -nw %v`, letter)
		type_ = meta.DataVolumeRollback
	}
	r, o, err = tools.Process(VShadow, args)
	if r != 0 {
		err = fmt.Errorf("failed to create shadow copy(letter=`%s`) args=`%v` out=%v err=%v", letter, args, o, err)
		return
	}
	if err != nil {
		return sci, err
	}

	/*创建卷影副本输出
	VSHADOW.EXE 2.2 - Volume Shadow Copy sample client
	Copyright (C) 2005 Microsoft Corporation. All rights reserved.

	(Option: Persistent shadow copy)
	(Option: No-writers option detected)
	(Option: Create shadow copy set)
	- Setting the VSS context to: 0x00000019
	Creating shadow set {39135445-99d7-4923-8035-9b4856138bb1} ...
	- Adding volume \\?\Volume{5397c73f-f5db-11ec-80b5-005056a9d215}\ [E:\] to the shadow set...
	Creating the shadow (DoSnapshotSet) ...
	(Waiting for the asynchronous operation to finish...)
	(Waiting for the asynchronous operation to finish...)
	Shadow copy set succesfully created.
	List of created shadow copies:

	Querying all shadow copies with the SnapshotSetID {39135445-99d7-4923-8035-9b4856138bb1} ...

	* SNAPSHOT ID = {e20ec41b-2a7d-48d6-9da5-b5d5f77255d1} ...
	- Shadow copy Set: {39135445-99d7-4923-8035-9b4856138bb1}
	- Original count of shadow copies = 1
	- Original Volume name: \\?\Volume{5397c73f-f5db-11ec-80b5-005056a9d215}\ [E:\]
	- Creation Time: 2022/6/30 11:09:38
	- Shadow copy device name: \\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy48
	- Originating machine: WIN-NBPECAUCU65
	- Service machine: WIN-NBPECAUCU65
	- Not Exposed
	- Provider id: {b5946137-7b9f-4925-af80-51abd60b20d5}
	- Attributes:  No_Auto_Release Persistent No_Writers Differential

	Snapshot creation done.
	*/
	for _, line := range strings.Split(o, "\n") {
		line = strings.TrimSpace(line)
		if !(strings.Contains(line, "SNAPSHOT ID")) {
			continue
		}
		snap := strings.Fields(line)[4]
		if snap == meta.UnsetStr {
			err = fmt.Errorf(`can't find snap id from "%s"`, snap)
			return
		}
		sci, err = DetailVSS(snap)
		if err != nil {
			return
		}
		sci.Type = type_
		return
	}
	err = fmt.Errorf("failed to create shadow copy(letter=`%s`), err=key information was not resolved",
		letter)
	return
}

func RevertSnapshotByVssAdmin(snapshot string) (err error) {
	// vssadmin Revert Shadow /Shadow=<snap_id> /ForceDismount /Quiet
	r, o, e := tools.Process("vssadmin",
		fmt.Sprintf("Revert Shadow /Shadow=%s /ForceDismount /Quiet", snapshot))
	logger.Fmt.Infof("RevertSnapshotByVssAdmin. r(%v) out(%v) err(%v)", r, o, e)
	if r != 0 || e != nil {
		return fmt.Errorf("failed to revert shadow %s", snapshot)
	}
	return nil
}

func DeleteVSS(snap string) (err error) {
	cs := fmt.Sprintf(` -ds=%s`, snap)
	r, o, err := tools.Process(VShadow, cs)
	if r != 0 {
		err = fmt.Errorf("failed to delete shadow copy(id=`%s`) out=%s err=%v", snap, o, err)
		return
	}
	return nil
}

func DetailVSS(snap string) (sci ShadowCopyIns, err error) {
	cs := fmt.Sprintf(`-s=%s`, snap)
	r, o, err := tools.Process(VShadow, cs)
	if r != 0 {
		err = fmt.Errorf("failed to detail shadow copy(id=`%s`) out=%s err=%v", snap, o, err)
		return
	}
	if err != nil {
		return sci, err
	}
	__splitter := func(__line string) string {
		return strings.TrimSpace(strings.Split(__line, ":")[1])
	}
	/* 卷影副本详情输出
	VSHADOW.EXE 2.2 - Volume Shadow Copy sample client
	Copyright (C) 2005 Microsoft Corporation. All rights reserved.


	(Option: Query shadow copy)
	- Setting the VSS context to: 0xffffffff
	* SNAPSHOT ID = {9425a14c-7dd9-433d-bba4-0a1661646493} ...
	- Shadow copy Set: {49228352-ba88-4b4a-8eb4-b3cd496c1fa2}
	- Original count of shadow copies = 1
	- Original Volume name: \\?\Volume{e3b9397c-0000-0000-0000-f0ff18000000}\ [D:\]
	- Creation Time: 2022/6/30 10:10:25
	- Shadow copy device name: \\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy11
	- Originating machine: DESKTOP-54OP5G1
	- Service machine: DESKTOP-54OP5G1
	- Not Exposed
	- Provider id: {b5946137-7b9f-4925-af80-51abd60b20d5}
	- Attributes:  No_Auto_Release Persistent No_Writers Differential
	*/
	lines := strings.Split(o, "\n")
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, snap) {
			continue
		}
		match := RegexOriginalVolume.FindStringSubmatch(lines[i+3])
		if len(match) == 1 || len(match) != 3 {
			errMsg := fmt.Sprintf("why not match `%v` from `%v`", RegexOriginalVolume, line[i+3])
			return sci, errors.New(errMsg)
		}
		sci.Valid = true
		sci.VolLetter = match[2]
		sci.VolID = fmt.Sprintf("{%s}", match[1])
		sci.SnapID = snap
		sci.CreationTime = strings.TrimPrefix(strings.TrimSpace(lines[i+4]), "- Creation Time: ")
		sci.SCopyPath = __splitter(lines[i+5])
		sci.OriginMachine = __splitter(lines[i+6])
		sci.ServerMachine = __splitter(lines[i+7])
		sci.Provider = __splitter(lines[i+9])
		sci.Type = meta.DataVolumeRollback
		sci.Attribute = __splitter(lines[i+10])

		va, e := tools.VolumeUsage(sci.VolLetter)
		if e == nil {
			sci.Size = va.Total
			sci.SizeHuman = tools.HumanizeBytes(int64(sci.Size))
		}
	}
	return
}

func DeleteShareInShadowCopy(db *gorm.DB, CIFSServer models.ClientNode, sci ShadowCopyIns) (err error) {
	// 在连接端删除共享文件夹
	for _, sr := range sci.ShareRemote {
		target, e_ := models.QueryClientNodeByID(db, sr.Node)
		if e_ != nil {
			logger.Fmt.Warnf("DeleteShareInShadowCopy query node err=%v", e_)
			continue
		}
		form := map[string]string{
			"share_ip":   CIFSServer.IP,
			"share_name": sci.ShareName,
			"drive":      sr.Drive,
		}
		url := fmt.Sprintf("https://%s:%v/api/v1/smb/disconnect", target.IP, target.Port)
		o, e := RequestUrl(http.MethodPost, url, form)
		logger.Fmt.Infof("DeleteShareInShadowCopy smb disconnect | form(%v) url(%v) out(%v) err(%v)",
			form, url, string(o), e)
	}
	if sci.ShareName != meta.UnsetStr {
		// 在共享服务端停止共享
		form := map[string]string{
			"share_name": sci.ShareName,
		}
		url := fmt.Sprintf("https://%s:%v/api/v1/smb/delete",
			CIFSServer.IP, CIFSServer.Port)
		o, e := RequestUrl(http.MethodPost, url, form)
		logger.Fmt.Infof("DeleteShareInShadowCopy smb delete | form(%v) url(%v) out(%v) err(%v)",
			form, url, string(o), e)
	}
	return nil
}
