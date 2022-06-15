package logic

import (
	"io"
	"io/ioutil"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/models"
	"os"
	"strings"
)

func RegisterServerInfo(ip string) (err error) {
	var (
		fp  *os.File
		rb  []byte
		rs  string
		end int64
	)
	fp, err = os.OpenFile(meta.ServerIPsWin, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		logger.Fmt.Errorf("RegisterServerInfo IP(%s) Err=%v", ip, err)
		return
	}
	defer fp.Close()

	rb, err = ioutil.ReadAll(fp)
	if err != nil {
		logger.Fmt.Errorf("RegisterServerInfo ReadAll Err=%v", err)
		return
	}
	rs = string(rb)

	if strings.Contains(rs, ip+",") {
		logger.Fmt.Infof("RegisterServerInfo contains(%s)", ip)
		return nil
	}
	end, err = fp.Seek(0, io.SeekEnd)
	if err != nil {
		logger.Fmt.Infof("RegisterServerInfo seek Err=%v", err)
		return
	}
	_, err = fp.WriteAt([]byte(ip+","), end)
	if err != nil {
		logger.Fmt.Infof("RegisterServerInfo WriteAt Err=%v", err)
		return
	}
	return nil
}

func ReloadCDPTask() {
	var (
		err error
		fp  *os.File
		rb  []byte
		rs  string
	)

	if _, err = os.Stat(meta.ServerIPsWin); err != nil {
		logger.Fmt.Infof("ReloadCDPTask not existed `%s`", meta.ServerIPsWin)
		return
	}

	fp, err = os.Open(meta.ServerIPsWin)
	if err != nil {
		logger.Fmt.Errorf("ReloadCDPTask Open Err=%v", err)
		return
	}
	defer fp.Close()

	rb, err = ioutil.ReadAll(fp)
	if err != nil {
		logger.Fmt.Errorf("ReloadCDPTask ReadAll Err=%v", err)
		return
	}
	rs = string(rb)

	for _, ip := range strings.Split(rs, ",") {
		if ip == "" {
			continue
		}
		if err = reloadTaskFromOneServer(ip); err != nil {
			logger.Fmt.Warnf("ReloadCDPTask reloadTaskFromOneServer IP(%s) ERR=%v", ip, err)
			continue
		}
	}
}

func reloadTaskFromOneServer(ip string) (err error) {
	var (
		dp *models.DBProxy
		cs []models.ConfigModel
		rs []models.RestoreTaskModel
	)

	dp, err = models.NewDBProxy(ip)
	if err != nil {
		return
	}

	rs, err = models.QueryAllRestoreTasks(dp.DB)
	if err != nil {
		return
	}

	for _, c := range rs {
		if err = models.FailedRestoreTask(dp.DB, c.ID); err != nil {
			return
		}
	}

	cs, err = models.QueryAllEnabledConfigs(dp.DB)
	if err != nil {
		return
	}

	for _, c := range cs {
		if err = LoadCDP(ip, c.ID, true); err != nil {
			return
		}
	}

	return nil
}
