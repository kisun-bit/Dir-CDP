package logic

import (
	"errors"
	"fmt"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/tools"
	"strings"
)

func CreateShareLocalUser() (err error) {
	if !meta.IsWin {
		return errors.New("unsupported platform")
	}
	r, o, err := tools.Process("net", "users")
	//logger.Fmt.Infof("CreateShareLocalUser exec `net users` returns %v | %v | %v", r, o, err)
	if err != nil {
		return errors.New("failed to query local users")
	}
	if strings.Contains(o, meta.WinShareUser) {
		return nil
	}
	r, o, err = tools.Process("net",
		fmt.Sprintf("user /add %s %s", meta.WinShareUser, meta.WinSharePwd))
	logger.Fmt.Infof("CreateShareLocalUser exec `net user /add` returns %v | %v | %v", r, o, err)
	if err != nil || r != 0 {
		return errors.New("failed to create local user " + meta.WinShareUser)
	}
	r, o, err = tools.Process("net",
		fmt.Sprintf("localgroup administrators %s /add", meta.WinShareUser))
	logger.Fmt.Infof("CreateShareLocalUser exec `net user localgroup` returns %v | %v | %v", r, o, err)
	if err != nil || r != 0 {
		return errors.New("failed to grant user " + meta.WinShareUser)
	}
	return nil
}

func CreateShareFolder(shareName, folder string) (err error) {
	if !meta.IsWin {
		return errors.New("unsupported platform")
	}
	if strings.HasSuffix(folder, ":") {
		folder += "\\"
	}
	args := fmt.Sprintf("share %s=%s /grant:RunstorShare,FULL", shareName, folder)
	r, o, err := tools.Process("net", args)
	logger.Fmt.Infof("CreateShareFolder exec `net %v` returns %v | %v | %v", args, r, o, err)
	if err != nil || r != 0 {
		return errors.New("failed to share " + folder)
	}
	return nil
}

func ConnectShareFolderAsDrive(ip, shareName, drive string) (err error) {
	if !meta.IsWin {
		return errors.New("unsupported platform")
	}
	args := fmt.Sprintf(`use %s \\%s\%s /user:%s %s`, drive, ip, shareName, meta.WinShareUser, meta.WinSharePwd)
	r, o, err := tools.Process("net", args)
	logger.Fmt.Infof("ConnectShareFolderAsDrive exec `net %v` returns %v | %v | %v", args, r, o, err)
	if err != nil || r != 0 {
		return errors.New("failed to map share " + shareName)
	}
	return nil
}

func ConnectShareFolder(ip, shareName string) (err error) {
	if !meta.IsWin {
		return errors.New("unsupported platform")
	}
	args := fmt.Sprintf(`use \\%s\%s /user:%s %s`, ip, shareName, meta.WinShareUser, meta.WinSharePwd)
	r, o, err := tools.Process("net", args)
	logger.Fmt.Infof("ConnectShareFolder exec `net %v` returns %v | %v | %v", args, r, o, err)
	if err != nil || r != 0 {
		return errors.New("failed to map share " + shareName)
	}
	return nil
}

func DeleteShare(shareName string) (err error) {
	if !meta.IsWin {
		return errors.New("unsupported platform")
	}
	args := fmt.Sprintf(`share %s /delete /y`, shareName)
	r, o, err := tools.Process("net", args)
	logger.Fmt.Infof("DeleteShare exec `net %v` returns %v | %v | %v", args, r, o, err)
	if err != nil || r != 0 {
		return fmt.Errorf(`failed to delete share %s`, shareName)
	}
	return nil
}

func DisconnectShareDrive(ip, name, drive string) (err error) {
	if !meta.IsWin {
		return errors.New("unsupported platform")
	}
	var args string
	if drive != meta.UnsetStr {
		args = fmt.Sprintf(`use %s /del /y`, drive)
	} else {
		args = fmt.Sprintf(`use \\%s\%s /del /y`, ip, name)
	}

	r, o, err := tools.Process("net", args)
	logger.Fmt.Infof("DisconnectShareDrive exec `net %v` returns %v | %v | %v", args, r, o, err)
	if err != nil || r != 0 {
		return fmt.Errorf(`failed to delete share drive %s`, drive)
	}
	return nil
}

func DisconnectShareFolder(ip, shareName string) (err error) {
	if !meta.IsWin {
		return errors.New("unsupported platform")
	}
	args := fmt.Sprintf(`use \\%s\%s /del /y`, ip, shareName)
	r, o, err := tools.Process("net", args)
	logger.Fmt.Infof("DisconnectShareFolder exec `net %v` returns %v | %v | %v", args, r, o, err)
	if err != nil || r != 0 {
		return fmt.Errorf(`failed to delete share folder %s`, shareName)
	}
	return nil
}

//func NetUseDetail() (out string, err error) {
//	if !meta.IsWin {
//		return "", errors.New("unsupported platform")
//	}
//	r, o, err := tools.Process("net", "use")
//	logger.Fmt.Infof("NetUseDetail exec `net use` returns %v | %v | %v", r, o, err)
//	if err != nil || r != 0 {
//		return "", errors.New("failed to query share")
//	}
//	return o, nil
//}
