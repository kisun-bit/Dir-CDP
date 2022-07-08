package tools

import (
	"bytes"
	"errors"
	"fmt"
	goCmd "github.com/go-cmd/cmd"
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/aws/credentials"
	"github.com/journeymidnight/aws-sdk-go/aws/session"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	gos3 "github.com/kisun-bit/go-s3"
	"github.com/shirou/gopsutil/disk"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
	"io/ioutil"
	"jingrongshuan/rongan-fnotify/meta"
	version2 "jingrongshuan/rongan-fnotify/tools/os_version/version"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ConvertMode2FType os.FileMode转FileType
func ConvertMode2FType(mode os.FileMode) meta.FileType {
	if mode&os.ModeSymlink != 0 {
		return meta.FTLink
	} else if mode&os.ModeDir != 0 {
		return meta.FTDir
	} else if strings.HasPrefix(mode.String(), "-") {
		return meta.FTFile
	} else {
		return meta.FTOther // a、T、L、D、p、S、u、g、c、t?
	}
}

// ConvertToWin32Event 监控事件标记转Event
func ConvertToWin32Event(no uint32) meta.Event {
	if no > uint32(meta.Win32EventRenameTo) || no < uint32(meta.Win32EventCreate) {
		panic("invalid win32 event")
	}
	return meta.Event(no)
}

func NewS3Client(
	ak,
	sk,
	endpoint,
	region string,
	ssl,
	path bool,
) (*session.Session, gos3.S3Client) {
	S3Session := session.Must(
		session.NewSessionWithOptions(
			session.Options{
				Config: aws.Config{
					Credentials:      credentials.NewStaticCredentials(ak, sk, ""),
					DisableSSL:       aws.Bool(!ssl),
					Endpoint:         aws.String(endpoint),
					Region:           aws.String(region),
					S3ForcePathStyle: aws.Bool(path),
				},
			},
		),
	)
	s3client := s3.New(S3Session)
	return S3Session, gos3.S3Client{Client: s3client}
}

func GenerateS3Key(conf int64, root, local, prefix string) string {
	relative, err := filepath.Rel(root, local)
	if err != nil {
		return meta.UnsetStr
	}
	if prefix != meta.UnsetStr {
		return fmt.Sprintf("%s/%s", prefix, relative)
	}
	return fmt.Sprintf("notify%v/%v", conf, relative)
}

func GenerateRemoteHostKey(root, local, remote string, targetWin bool) string {
	relative, err := filepath.Rel(root, local)
	if err != nil {
		return meta.UnsetStr
	}
	if remote != meta.UnsetStr {
		return CorrectPathWithPlatform(filepath.Join(remote, relative), targetWin)
	}
	return meta.UnsetStr
}

func String2Time(str string) (t time.Time) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		loc = time.FixedZone("CST", 8*3600)
	}
	t, err = time.ParseInLocation("2006-01-02 15:04:05", str, loc)
	if err != nil {
		panic(err)
	}
	return t
}

func IsWin(s string) bool {
	return strings.Contains(strings.ToLower(s), "win")
}

func ConvertModTime2VersionFlag(mod int64) (ver string) {
	ver = time.Unix(mod, 0).Format(meta.TimeFMT)
	ver = strings.ReplaceAll(ver, "-", "")
	ver = strings.ReplaceAll(ver, ":", "")
	ver = strings.ReplaceAll(ver, " ", "")
	return ver
}

func CorrectPathWithPlatform(path_ string, win bool) string {
	if win {
		path_ = strings.ReplaceAll(path_, "/", "\\")
		return strings.ReplaceAll(path_, `\\`, "\\")
	}
	path_ = strings.ReplaceAll(path_, `\`, "/")
	return strings.ReplaceAll(path_, `//`, "/")
}

func CorrectDirWithPlatform(dir string, win bool) string {
	dir = CorrectPathWithPlatform(dir, win)
	if win && !strings.HasSuffix(dir, "\\") {
		return dir + "\\"
	} else if !win && strings.HasSuffix(dir, "/") {
		return dir + "/"
	}
	return dir
}

func HumanizeBytes(bytesNum int64) string {
	var size string

	if valPB := bytesNum / (1 << 50); valPB != 0 {
		num1, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(bytesNum)/float64(1<<50)), 64)
		size = fmt.Sprintf("%vPB", num1)
	} else if valTB := bytesNum / (1 << 40); valTB != 0 {
		num2, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(bytesNum)/float64(1<<40)), 64)
		size = fmt.Sprintf("%vTB", num2)
	} else if valGB := bytesNum / (1 << 30); valGB != 0 {
		num3, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(bytesNum)/float64(1<<30)), 64)
		size = fmt.Sprintf("%vGB", num3)
	} else if valMB := bytesNum / (1 << 20); valMB != 0 {
		num4, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(bytesNum)/float64(1<<20)), 64)
		size = fmt.Sprintf("%vMB", num4)
	} else if valKB := bytesNum / (1 << 10); valKB != 0 {
		num5, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(bytesNum)/float64(1<<10)), 64)
		size = fmt.Sprintf("%vKB", num5)
	} else {
		size = fmt.Sprintf("%vB", bytesNum)
	}

	return size
}

func GBK2UTF8(s []byte) ([]byte, error) {
	reader := transform.NewReader(bytes.NewReader(s), simplifiedchinese.GBK.NewDecoder())
	d, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil, e
	}
	return d, nil
}

func Process(caller, args string) (r int, out string, err error) {
	cs := strings.Fields(args)
	if len(cs) > 0 {
		for i := 0; i < len(cs[1:]); i++ {
			cs[i] = strings.Trim(cs[i], "\"")
		}
	}
	c := goCmd.NewCmd(caller, cs...)
	s := <-c.Start()
	out = strings.Join(s.Stdout, "\n")
	if meta.IsWin {
		ob, err := GBK2UTF8([]byte(out))
		if err == nil {
			out = string(ob)
		}
	}
	return s.Exit, out, s.Error
}

func VolumeUsage(letter string) (va *disk.UsageStat, err error) {
	parts, err := disk.Partitions(true)
	if err != nil {
		return nil, err
	}
	for _, part := range parts {
		if !strings.HasPrefix(part.Device, strings.ToUpper(letter)) {
			continue
		}
		va, err = disk.Usage(part.Mountpoint)
		return
	}
	return nil, errors.New("volume usage not fount")
}

var IsWinServer = true

func init() {
	version, err := version2.OSVersion()
	if err != nil {
		fmt.Printf("InitWinServerFlag ERR=%v\n", err)
		return
	}
	if version != meta.UnsetStr {
		IsWinServer = strings.Contains(version, "Server")
	}
}

var DriveLetters = []string{
	"A:", "B:", "C:", "D:", "E:", "F:", "G:",
	"H:", "I:", "J:", "K:", "L:", "M:", "N:",
	"O:", "P:", "Q:", "R:", "S:", "T:", "U:",
	"V:", "W:", "X:", "Y:", "Z:"}

func MallocDrive() (_ string, err error) {
	// 使用mountvol命令查询所有的挂载点
	_, o, _ := Process("mountvol", "")
	if strings.TrimSpace(o) == meta.UnsetStr {
		return meta.UnsetStr, errors.New("failed to exec `mountvol`")
	}
	for i := range DriveLetters {
		if !strings.Contains(o, DriveLetters[len(DriveLetters)-i-1]) {
			return DriveLetters[len(DriveLetters)-i-1], nil
		}
	}
	return meta.UnsetStr, errors.New("the drive letter was used up")
}

func IPs() (ips []string) {
	interfaceAddr, err := net.InterfaceAddrs()
	if err != nil {
		return ips
	}

	for _, address := range interfaceAddr {
		ipNet, isVailIpNet := address.(*net.IPNet)
		// 检查ip地址判断是否回环地址
		if isVailIpNet && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				ips = append(ips, ipNet.IP.String())
			}
		}
	}
	return ips
}
