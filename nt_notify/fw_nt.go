package nt_notify

import (
	"fmt"
	"io/ioutil"
	"jingrongshuan/rongan-fnotify/logging"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/tools"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

const (
	WatchingBufferSize = 1 << 20
	WatchingQueueSize  = 0
)

type Win32Watcher struct {
	depth          int
	root           string
	rootPtr        *uint16
	rootDescriptor syscall.Handle
	recursive      bool
	buffer         []byte
	notify         chan FileWatchingObj
	stopNotify     *int32
	closeOnce      *sync.Once
}

func NewWatcher(Dir string, IsRecursive bool, depth int) (w *Win32Watcher, err error) {
	if _, err = os.Stat(Dir); err != nil {
		return nil, err
	}

	w = new(Win32Watcher)
	w.depth = depth
	w.root = Dir
	w.recursive = IsRecursive
	w.buffer = make([]byte, WatchingBufferSize)
	w.notify = make(chan FileWatchingObj, WatchingQueueSize)
	w.stopNotify = new(int32)
	w.closeOnce = new(sync.Once)
	return w, err
}

func (w *Win32Watcher) isStopped() bool {
	return atomic.LoadInt32(w.stopNotify) == 1
}

func (w Win32Watcher) closeNotify() {
	w.closeOnce.Do(func() {
		close(w.notify)
	})
}

func (w *Win32Watcher) SetStop() {
	if w.isStopped() {
		return
	}

	logging.Logger.Fmt.Infof("%v.SetStop exit...", w.Str())
	atomic.StoreInt32(w.stopNotify, 1)
	w.closeNotify()

	// 产生一个临时变更事件，用于及时退出此线程
	fp, err := ioutil.TempFile(w.root, meta.IgnoreFlag)
	if err != nil {
		return
	}
	defer func() {
		fp.Close()
		_ = os.Remove(fp.Name())
	}()
	_, _ = fp.WriteString(meta.IgnoreFlag)
}

func (w *Win32Watcher) Start() (_ chan FileWatchingObj, err error) {
	if err = w.changeDirAccessRight(); err != nil {
		return
	}

	if err = w.asyncLoop(); err != nil {
		return
	}
	return w.notify, nil
}

func (w *Win32Watcher) changeDirAccessRight() (err error) {
	if w.rootPtr, err = syscall.UTF16PtrFromString(w.root); err != nil {
		return err
	}

	w.rootDescriptor, err = syscall.CreateFile(w.rootPtr,
		syscall.FILE_LIST_DIRECTORY,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil,
		syscall.OPEN_EXISTING,
		syscall.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return err
	}
	return nil
}

func (w *Win32Watcher) asyncLoop() (err error) {
	go func() {
		defer func() {
			logging.Logger.Fmt.Infof("%v.asyncLoop【终止】", w.Str())
		}()

		defer syscall.CloseHandle(w.rootDescriptor)
		//defer w.closeNotify()

		for {
			if w.isStopped() {
				return
			}
			if err = w.readDirectoryChanges(); err != nil {
				break
			}
			w.collectChangeInfo()
		}
	}()
	return
}

func (w *Win32Watcher) readDirectoryChanges() (err error) {
	return syscall.ReadDirectoryChanges(
		w.rootDescriptor,
		&w.buffer[0],
		WatchingBufferSize,
		w.recursive,
		syscall.FILE_NOTIFY_CHANGE_FILE_NAME|syscall.FILE_NOTIFY_CHANGE_DIR_NAME|syscall.FILE_NOTIFY_CHANGE_ATTRIBUTES|syscall.FILE_NOTIFY_CHANGE_SIZE|syscall.FILE_NOTIFY_CHANGE_LAST_WRITE|syscall.FILE_NOTIFY_CHANGE_CREATION,
		nil,
		&syscall.Overlapped{},
		0,
	)
}

func (w *Win32Watcher) collectChangeInfo() {
	defer func() {
		recover() // send to notify channel
	}()

	if w.isStopped() {
		return
	}

	var offset uint32
	for {
		raw := (*syscall.FileNotifyInformation)(unsafe.Pointer(&w.buffer[offset]))
		buf := (*[syscall.MAX_PATH]uint16)(unsafe.Pointer(&raw.FileName))
		name := syscall.UTF16ToString(buf[:raw.FileNameLength/2])
		path := filepath.Join(w.root, name)

		info := new(FileWatchingObj)
		info.Path = path
		info.Event = tools.ConvertToWin32Event(raw.Action)
		info.Name = filepath.Base(info.Path)
		if meta.AppIsDebugMode {
			logging.Logger.Fmt.Debugf(
				"%v事件(CAP:%v) >>>>>>>>>> (%v) `%v`", info.Event.Str(), cap(w.buffer), info.Name, info.Path)
		}

		if !w.isValidDepth(info.Path) {
			break
		}

		if w.NeedDelete(info.Event) {
			// TODO 若删除的是目录需要手动递归删除
		} else {
			fi, err := os.Stat(path)
			if err != nil {
				break
			}
			info.Type = tools.ConvertMode2FType(fi.Mode())
			if fi.Mode()&os.ModeSymlink != 0 {
				// TODO 链接文件备份
				break
			} else {
				info.Time = fi.ModTime().Unix()
				info.Mode = fi.Mode()
				info.Size = fi.Size()
			}
		}

		if w.isStopped() {
			return
		}
		w.notify <- *info

		if raw.NextEntryOffset == 0 {
			break
		}
		offset += raw.NextEntryOffset
		if offset >= WatchingBufferSize {
			break
		}
	}
}

func (w *Win32Watcher) isValidDepth(path string) bool {
	if w.depth == -1 {
		return true
	}
	tss := strings.Count(tools.CorrectPathWithPlatform(path, meta.IsWin), meta.Sep)
	oss := strings.Count(tools.CorrectDirWithPlatform(w.root, meta.IsWin), meta.Sep)
	return tss-oss <= w.depth
}

func (w *Win32Watcher) NeedDelete(event meta.Event) bool {
	return event == meta.Win32EventDelete || event == meta.Win32EventRenameFrom
}

func (w *Win32Watcher) Str() string {
	return fmt.Sprintf("<Win32Watcher(dir=%v)>", w.root)
}
