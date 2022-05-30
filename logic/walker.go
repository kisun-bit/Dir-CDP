package logic

import (
	"fmt"
	"io/ioutil"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/nt_notify"
	"jingrongshuan/rongan-fnotify/tools"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// walker 实现了路径迭代器(比filepath.walk性能更好)
type Walker struct {
	Root        string
	Depth       int
	ThreadSize  int
	CurrentTime time.Time
	ValidDays   int
	Grep        Grep
	semaphore   chan struct{}
	wg          *sync.WaitGroup
	pipe        chan nt_notify.FileWatchingObj
	stopNotify  *int32
	pipClosed   *int32
}

func NewWalker(
	root string, maxDepth, maxThreadsNum int, Include, Exclude string, now time.Time, validDays int) *Walker {
	w := new(Walker)

	w.Root, w.Depth, w.ThreadSize = root, maxDepth, maxThreadsNum
	w.pipe = make(chan nt_notify.FileWatchingObj, 10)
	w.semaphore = make(chan struct{}, w.ThreadSize)
	w.Grep = *NewGrep(Include, Exclude)
	w.CurrentTime = now
	w.ValidDays = validDays
	w.stopNotify = new(int32)
	w.pipClosed = new(int32)
	w.wg = new(sync.WaitGroup)
	logger.Fmt.Infof("NewWalker -> %v", w.String())
	return w
}

func (w *Walker) isStopped() bool {
	return atomic.LoadInt32(w.stopNotify) == 1
}

func (w *Walker) SetStop() {
	defer func() {
		if e := recover(); e != nil {
			logger.Fmt.Warnf("%v.SetStop PANIC=%v", w.String(), e)
		}
	}()
	if w.isStopped() {
		logger.Fmt.Infof("%v.SetStop 已禁用路径枚举器", w.String())
		return
	}
	logger.Fmt.Infof("%v.SetStop 正在关闭路径枚举器...", w.String())
	atomic.StoreInt32(w.stopNotify, 1)
	for {
		defer func() {
			defer func() {
				if e := recover(); e != nil {
					atomic.StoreInt32(w.pipClosed, 1)
					//logger.Fmt.Warnf("%v.SetStop For PANIC=%v", w.String(), e)
				}
			}()
		}()
		if atomic.LoadInt32(w.pipClosed) == 1 {
			break
		}
		w.wg.Done()
	}
}

func (w *Walker) Walk() (pq chan nt_notify.FileWatchingObj, err error) {
	logger.Info("walker Walk. start")

	w.wg.Add(1)
	go w.walkDir(w.Root)

	go func() {
		defer func() {
			if e := recover(); e != nil {
				logger.Fmt.Warnf("%v.Walk PANIC=%v", w.String(), e)
			}
			close(w.pipe) // 关闭通道不会影响从通道读数据
			atomic.StoreInt32(w.pipClosed, 1)
			logger.Fmt.Infof("%v.Walk 已关闭路径枚举器的枚举通道...", w.String())
		}()
		w.wg.Wait()
	}()
	return w.pipe, nil
}

func (w *Walker) walkDir(dir string) {
	defer func() {
		if e := recover(); e != nil {
			//logger.Fmt.Warnf("%v.walkDir recover PANIC=%v. will exit current goroutine", w.String(), e)
			runtime.Goexit()
		}
	}()
	defer w.wg.Done()

	// 记录目录信息
	w.pipe <- nt_notify.FileWatchingObj{
		FileInfo: nt_notify.FileInfo{
			Time: time.Now().Unix(),
			Path: dir,
			Name: filepath.Base(dir),
			Type: meta.FTDir,
		},
		Event: meta.Win32EventFullScan,
	}

	if w.isStopped() {
		logger.Fmt.Info("%v.walkDir【终止】", w.String())
		return
	}

	if w.Depth != -1 && strings.Count(dir, meta.Sep) > w.Depth {
		logger.Fmt.Warnf("walker walkDir. limit depth %d. ignore to walk %s", w.Depth, dir)
		return
	}

	for _, entry := range w.dirListObjs(dir) {
		if w.isStopped() {
			logger.Fmt.Info("%v.walkDir【终止】", w.String())
			return
		}
		path_, err := filepath.Abs(path.Join(dir, entry.Name()))
		if err != nil {
			logger.Fmt.Errorf("walker walkDir. failed to get abs-path from %s", path_)
			continue
		}
		// ignore link file
		if entry.Mode()&os.ModeSymlink != 0 {
			logger.Fmt.Info("walker walkDir. ignore to backup link file ", path_)
			continue
		}
		if entry.IsDir() {
			w.wg.Add(1)
			go w.walkDir(path_)
		} else {
			if w.Grep.IsValidByGrep(path_) {
				if w.ValidDays != -1 {
					if entry.ModTime().Add(time.Hour * 24 * time.Duration(w.ValidDays)).Before(w.CurrentTime) {
						logger.Fmt.Warnf("walker walkDir. ignore to backup `%s` due to modify time", path_)
						continue
					}
				}
				fwo := nt_notify.FileWatchingObj{
					FileInfo: nt_notify.FileInfo{
						Time: entry.ModTime().Unix(),
						Path: path_,
						Name: filepath.Base(path_),
						Type: tools.ConvertMode2FType(entry.Mode()),
						Mode: entry.Mode(),
						Size: entry.Size(),
					},
					Event: meta.Win32EventFullScan,
				}
				w.pipe <- fwo
			}
		}
	}
}

func (w *Walker) dirListObjs(dir string) []os.FileInfo {
	w.semaphore <- struct{}{}
	defer func() { <-w.semaphore }()
	entries, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil
	}
	return entries
}

func (w *Walker) String() string {
	return fmt.Sprintf("<walker: Root(%s) Depth(%d) Threads(%d) Include(%v) Exclude(%v)>",
		w.Root,
		w.Depth,
		w.ThreadSize,
		w.Grep.Include,
		w.Grep.Exclude)
}
