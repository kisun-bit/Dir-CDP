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

// walkers 实现了路径迭代器(比filepath.walk性能更好)
type Walker struct {
	Root       string
	Depth      int
	ThreadSize int
	semaphore  chan struct{}
	wg         *sync.WaitGroup
	pipe       chan nt_notify.FileWatchingObj
	stopNotify *int32
	pipClosed  *int32
	counter    *int32
}

func NewWalker(root string, maxDepth, maxThreadsNum int, pipe chan nt_notify.FileWatchingObj,
	counter *int32) (_ *Walker, err error) {
	if _, err = os.Stat(root); err != nil {
		return
	}

	w := new(Walker)
	w.Root, w.Depth, w.ThreadSize, w.pipe = root, maxDepth, maxThreadsNum, pipe
	w.semaphore = make(chan struct{}, w.ThreadSize)
	w.stopNotify = new(int32)
	w.pipClosed = new(int32)
	w.counter = counter
	w.wg = new(sync.WaitGroup)

	return w, nil
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
		return
	}
	atomic.StoreInt32(w.stopNotify, 1)
	for {
		if !w.continueResetWaitGroup() {
			break
		}
	}
}

func (w *Walker) reportFinished() {
	if w.counter != nil {
		atomic.AddInt32(w.counter, 1)
	}
}

func (w *Walker) continueResetWaitGroup() bool {
	defer func() {
		defer func() {
			if e := recover(); e != nil {
				atomic.StoreInt32(w.pipClosed, 1)
			}
		}()
	}()
	if atomic.LoadInt32(w.pipClosed) == 1 {
		return false
	}
	w.wg.Done()
	return true
}

func (w *Walker) Walk() (err error) {
	logger.Fmt.Infof("%v.Walk. called...", w.String())

	w.wg.Add(1)
	go w.walkDir(w.Root)

	go func() {
		defer func() {
			if e := recover(); e != nil {
				logger.Fmt.Warnf("%v.Walk PANIC=%v", w.String(), e)
			}
			w.reportFinished()
			atomic.StoreInt32(w.pipClosed, 1)
			logger.Fmt.Infof("%v.Walk exit...", w.String())
		}()
		w.wg.Wait()
	}()
	return nil
}

func (w *Walker) walkDir(dir string) {
	defer func() {
		if e := recover(); e != nil {
			runtime.Goexit()
		}
	}()
	defer w.wg.Done()

	depth := w.relativeDepth(dir)
	if !w.isDepthValid(depth) {
		return
	}

	if w.isChildDir(dir) {
		w.pipe <- nt_notify.FileWatchingObj{
			FileInfo: nt_notify.FileInfo{
				Time: time.Now().Unix(),
				Path: dir,
				Name: filepath.Base(dir),
				Type: meta.FTDir,
			},
			Event: meta.Win32EventFullScan,
		}
	}

	if w.isStopped() {
		logger.Fmt.Info("%v.walkDir【终止】", w.String())
		return
	}

	for _, entry := range w.dirListObjs(dir) {
		if w.isStopped() {
			logger.Fmt.Info("%v.walkDir【终止】", w.String())
			return
		}

		path_, err := filepath.Abs(path.Join(dir, entry.Name()))
		if err != nil {
			continue
		}
		if entry.Mode()&os.ModeSymlink != 0 {
			continue
		}
		if entry.IsDir() {
			w.wg.Add(1)
			go w.walkDir(path_)
			continue
		}
		w.pipe <- nt_notify.FileWatchingObj{
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
	}
}

func (w *Walker) isDepthValid(curDepth int) bool {
	if w.Depth == meta.UnsetInt {
		return true
	}
	return curDepth <= w.Depth
}

func (w *Walker) relativeDepth(dir string) int {
	tss := strings.Count(tools.CorrectDirWithPlatform(dir, meta.IsWin), meta.Sep)
	oss := strings.Count(tools.CorrectDirWithPlatform(w.Root, meta.IsWin), meta.Sep)
	return tss - oss
}

func (w *Walker) isChildDir(dir string) bool {
	return tools.CorrectDirWithPlatform(dir, meta.IsWin) != tools.CorrectDirWithPlatform(w.Root, meta.IsWin)
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
	return fmt.Sprintf("<walkers: Root(%s) LocalEnabledDepth(%d) Threads(%d)>", w.Root, w.Depth, w.ThreadSize)
}
