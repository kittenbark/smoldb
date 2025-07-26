package smoldb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/kittenbark/smoldb/ysmol"
	"log/slog"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	StatsRead  = atomic.Int64{}
	StatsWrite = atomic.Int64{}
)

const flags = os.O_EXCL | os.O_SYNC

func New[Key comparable, Value any](filename string) (*Smol[Key, Value], error) {
	smol := &Smol[Key, Value]{
		filename: filename,
		data:     make(map[Key]Value),
	}
	return smol, smol.Load()
}

type Smol[Key comparable, Value any] struct {
	filename     string
	filenameTemp string
	mutex        sync.RWMutex
	data         map[Key]Value
	meta         map[Key]metadata
	numeric      *bool
	ttl          time.Duration
	synced       time.Time
	id           atomic.Int64
	cleans       atomic.Int64
}

type metadata struct {
	id int64
}

func (smol *Smol[Key, Value]) TryGet(key Key) (result Value, ok bool, err error) {
	smol.mutex.Lock()
	defer smol.mutex.Unlock()

	if err = smol.unsyncLoad(); err != nil {
		return result, false, err
	}

	val, ok := smol.data[key]
	if !ok {
		return result, false, nil
	}
	return val, true, nil
}

func (smol *Smol[Key, Value]) Get(key Key) (result Value, err error) {
	result, ok, err := smol.TryGet(key)
	if err != nil {
		return result, err
	}
	if !ok {
		return result, notFound
	}
	return result, nil
}

func (smol *Smol[Key, Value]) Set(key Key, val Value) (err error) {
	smol.mutex.Lock()
	defer smol.mutex.Unlock()

	_, rewritten := smol.data[key]
	smol.data[key] = val
	if smol.ttl != 0 {
		id := smol.id.Add(1)
		smol.meta[key] = metadata{id: id}
		time.AfterFunc(smol.ttl, smol.scheduledDelete(key, id))
	}
	if rewritten || len(smol.data) == 1 {
		return smol.unsyncSave()
	}
	return smol.appendSave(key, val)
}

func (smol *Smol[Key, Value]) Del(key Key) (err error) {
	smol.mutex.Lock()
	defer smol.mutex.Unlock()
	if _, ok := smol.data[key]; ok {
		delete(smol.data, key)
		delete(smol.data, key)
		return smol.cleanSave(key)
	}
	return nil
}

func (smol *Smol[Key, Value]) Load() (err error) {
	smol.mutex.Lock()
	defer smol.mutex.Unlock()
	return smol.unsyncLoad()
}

func (smol *Smol[Key, Value]) Save() (err error) {
	smol.mutex.Lock()
	defer smol.mutex.Unlock()
	return smol.unsyncSave()
}

func (smol *Smol[Key, Value]) Keys() []Key {
	smol.mutex.RLock()
	defer smol.mutex.RUnlock()
	keys := make([]Key, 0, len(smol.data))
	for k := range smol.data {
		keys = append(keys, k)
	}
	return keys
}

func (smol *Smol[Key, Value]) Ttl(ttl time.Duration) *Smol[Key, Value] {
	smol.mutex.Lock()
	defer smol.mutex.Unlock()
	smol.ttl = ttl
	return smol
}

func (smol *Smol[Key, Value]) Size() int {
	smol.mutex.RLock()
	defer smol.mutex.RUnlock()
	return len(smol.data)
}

func (smol *Smol[Key, Value]) unsyncLoad() (err error) {
	if smol.data == nil {
		smol.data = make(map[Key]Value)
	}
	if smol.meta == nil {
		smol.meta = make(map[Key]metadata)
	}

	stat, err := os.Stat(smol.filename)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if os.IsNotExist(err) {
		if err = smol.unsyncSave(); err != nil {
			return err
		}
	}
	if stat != nil && !smol.synced.IsZero() && stat.ModTime().After(smol.synced) {
		return nil
	}

	file, err := os.OpenFile(smol.filename, os.O_RDONLY|flags, os.ModePerm)
	if err != nil {
		return
	}
	defer func() { err = errors.Join(err, file.Close()) }()

	StatsRead.Add(1)
	smol.synced = time.Now()
	if err = ysmol.NewDecoder(file).Decode(&smol.data); err != nil {
		return
	}
	return
}

func (smol *Smol[Key, Value]) unsyncSave() (err error) {
	defer smol.cleans.Store(0)
	file, err := os.OpenFile(smol.filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_SYNC, os.ModePerm)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, file.Close(), os.Remove(file.Name()))
		}
	}()

	StatsWrite.Add(1)
	if err = ysmol.NewEncoder(file).Encode(smol.data); err != nil {
		return
	}
	smol.synced = time.Now()
	return
}

func (smol *Smol[Key, Value]) scheduledDelete(key Key, id int64) func() {
	return func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Warn("smoldb: unexpected panic", "what", r, "stacktrace", string(debug.Stack()))
			}
		}()

		smol.mutex.Lock()
		defer smol.mutex.Unlock()
		if meta, ok := smol.meta[key]; !ok || meta.id != id {
			return
		}
		delete(smol.data, key)
		delete(smol.meta, key)
		_ = smol.cleanSave(key)
	}
}

func (smol *Smol[Key, Value]) cleanSave(key Key) error {
	if smol.cleans.Load()*10 > int64(len(smol.data)) {
		return smol.unsyncSave()
	}
	defer smol.cleans.Add(1)
	file, err := os.OpenFile(smol.filename, os.O_RDWR|flags, os.ModePerm)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()

	if smol.numeric == nil {
		smol.numeric = new(bool)
		kind := reflect.ValueOf(key).Type().Kind()
		*smol.numeric = reflect.Int <= kind && kind <= reflect.Float64
	}

	buff := bytes.NewBuffer(nil)
	prefix := ysmol.NewEncoder(buff).EncodeString(fmt.Sprint(key), 0, *smol.numeric).String() + ":"
	scanner := bufio.NewScanner(file)
	from, to := int64(0), int64(-1)
	for scanner.Scan() {
		line := scanner.Text()
		found := false
		switch {
		case to > -1 && strings.IndexAny(line, " \t\r\n") > 0:
			found = true
			break
		case to > -1:
			to += int64(len(line)) + 1
		case strings.HasPrefix(line, prefix):
			to = from + int64(len(line)) + 1
		default:
			from += int64(len(line)) + 1
		}

		if found {
			break
		}
	}
	if _, err := file.Seek(from, 0); err != nil {
		return err
	}
	filler := make([]byte, to-from-1)
	for i := range filler {
		filler[i] = '\n'
	}
	if _, err := file.Write(filler); err != nil {
		return err
	}
	return nil
}

func (smol *Smol[Key, Value]) appendSave(key Key, val Value) (err error) {
	data, err := ysmol.Marshal(map[Key]Value{key: val})
	if err != nil {
		return err
	}
	data = append(data, '\n')
	file, err := os.OpenFile(smol.filename, os.O_APPEND|os.O_WRONLY|flags, os.ModePerm)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	if _, err := file.Write(data); err != nil {
		return err
	}
	return nil
}

var notFound = errors.New("smoldb: not found")

func NotFound(err error) bool {
	return errors.Is(err, notFound)
}
