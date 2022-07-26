package store

// written by Aleksandr Fofanov (aleks-fofanov) on behalf of Speedscale

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

const (
	// JSONFileCacheType represents the storage type as file
	JSONFileCacheType = "jsonfilecache"
	// JSONFileCachePermission represents file permission
	JSONFileCachePermission = 0644
)

type FileIO interface {
	readFile() ([]byte, error)
	writeFile(content []byte) error
}

// JSONFileStore is a thread-unsafe implementation of cache storage in JSON file
type JSONFileStore struct {
	path    string
	fileIO  FileIO
	options *Options
}

func NewJSONFileStore(path string, options *Options) (StoreInterface, error) {
	if err := validateFile(path); err != nil {
		return nil, err
	}

	return &JSONFileStore{path: path, options: options, fileIO: newFileIO(path)}, nil
}

func (s *JSONFileStore) Get(ctx context.Context, key interface{}) (interface{}, error) {
	k, err := keyAsStr(key)
	if err != nil {
		return nil, err
	}

	val, exist, expriesAt, err := s.readKey(k)
	if err != nil {
		return nil, err
	} else if !exist || isPast(expriesAt) {
		return nil, fmt.Errorf("key %s is undefined", k)
	}

	return val, nil
}

func (s *JSONFileStore) GetWithTTL(ctx context.Context, key interface{}) (interface{}, time.Duration, error) {
	k, err := keyAsStr(key)
	if err != nil {
		return nil, time.Duration(0), err
	}

	val, exist, expiresAt, err := s.readKey(k)
	if err != nil {
		return nil, time.Duration(0), err
	} else if !exist || isPast(expiresAt) {
		return nil, time.Duration(0), fmt.Errorf("key %s is undefined", k)
	}

	return val, time.Duration(expiresAt-time.Now().Unix()) * time.Second, nil
}

func (s *JSONFileStore) Set(ctx context.Context, key any, value any, options ...Option) error {
	k, err := keyAsStr(key)
	if err != nil {
		return err
	}

	value, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("value type (%T) is not supported", value)
	}

	opts := applyOptions(options...)
	if opts == nil {
		opts = s.options
	}

	expiresAt := time.Now().Add(opts.expiration).Unix()
	err = s.writeKey(k, value, expiresAt, opts.tags)
	if err != nil {
		return err
	}

	return nil
}

func (s *JSONFileStore) Delete(ctx context.Context, key interface{}) error {
	k, err := keyAsStr(key)
	if err != nil {
		return err
	}

	return s.deleteKey(k, true)
}

func (s *JSONFileStore) Invalidate(ctx context.Context, options ...InvalidateOption) error {
	opts := applyInvalidateOptions(options...)
	if len(opts.tags) > 0 {
		for _, tag := range opts.tags {
			if err := s.invalidate(tag); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *JSONFileStore) Clear(context.Context) error {
	return os.Remove(s.path)
}

func (s *JSONFileStore) GetType() string {
	return JSONFileCacheType
}

func (s *JSONFileStore) writeKey(key string, value interface{}, expiresAt int64, tags []string) error {
	content, err := s.fileIO.readFile()
	if err != nil {
		return err
	}
	c, err := newJSONCacheFromJSON(content)
	if err != nil {
		return err
	}
	c.Set(key, value, expiresAt, tags)
	jsonString, err := c.ToJSON()
	if err != nil {
		return err
	}
	if err = s.fileIO.writeFile(jsonString); err != nil {
		return err
	}

	return nil
}

func (s *JSONFileStore) readKey(key string) (interface{}, bool, int64, error) {
	content, err := s.fileIO.readFile()
	if err != nil {
		return nil, false, 0, err
	}
	c, err := newJSONCacheFromJSON(content)
	if err != nil {
		return nil, false, 0, err
	}
	val, exist, expiresAt := c.Get(key)
	if exist && isPast(expiresAt) {
		// GC
		defer func() {
			_ = s.deleteKey(key, true)
		}()
	}

	return val, exist, expiresAt, nil
}

func (s *JSONFileStore) deleteKey(key string, silently bool) error {
	content, err := s.fileIO.readFile()
	if err != nil {
		return err
	}
	c, err := newJSONCacheFromJSON(content)
	if err != nil {
		return err
	}
	if exist := c.Delete(key); !exist && !silently {
		return fmt.Errorf("key %s doesn't exist", key)
	}
	jsonString, err := c.ToJSON()
	if err != nil {
		return err
	}
	if err = s.fileIO.writeFile(jsonString); err != nil {
		return err
	}

	return nil
}

func (s *JSONFileStore) invalidate(tag string) error {
	content, err := s.fileIO.readFile()
	if err != nil {
		return err
	}
	c, err := newJSONCacheFromJSON(content)
	if err != nil {
		return err
	}

	c.Invalidate(tag)
	jsonString, err := c.ToJSON()
	if err != nil {
		return err
	}
	if err = s.fileIO.writeFile(jsonString); err != nil {
		return err
	}

	return nil
}

type simpleFileIO struct {
	path string
}

func newFileIO(path string) *simpleFileIO {
	return &simpleFileIO{path: path}
}

func (f *simpleFileIO) readFile() ([]byte, error) {
	return ioutil.ReadFile(f.path)
}

func (f *simpleFileIO) writeFile(content []byte) error {
	return ioutil.WriteFile(f.path, content, JSONFileCachePermission)
}

type jsonCache struct {
	Tags        map[string]map[string]bool // Tag to keys map
	Expirations map[string]int64           // Keys expirations
	Items       map[string]interface{}     // Cache items
}

func newJSONCacheFromJSON(source []byte) (*jsonCache, error) {
	s := &jsonCache{
		map[string]map[string]bool{},
		map[string]int64{},
		map[string]interface{}{},
	}
	if len(source) > 0 {
		err := json.Unmarshal(source, s)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (s *jsonCache) Set(key string, val interface{}, expiresAt int64, tags []string) {
	s.Items[key] = val
	if expiresAt != 0 {
		s.Expirations[key] = expiresAt
	}
	s.SetTags(key, tags)
}

func (s *jsonCache) Get(key string) (interface{}, bool, int64) {
	val, valExist := s.Items[key]
	exp, expExist := s.Expirations[key]
	if !expExist {
		exp = 0
	}

	return val, valExist, exp
}

func (s *jsonCache) Delete(key string) bool {
	_, exist := s.Items[key]
	if exist {
		delete(s.Items, key)
	}

	_, exist = s.Expirations[key]
	if exist {
		delete(s.Expirations, key)
	}

	if len(s.Tags) > 0 {
		for tag, keys := range s.Tags {
			if _, exist = keys[key]; exist {
				delete(keys, key)
				s.Tags[tag] = keys
			}
		}
	}

	return exist
}

func (s *jsonCache) SetTags(key string, tags []string) {
	if _, exist := s.Items[key]; !exist || len(tags) == 0 {
		return
	}
	for _, tag := range tags {
		if items, exist := s.Tags[tag]; exist {
			items[key] = true
		} else {
			s.Tags[tag] = map[string]bool{key: true}
		}
	}
}

func (s *jsonCache) GetKeysForTag(tag string) []string {
	var res []string

	if _, exist := s.Tags[tag]; !exist {
		return res
	}
	for key := range s.Tags[tag] {
		res = append(res, key)
	}

	return res
}

func (s *jsonCache) Invalidate(tag string) {
	keys := s.GetKeysForTag(tag)
	if len(keys) == 0 {
		return
	}
	for _, key := range keys {
		s.Delete(key)
	}
}

func (s *jsonCache) ToJSON() ([]byte, error) {
	return json.Marshal(s)
}

func validateFile(path string) error {
	stat, err := os.Stat(path)
	if os.IsNotExist(err) {
		// Path doesn't exist, check if we have permissions to read and write
		if file, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR, JSONFileCachePermission); os.IsPermission(err) {
			return err
		} else {
			_ = file.Close()
		}
	} else if stat.IsDir() {
		return errors.New("provided path is a directory")
	} else {
		// Otherwise the path is a file and we need to
		// check if we have permissions to read and write it
		if file, err := os.OpenFile(filepath.Clean(path), os.O_RDWR, JSONFileCachePermission); os.IsPermission(err) {
			return err
		} else {
			_ = file.Close()
		}
	}

	return nil
}

func keyAsStr(key interface{}) (string, error) {
	var k string
	if _, ok := key.(string); !ok {
		return "", fmt.Errorf("key type (%T) is not supported", key)
	} else {
		k, _ = key.(string)
	}

	return k, nil
}

func isPast(epochTime int64) bool {
	if epochTime == 0 {
		return false
	}

	return epochTime < time.Now().Unix()
}
