package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"git.yujing.live/Golang/source/configmanager"
	"git.yujing.live/Golang/source/log"
	mredis "git.yujing.live/Golang/source/pkg/database/redis"
	"git.yujing.live/Golang/source/pkg/utils/snowflake"
	"github.com/lesismal/arpc/util"
	"gorm.io/gorm"
)

const (
	ActionSet       = "set"        // 操作类型：设置缓存
	ActionDelete    = "delete"     // 操作类型：删除缓存
	ActionUpdate    = "update"     // 操作类型：更新缓存
	ActionUpdateAll = "update_all" // 操作类型：更新缓存
)

type MCacheEventType string

const (
	MCacheEventCreate MCacheEventType = "create"
	MCacheTypeUpdate  MCacheEventType = "update"
	MCacheEventDelete MCacheEventType = "delete"
)

// LoadByKey 定义了一个数据加载函数的类型
type LoadByKey[TKey comparable, TValue any] func(ctx context.Context, key TKey) (*TValue, error)

// LoadAll 定义了一个数据加载函数的类型
type LoadAll[TKey comparable, TValue any] func(ctx context.Context) (map[TKey]*TValue, error)

// OnEvent
type OnEvent[TKey comparable, TValue any] func(event MCacheEventType, key TKey, value ...*TValue)

// Message 表示 Redis Pub/Sub 消息的 JSON 格式结构
type Message[TKey comparable, TValue any] struct {
	NodeID string  `json:"node_id"`         // 节点的唯一标识符
	Action string  `json:"action"`          // 操作类型："set"、"delete" 或 "update"
	Key    TKey    `json:"key"`             // 缓存的键
	Keys   []TKey  `json:"keys"`            // 缓存的键列表
	Value  *TValue `json:"value,omitempty"` // 缓存的值
}

// MCache 是一个通用的缓存结构，支持数据加载和缓存管理
type MCache[TKey comparable, TValue any] struct {
	table     string
	rds       *mredis.Client          // Redis 客户端
	db        *gorm.DB                // db 客户端
	data      sync.Map                // 缓存数据存储
	nodeID    string                  // 当前节点的唯一标识符
	channel   string                  // Redis Pub/Sub 频道
	loadByKey LoadByKey[TKey, TValue] // 数据加载函数
	loadAll   LoadAll[TKey, TValue]   // 数据加载函数
	onEvent   OnEvent[TKey, TValue]   // 数据变更事件
	err       error
	keyName   string
	keyDBName string
}

// NewMCache 创建或返回现有的 mcache 实例
// 根据给定的频道代码创建或返回一个已存在的 mcache 实例
func NewMCache[TKey comparable, TValue any](dbCli *gorm.DB, rdsCli *mredis.Client, table string) *MCache[TKey, TValue] {
	cache := &MCache[TKey, TValue]{
		db:    dbCli,
		rds:   rdsCli,
		table: table,
	}
	return cache
}

// WithLoadByKey .
func (m *MCache[TKey, TValue]) WithLoadByKey(loadBykey LoadByKey[TKey, TValue]) *MCache[TKey, TValue] {
	m.loadByKey = loadBykey
	return m
}

// WithLoadAll .
func (m *MCache[TKey, TValue]) WithLoadAll(loadAll LoadAll[TKey, TValue]) *MCache[TKey, TValue] {
	m.loadAll = loadAll
	return m
}

// loadByKeyOfGorm .
func (m *MCache[TKey, TValue]) loadByKeyOfGorm(_ context.Context, key TKey) (*TValue, error) {
	var t TValue
	err := m.db.Where(fmt.Sprintf("%s=?", m.keyDBName), key).First(&t).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &t, err
}

// loadAllOfGorm .
func (m *MCache[TKey, TValue]) loadAllOfGorm(_ context.Context) (map[TKey]*TValue, error) {
	var list []*TValue
	err := m.db.Find(&list).Error
	if err != nil {
		return nil, err
	}
	var result = make(map[TKey]*TValue)
	for _, item := range list {
		val := reflect.ValueOf(item)
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
		}
		idField := val.FieldByName(m.keyName)
		result[idField.Interface().(TKey)] = item
	}
	return result, err
}

// WithGorm .
func (m *MCache[TKey, TValue]) WithGorm(keyName, keyDBName string) *MCache[TKey, TValue] {
	var t TValue
	stmt := m.db.Model(&t).Statement
	err := stmt.Parse(&t)
	if err != nil {
		m.err = err
		return m
	}
	m.table = stmt.Schema.Table
	if len(stmt.Schema.PrimaryFields) > 0 {
		m.keyName = stmt.Schema.PrimaryFields[0].Name
		m.keyDBName = stmt.Schema.PrimaryFields[0].DBName
	}
	if keyName != "" {
		m.keyName = keyName
	}
	if keyDBName != "" {
		m.keyDBName = keyDBName
	}
	if m.table == "" {
		m.err = fmt.Errorf("get table name fail")
		return m
	}
	if m.keyName == "" || m.keyDBName == "" {
		m.err = fmt.Errorf("mcache '%s' get table primary key fail", m.table)
		return m
	}
	val := reflect.ValueOf(t)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	idField := val.FieldByName(m.keyName)
	if !idField.IsValid() {
		m.err = fmt.Errorf("mcache '%s' key '%s' name fail", m.table, m.keyName)
		return m
	}
	if _, ok := idField.Interface().(TKey); !ok {
		m.err = fmt.Errorf("mcache '%s' key '%s' type fail", m.table, m.keyName)
		return m
	}
	m.loadByKey = m.loadByKeyOfGorm
	m.loadAll = m.loadAllOfGorm
	return m
}

// OnEvent .
func (m *MCache[TKey, TValue]) WithOnEvent(onEvent OnEvent[TKey, TValue]) *MCache[TKey, TValue] {
	m.onEvent = onEvent
	return m
}

func (m *MCache[TKey, TValue]) Open() (*MCache[TKey, TValue], error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.loadByKey == nil {
		return nil, fmt.Errorf("mcache '%s' loadByKey not config", m.table)
	}
	channel := fmt.Sprintf("%s:%s:mcache:%s", configmanager.GetString("app", "platform"), configmanager.GetString("service.metadata.tenant_name", "platform"), m.table)
	hostId := configmanager.GetInt64("hostid", -1)
	instanceId := configmanager.GetInt64("service.instanceid", 0)
	snow, err := snowflake.NewWithHostInstance(hostId, instanceId)
	if err != nil {
		return m, err
	}
	m.channel = channel
	m.nodeID = strconv.FormatInt(snow.Gen().Int64(), 10)
	go m.listenForUpdates()
	return m, nil
}

// Set 更新本地缓存并发布缓存更新通知到 Redis
func (m *MCache[TKey, TValue]) Set(ctx context.Context, key TKey, value *TValue) error {
	m.setData(key, value)

	msg := Message[TKey, TValue]{
		NodeID: m.nodeID,
		Action: ActionSet,
		Key:    key,
		Value:  value,
	}
	msgJSON, err := json.Marshal(msg)
	if err != nil {
		log.L().Errorc(ctx, "[mcache] %s Serialization of set '%s' message failed: %v", m.table, key, err)
		return err
	}
	if err := m.rds.Client.Publish(ctx, m.channel, msgJSON).Err(); err != nil {
		log.L().Errorc(ctx, "[mcache] %s Publishing set '%s' message failed: %v", m.table, key, err)
		return err
	}

	log.L().Debugc(ctx, "[mcache] %s Set Key '%s'", m.table, key)
	return nil
}

// Delete 更新本地缓存并发布缓存删除通知到 Redis
func (m *MCache[TKey, TValue]) Delete(ctx context.Context, key TKey) error {
	m.delData(key)

	msg := Message[TKey, TValue]{
		NodeID: m.nodeID,
		Action: ActionDelete,
		Key:    key,
	}
	msgJSON, err := json.Marshal(msg)
	if err != nil {
		log.L().Errorc(ctx, "[mcache] %s Serialization of delete '%s' message failed: %v", m.table, key, err)
		return err
	}
	if err := m.rds.Client.Publish(ctx, m.channel, msgJSON).Err(); err != nil {
		log.L().Errorc(ctx, "[mcache] %s Publishing delete '%s' message failed: %v", m.table, key, err)
		return err
	}

	log.L().Debugc(ctx, "[mcache] %s Delete Key '%s'", m.table, key)
	return nil
}

func (m *MCache[TKey, TValue]) DeleteAsync(ctx context.Context, key TKey) {
	go util.Safe(func() {
		m.Delete(ctx, key)
	})
}

func (m *MCache[TKey, TValue]) Deletes(ctx context.Context, keys ...TKey) error {
	if len(keys) <= 0 {
		return nil
	}
	for i := range keys {
		m.delData(keys[i])
	}
	msg := Message[TKey, TValue]{
		NodeID: m.nodeID,
		Action: ActionDelete,
		Keys:   keys,
	}
	msgJSON, err := json.Marshal(msg)
	if err != nil {
		log.L().Errorc(ctx, "[mcache] %s Serialization of deletes keys '%v' message failed: %v", m.table, keys, err)
		return err
	}
	if err := m.rds.Client.Publish(ctx, m.channel, msgJSON).Err(); err != nil {
		log.L().Errorc(ctx, "[mcache] %s Publishing deletes keys '%v' message failed: %v", m.table, keys, err)
		return err
	}
	log.L().Debugc(ctx, "[mcache] %s Deletes keys '%v'", m.table, keys)
	return nil
}

func (m *MCache[TKey, TValue]) DeletesAsync(ctx context.Context, keys ...TKey) {
	go util.Safe(func() {
		m.Deletes(ctx, keys...)
	})
}

// Update 重新加载数据到缓存并发布更新通知到 Redis
func (m *MCache[TKey, TValue]) Update(ctx context.Context, key TKey) error {
	if _, err := m.Load(ctx, key); err != nil {
		return err
	}

	msg := Message[TKey, TValue]{
		NodeID: m.nodeID,
		Action: ActionUpdate,
		Key:    key,
	}
	msgJSON, err := json.Marshal(msg)
	if err != nil {
		log.L().Errorc(ctx, "[mcache] %s Serialization of update '%s' message failed: %v", m.table, key, err)
		return err
	}
	if err := m.rds.Client.Publish(ctx, m.channel, msgJSON).Err(); err != nil {
		log.L().Errorc(ctx, "[mcache] %s Publishing update '%s' message failed: %v", m.table, key, err)
		return err
	}

	log.L().Debugc(ctx, "[mcache] %s Update Key '%s'", m.table, key)
	return nil
}

func (m *MCache[TKey, TValue]) UpdateAsync(ctx context.Context, key TKey) {
	go util.Safe(func() {
		m.Update(ctx, key)
	})
}

func (m *MCache[TKey, TValue]) Updates(ctx context.Context, keys ...TKey) error {
	if len(keys) <= 0 {
		return nil
	}
	var ret error
	for i := range keys {
		_, err := m.Load(ctx, keys[i])
		if err != nil {
			ret = err
		}
	}
	msg := Message[TKey, TValue]{
		NodeID: m.nodeID,
		Action: ActionUpdate,
		Keys:   keys,
	}
	msgJSON, err := json.Marshal(msg)
	if err != nil {
		log.L().Errorc(ctx, "[mcache] %s Serialization of updates keys '%v' message failed: %v", m.table, keys, err)
		return err
	}
	if err := m.rds.Client.Publish(ctx, m.channel, msgJSON).Err(); err != nil {
		log.L().Errorc(ctx, "[mcache] %s Publishing updates keys '%v' message failed: %v", m.table, keys, err)
		return err
	}
	log.L().Debugc(ctx, "[mcache] %s Updates keys '%v'", m.table, keys)
	return ret
}

func (m *MCache[TKey, TValue]) UpdatesAsync(ctx context.Context, keys ...TKey) {
	go util.Safe(func() {
		m.Updates(ctx, keys...)
	})
}

func (m *MCache[TKey, TValue]) UpdateAll(ctx context.Context) error {
	if err := m.LoadAll(ctx); err != nil {
		return err
	}
	msg := Message[TKey, TValue]{
		NodeID: m.nodeID,
		Action: ActionUpdateAll,
	}
	msgJSON, err := json.Marshal(msg)
	if err != nil {
		log.L().Errorc(ctx, "[mcache] %s Serialization of update all message failed: %v", m.table, err)
		return err
	}
	if err := m.rds.Client.Publish(ctx, m.channel, msgJSON).Err(); err != nil {
		log.L().Errorc(ctx, "[mcache] %s Publishing update all message failed: %v", m.table, err)
		return err
	}
	log.L().Debugc(ctx, "[mcache] %s Update all ", m.table)
	return nil
}

func (m *MCache[TKey, TValue]) UpdateAllAsync(ctx context.Context) {
	go util.Safe(func() {
		m.UpdateAll(ctx)
	})
}

// LoadAll
func (m *MCache[TKey, TValue]) LoadAll(ctx context.Context) error {
	if m.loadAll == nil {
		return fmt.Errorf("mcache '%s' loadAll not config", m.table)
	}
	dict, err := m.loadAll(ctx)
	if err != nil {
		log.L().Errorc(ctx, "[mcache] %s loadd all failed: %v", m.table, err)
		return err
	}
	m.data.Range(func(key, value interface{}) bool {
		m.delData(key.(TKey))
		return true
	})
	for k, item := range dict {
		m.setData(k, item)
	}
	log.L().Debugc(ctx, "[mcache] %s LoadAll", m.table)
	return nil
}

func (m *MCache[TKey, TValue]) LoadAllAsync(ctx context.Context) {
	go util.Safe(func() {
		m.loadAll(ctx)
	})
}

func (m *MCache[TKey, TValue]) Load(ctx context.Context, key TKey) (*TValue, error) {
	// 缓存中不存在，使用数据加载函数加载数据
	value, err := m.loadByKey(ctx, key)
	if err != nil {
		log.L().Errorc(ctx, "[mcache] %s loadData failed: %v", m.table, err)
		return nil, err
	}

	// 加载的数据存入缓存
	m.setData(key, value)

	log.L().Debugc(ctx, "[mcache] %s load Key '%s'", m.table, key)
	return value, nil
}

func (m *MCache[TKey, TValue]) LoadAsync(ctx context.Context, key TKey) {
	go util.Safe(func() {
		m.Load(ctx, key)
	})
}

// Get 从缓存中获取数据，如果缓存中不存在，则通过数据加载函数加载数据
func (m *MCache[TKey, TValue]) Get(ctx context.Context, key TKey) (*TValue, error) {
	if value, ok := m.data.Load(key); ok {
		return value.(*TValue), nil
	}
	return m.Load(ctx, key)
}

// GetCache 从缓存中获取数据
func (m *MCache[TKey, TValue]) GetCache(ctx context.Context, key TKey) *TValue {
	if value, ok := m.data.Load(key); ok {
		return value.(*TValue)
	}
	return nil
}

func (m *MCache[TKey, TValue]) setData(key TKey, value *TValue) {
	oldVal, loaded := m.data.Swap(key, value)
	if m.onEvent != nil {
		if loaded {
			m.onEvent(MCacheTypeUpdate, key, value, oldVal.(*TValue))
		} else {
			m.onEvent(MCacheEventCreate, key, value)
		}
	}
}

func (m *MCache[TKey, TValue]) delData(key TKey) {
	val, loaded := m.data.LoadAndDelete(key)
	if !loaded {
		return
	}
	if m.onEvent != nil {
		m.onEvent(MCacheEventDelete, key, val.(*TValue))
	}
}

// Find 根据条件过滤列表
func (m *MCache[TKey, TValue]) Find(ctx context.Context, filter func(key TKey, value *TValue) bool) []*TValue {
	var list []*TValue
	m.data.Range(func(key, value any) bool {
		if value == nil {
			return true
		}
		if filter(key.(TKey), value.(*TValue)) {
			list = append(list, value.(*TValue))
		}
		return true
	})
	return list
}

// FindAll
func (m *MCache[TKey, TValue]) FindAll(ctx context.Context) []*TValue {
	var list []*TValue
	m.data.Range(func(key, value any) bool {
		if value == nil {
			return true
		}
		list = append(list, value.(*TValue))
		return true
	})
	return list
}

// Range 根据条件过滤列表
func (m *MCache[TKey, TValue]) Range(f func(key TKey, value *TValue) bool) {
	m.data.Range(func(key, value any) bool {
		if value == nil {
			return true
		}
		return f(key.(TKey), value.(*TValue))
	})
}

// First 根据条件过滤列表
func (m *MCache[TKey, TValue]) First(ctx context.Context, filter func(key TKey, value *TValue) bool) *TValue {
	var ret *TValue
	m.data.Range(func(key, value any) bool {
		if value == nil {
			return true
		}
		if filter(key.(TKey), value.(*TValue)) {
			ret = value.(*TValue)
			return false
		}
		return true
	})
	return ret
}

// Last 根据条件过滤列表
func (m *MCache[TKey, TValue]) Last(ctx context.Context, filter func(key TKey, value *TValue) bool) *TValue {
	var ret *TValue
	m.data.Range(func(key, value any) bool {
		if value == nil {
			return true
		}
		if filter(key.(TKey), value.(*TValue)) {
			ret = value.(*TValue)
		}
		return true
	})
	return ret
}

// listenForUpdates 监听 Redis 消息频道并处理缓存更新通知
func (m *MCache[TKey, TValue]) listenForUpdates() {
	log.L().Debugc(context.Background(), "[mcache] %s Subscribing to channel '%s'", m.table, m.channel)
	sub := m.rds.Subscribe(context.Background(), m.channel)
	for msg := range sub.Channel() {
		log.L().Debugc(context.Background(), "[mcache] %s Received message '%s' from channel '%s'", m.table, msg.Payload, msg.Channel)
		if err := m.handleMessage(msg.Payload); err != nil {
			log.L().Errorc(context.Background(), "[mcache] %s Message handling failed: %v", m.table, err)
		}
	}
}

// handleMessage 处理来自 Redis 的缓存更新或删除消息
func (m *MCache[TKey, TValue]) handleMessage(message string) error {
	var msg Message[TKey, TValue]
	ctx := context.Background()
	if err := json.Unmarshal([]byte(message), &msg); err != nil {
		log.L().Errorc(ctx, "[mcache] %s Failed to parse message: %v", m.table, err)
		return err
	}

	if msg.NodeID == m.nodeID {
		return nil
	}

	switch msg.Action {
	case ActionSet:
		m.setData(msg.Key, msg.Value)
	case ActionDelete:
		if len(msg.Keys) > 0 {
			for i := range msg.Keys {
				m.delData(msg.Keys[i])
			}
		} else {
			m.delData(msg.Key)
		}
	case ActionUpdate:
		if len(msg.Keys) > 0 {
			var ret error
			for i := range msg.Keys {
				if _, err := m.Load(ctx, msg.Keys[i]); err != nil {
					ret = err
				}
			}
			if ret != nil {
				return ret
			}
		} else {
			if _, err := m.Load(ctx, msg.Key); err != nil {
				return err
			}
		}
	case ActionUpdateAll:
		if err := m.LoadAll(ctx); err != nil {
			return err
		}
	default:
		err := fmt.Errorf("unknown action: %s", msg.Action)
		log.L().Errorc(ctx, "[mcache] %s %v", m.table, err)
		return err
	}

	return nil
}

// Close 关闭 Redis 客户端和订阅
func (m *MCache[TKey, TValue]) Close() error {
	return m.rds.Client.Close()
}
