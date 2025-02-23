package mcache

import (
	"context"
	"sync"

	"git.yj.live/Golang/source/log"
	"git.yj.live/Golang/source/pkg/database"
	mredis "git.yj.live/Golang/source/pkg/database/redis"
	"gorm.io/gorm"
)

type GameType struct {
	Id          int64  `gorm:"column:id;primaryKey;comment:'ID'"`
	GameSubtype string `gorm:"column:game_subtype;comment:'游戏子类型'"`
	Type        int32  `gorm:"column:type;comment:'顶级类型, 4 电子游戏 5 体育游戏'"`
	Status      int32  `gorm:"column:status;comment:'状态 1 开启 2 关闭'"`
	RuleId      int64  `gorm:"column:rule_id;uniqueIndex;comment:'规则ID'"`
	Updated     int64  `gorm:"column:updated;autoUpdateTime:milli;comment:'修改时间'"`
	Created     int64  `gorm:"column:created;comment:'创建时间'"`
}

func (GameType) TableName() string {
	return "game_type"
}

type GameTypeCache struct {
	*database.MCache[int64, GameType]
	dictRuleId sync.Map
}

func NewGameTypeCache(dbCli *gorm.DB, rdsCli *mredis.Client) (*GameTypeCache, error) {
	model := GameTypeCache{}
	mcache, err := database.NewMCache[int64, GameType](dbCli, rdsCli, "").
		WithGorm("", "").
		WithOnEvent(model.onEvent).
		Open()
	if err != nil {
		return nil, err
	}
	model.MCache = mcache
	if err := mcache.LoadAll(context.TODO()); err != nil {
		return nil, err
	}
	return &model, nil
}

func MustNewGameTypeCache(dbCli *gorm.DB, rdsCli *mredis.Client) *GameTypeCache {
	cache, err := NewGameTypeCache(dbCli, rdsCli)
	if err != nil {
		log.L().Panic(err)
	}
	return cache
}

func (g *GameTypeCache) onEvent(event database.MCacheEventType, id int64, gameTypes ...*GameType) {
	if gameTypes[0] == nil {
		return
	}
	switch event {
	case database.MCacheEventCreate, database.MCacheTypeUpdate:
		g.dictRuleId.Store(gameTypes[0].RuleId, gameTypes[0].Id)
	case database.MCacheEventDelete:
		g.dictRuleId.Delete(gameTypes[0].RuleId)
	}
}

func (g *GameTypeCache) Get(ctx context.Context, id int64) *GameType {
	return g.MCache.GetCache(ctx, id)
}

func (g *GameTypeCache) GetByRuleId(ctx context.Context, ruleId int64) *GameType {
	id, ok := g.dictRuleId.Load(ruleId)
	if !ok {
		return nil
	}
	return g.MCache.GetCache(ctx, id.(int64))
}

func (g *GameTypeCache) GetRuleIdById(ctx context.Context, id int64) int64 {
	gameType := g.MCache.GetCache(ctx, id)
	if gameType == nil {
		return id
	}
	return gameType.RuleId
}

func (g *GameTypeCache) GetIdByRuleId(ctx context.Context, ruleId int64) int64 {
	id, ok := g.dictRuleId.Load(ruleId)
	if !ok {
		return ruleId
	}
	return id.(int64)
}
