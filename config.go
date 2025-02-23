package config

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"git.yujing.live/Golang/source/pkg/threading"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/signer"

	"github.com/panjf2000/ants"

	"github.com/go-resty/resty/v2"

	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v3"

	errDef "srv_admin/model/errors"

	"go_table_memcache/source/mcache"
)

var GameTypeAliasCache *mcache.GameTypeAliasCache


func (g *GameTypeAlias) GetAliases(ctx context.Context, in *cpb.ReqWithId) (*pb.GameTypeAliases, error) {
	db := config.NewDB()
	var aliases []*GameTypeAlias
	if err := db.Where("game_type_id=?", in.Id).Find(&aliases).Error; err != nil {
		log.L().Errorc(ctx, "[game_type_alias] find list fail [%s], in: '%v'", err.Error(), in)
		return nil, errDef.Errorf(merrors.INTERNAL_SERVER_ERR, codes.Internal, "internal server error")
	}
	var aliasInfos []*pb.GameTypeAliasInfo
	for _, item := range aliases {
		aliasInfos = append(aliasInfos, g.MarshalMPb(&item.GameTypeAlias))
	}
	return &pb.GameTypeAliases{Data: aliasInfos}, nil
}

func (g *GameTypeAlias) GetAliasesWithCache(ctx context.Context, in *cpb.ReqWithId) (*pb.GameTypeAliases, error) {
	list := config.GameTypeAliasCache.GetAll(ctx, in.Id)
	var aliasInfos []*pb.GameTypeAliasInfo
	for _, item := range list {
		aliasInfos = append(aliasInfos, g.MarshalMPb(item))
	}
	return &pb.GameTypeAliases{Data: aliasInfos}, nil
}

func (g *GameTypeAlias) GetAliasWithCache(ctx context.Context, in *cpb.ReqWithIdLanguage) (*pb.GameTypeAliasInfo, error) {
	defaultLanguage, systemLanguage := GetDefaultLanguage(in.Language)
	item := config.GameTypeAliasCache.Get(ctx, in.Id, in.Language, defaultLanguage, systemLanguage)
	if item == nil {
		return &pb.GameTypeAliasInfo{}, errDef.Errorf(errDef.ERR_GAME_TYPE_ALIAS_NOT_FOUND,
			codes.NotFound,
			"game type %d alias in language '%s' not found", in.Id, in.Language)
	}
	return g.MarshalMPb(item), nil
}
