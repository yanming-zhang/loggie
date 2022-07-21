package apollo

import (
	"context"
)

type Option func(*options)

type options struct {
	ctx            context.Context
	appid          string
	cluster        string
	addr           string
	secret         string
	isBackupConfig bool
	namespace      string
}

type Data struct {
	Key string
	Val []byte
}

func WithContext(ctx context.Context) Option {
	return func(o *options) {
		o.ctx = ctx
	}
}

func WithAppid(appid string) Option {
	return func(o *options) {
		o.appid = appid
	}
}

func WithCluster(cluster string) Option {
	return func(o *options) {
		o.cluster = cluster
	}
}
func WithAddr(addr string) Option {
	return func(o *options) {
		o.addr = addr
	}
}

func WithSecret(secret string) Option {
	return func(o *options) {
		o.secret = secret
	}
}

func WithIsBackupConfig(isBackupConfig bool) Option {
	return func(o *options) {
		o.isBackupConfig = isBackupConfig
	}
}

func WithNamespace(namespace string) Option {
	return func(o *options) {
		o.namespace = namespace
	}
}

func NewOptions(opts ...Option) *options {
	options := &options{
		ctx:       context.Background(),
		namespace: "loggie",
	}
	for _, opt := range opts {
		opt(options)
	}
	return options
}
