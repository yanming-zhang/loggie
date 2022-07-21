package apollo

import (
	"github.com/apolloconfig/agollo/v4"
	"github.com/apolloconfig/agollo/v4/env/config"
	"strings"
)

type apolloConfig struct {
	client  agollo.Client
	options *options
}

func NewApolloConfig(opts ...Option) (*apolloConfig, error) {
	options := NewOptions(opts...)
	client, err := agollo.StartWithConfig(func() (*config.AppConfig, error) {
		return &config.AppConfig{
			AppID:          options.appid,
			Cluster:        options.cluster,
			NamespaceName:  options.namespace,
			IP:             options.addr,
			IsBackupConfig: options.isBackupConfig,
			Secret:         options.secret,
		}, nil
	})
	if err != nil {
		return nil, err
	}
	conf := &apolloConfig{
		client:  client,
		options: options,
	}
	return conf, nil
}

func (c *apolloConfig) Load() ([]*Data, error) {
	data := make([]*Data, 0)
	for _, v := range strings.Split(c.options.namespace, ",") {
		data = append(data, c.loadNameSpace(v))
	}
	return data, nil
}

func (c *apolloConfig) loadNameSpace(namespace string) *Data {
	val := c.client.GetConfig(namespace).GetContent()
	val = strings.TrimPrefix(val, "content=")
	return &Data{
		Key: namespace,
		Val: []byte(val),
	}
}

func (c *apolloConfig) Watch() (*watcher, error) {
	return newWatcher(c), nil
}
