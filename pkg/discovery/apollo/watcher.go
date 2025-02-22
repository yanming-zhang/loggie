package apollo

import (
	"github.com/apolloconfig/agollo/v4/storage"
)

type watcher struct {
	apolloConfig    *apolloConfig
	closeChan       chan struct{}
	changeNamespace chan string
}

func newWatcher(s *apolloConfig) *watcher {
	w := &watcher{
		apolloConfig:    s,
		changeNamespace: make(chan string),
		closeChan:       make(chan struct{}),
	}
	w.apolloConfig.client.AddChangeListener(&customChangeListener{in: w.changeNamespace})
	return w
}

type customChangeListener struct {
	in chan<- string
}

func (c *customChangeListener) OnChange(event *storage.ChangeEvent) {
	c.in <- event.Namespace
}

func (c *customChangeListener) OnNewestChange(event *storage.FullChangeEvent) {}

func (w *watcher) Change() ([]*Data, error) {
	select {
	case <-w.closeChan:
		return nil, nil
	case v, ok := <-w.changeNamespace:
		if !ok {
			return nil, nil
		}
		var data = make([]*Data, 0)
		data = append(data, w.apolloConfig.loadNameSpace(v))
		return data, nil
	}
}

func (w *watcher) Close() error {
	close(w.closeChan)
	return nil
}
