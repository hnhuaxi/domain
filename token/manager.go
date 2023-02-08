package token

import (
	"context"
	"time"

	"github.com/hnhuaxi/platform/logger"
	"github.com/hnhuaxi/platform/utils"
)

type Manager struct {
	Interval time.Duration `default:"30s"`
	Logger   *logger.Logger

	store utils.Map[Token, bool]
	ctx   context.Context
	close context.CancelFunc
}

func (mgr *Manager) Add(token Token) bool {
	if _, ok := mgr.store.LoadOrStore(token, true); ok {
		return false
	}
	return true
}

func (mgr *Manager) Delete(token Token) bool {
	if _, ok := mgr.store.LoadAndDelete(token); ok {
		return true
	}

	return false
}

func (mgr *Manager) Start() error {
	var (
		ctx, cancel = context.WithCancel(context.Background())
		tick        = time.NewTicker(mgr.Interval)
		log         = mgr.Logger.Sugar()
	)

	mgr.ctx = ctx
	mgr.close = cancel
	for {
		select {
		case t := <-tick.C:
			mgr.store.Range(func(key Token, value bool) bool {
				log.Infof("key %s expires at %s", key.AccessToken(), key.AccessTokenExpires())
				if key.AccessTokenExpires().Before(t) {
					factory, ok := LookupFactory(key)
					if !ok {
						return true
					}
					if err := factory().Refresh(key); err != nil {
						log.Warnf("refresh key %s error %s", key.AccessToken(), err)
					}
				}
				return true
			})
		case <-mgr.ctx.Done():
			return mgr.ctx.Err()
		}
	}
}

func (mgr *Manager) Close() error {
	if mgr.close != nil {
		mgr.close()
	}

	return nil
}
