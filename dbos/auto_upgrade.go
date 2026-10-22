package dbos

// Auto-upgrade: hand work left on an older application version to the latest one.

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
)

type autoUpgradeLoop struct {
	pass     AutoUpgradePass
	interval time.Duration
	logger   *slog.Logger

	// Derived from the root DBOS context, so cancelling the root ends the loop and the
	// pass in flight; stop cancels it on its own when the root is still live.
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	stopOnce sync.Once
	stopErr  error
}

func newAutoUpgradeLoop(parent context.Context, pass AutoUpgradePass, interval time.Duration, logger *slog.Logger) *autoUpgradeLoop {
	ctx, cancel := context.WithCancel(parent)
	return &autoUpgradeLoop{
		pass:     pass,
		interval: interval,
		logger:   logger,
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
}

func (l *autoUpgradeLoop) start() {
	l.logger.Debug("Starting auto-upgrade", "interval", l.interval)
	go l.run()
}

func (l *autoUpgradeLoop) run() {
	defer close(l.done)
	timer := time.NewTimer(l.interval)
	defer timer.Stop()
	for {
		if _, err := l.pass.Run(l.ctx); err != nil && l.ctx.Err() == nil {
			l.logger.Warn("Auto-upgrade pass failed", "error", err)
		}
		select {
		case <-l.ctx.Done():
			return
		case <-timer.C:
			timer.Reset(l.interval)
		}
	}
}

// stop cancels the pass in flight, waits for the loop to exit for up to timeout, then
// closes the pass. It returns an error if the loop did not exit within timeout.
func (l *autoUpgradeLoop) stop(timeout time.Duration) error {
	l.stopOnce.Do(func() {
		l.cancel()
		select {
		case <-l.done:
		case <-time.After(timeout):
			l.logger.Warn("Timeout waiting for the auto-upgrade pass to stop", "timeout", timeout)
			l.stopErr = fmt.Errorf("auto-upgrade did not stop within %v", timeout)
		}
		if err := l.pass.Close(); err != nil {
			l.logger.Warn("Auto-upgrade pass did not close cleanly", "error", err)
		}
	})
	return l.stopErr
}

// startAutoUpgrade builds and starts the pass if any registered workflow enrolled.
func (c *dbosContext) startAutoUpgrade() error {
	names := c.autoUpgradableWorkflows()
	if len(names) == 0 {
		return nil
	}
	// Entitlement check here if it wasn't done when creating the context.
	if !c.config.AutoUpgrade {
		if err := requireEnterprise([]enterpriseFeature{autoUpgradeFeature}, c.config.TransactEnterpriseKey, c.config.ConductorAPIKey, c.config.ConductorURL); err != nil {
			return err
		}
	}
	factory := registeredAutoUpgradeFactory()
	if factory == nil {
		return enterpriseUnavailableError(autoUpgradeFeature.description)
	}
	pass, err := factory(c, AutoUpgradeOptions{GracePeriod: c.config.AutoUpgradeGracePeriod})
	if err != nil {
		return models.NewInitializationError(fmt.Sprintf("failed to initialize auto-upgrade: %v", err))
	}
	c.autoUpgrader = newAutoUpgradeLoop(c.ctx, pass, c.config.AutoUpgradeInterval, c.logger)
	c.autoUpgrader.start()
	c.logger.Debug("Auto-upgrade started", "workflows", names)
	return nil
}
