package sqlqueue

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrQuotaHolderLapsed means the holder's lease expired; its slots no longer
// count and it must register again under a new name.
var ErrQuotaHolderLapsed = errors.New("sqlqueue: quota holder lease lapsed")

const dbNowMs = `(extract(epoch FROM clock_timestamp()) * 1000)::bigint`

// RegisterQuotaHolder creates a holder whose concurrency slots count until its
// lease lapses.
func (s *Store) RegisterQuotaHolder(ctx context.Context, holder string, ttl time.Duration) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO async_quota_holders (holder, expires_ms) VALUES ($1, `+dbNowMs+` + $2)`,
		holder, ttl.Milliseconds())
	if err != nil {
		return fmt.Errorf("sqlqueue: register quota holder: %w", err)
	}
	return nil
}

// RenewQuotaHolder extends a live holder's lease. It returns false once the
// lease has lapsed, and a lapsed holder is never renewed.
func (s *Store) RenewQuotaHolder(ctx context.Context, holder string, ttl time.Duration) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE async_quota_holders SET expires_ms = `+dbNowMs+` + $2 WHERE holder = $1 AND expires_ms > `+dbNowMs,
		holder, ttl.Milliseconds())
	if err != nil {
		return false, fmt.Errorf("sqlqueue: renew quota holder: %w", err)
	}
	return oneRow(res)
}

// AcquireQuotaSlots takes up to n of limit concurrent slots for key on behalf
// of holder and returns how many it took.
func (s *Store) AcquireQuotaSlots(ctx context.Context, holder, key string, n, limit int) (int, error) {
	var granted int
	err := s.db.QueryRowContext(ctx, `SELECT async_quota_acquire($1, $2, $3, $4)`, key, holder, n, limit).Scan(&granted)
	if err != nil {
		return 0, fmt.Errorf("sqlqueue: acquire quota slots: %w", err)
	}
	if granted < 0 {
		return 0, ErrQuotaHolderLapsed
	}
	return granted, nil
}

// ReleaseQuotaSlots returns counts[i] of holder's slots for keys[i].
func (s *Store) ReleaseQuotaSlots(ctx context.Context, holder string, keys []string, counts []int) error {
	if len(keys) != len(counts) {
		return fmt.Errorf("sqlqueue: release quota slots: %d keys but %d counts", len(keys), len(counts))
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE async_quota_slots s SET used = greatest(s.used - r.n, 0)
FROM unnest($2::text[], $3::int[]) AS r(key, n)
WHERE s.holder = $1 AND s.key = r.key`, holder, keys, counts)
	if err != nil {
		return fmt.Errorf("sqlqueue: release quota slots: %w", err)
	}
	return nil
}

// AdmitQuota admits up to n requests for key while fewer than limit were
// admitted in the trailing window, and returns how many it admitted.
func (s *Store) AdmitQuota(ctx context.Context, key string, n, limit int, window time.Duration) (int, error) {
	var granted int
	err := s.db.QueryRowContext(ctx, `SELECT async_quota_admit($1, $2, $3, $4)`, key, n, limit, window.Milliseconds()).Scan(&granted)
	if err != nil {
		return 0, fmt.Errorf("sqlqueue: admit quota: %w", err)
	}
	return granted, nil
}

// DeleteQuotaHolder deletes a holder and its slots, freeing everything it still counts.
func (s *Store) DeleteQuotaHolder(ctx context.Context, holder string) error {
	_, err := s.db.ExecContext(ctx, `
WITH gone AS (
	DELETE FROM async_quota_holders WHERE holder = $1 RETURNING holder
)
DELETE FROM async_quota_slots s USING gone WHERE s.holder = gone.holder`, holder)
	if err != nil {
		return fmt.Errorf("sqlqueue: delete quota holder: %w", err)
	}
	return nil
}

// ReapQuotaHolders deletes holders whose lease lapsed more than grace ago,
// along with their slots.
func (s *Store) ReapQuotaHolders(ctx context.Context, grace time.Duration) error {
	_, err := s.db.ExecContext(ctx, `
WITH dead AS (
	DELETE FROM async_quota_holders WHERE expires_ms < `+dbNowMs+` - $1 RETURNING holder
)
DELETE FROM async_quota_slots s USING dead WHERE s.holder = dead.holder`, grace.Milliseconds())
	if err != nil {
		return fmt.Errorf("sqlqueue: reap quota holders: %w", err)
	}
	return nil
}
