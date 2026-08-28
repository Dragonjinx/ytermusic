use std::time::{Duration, Instant};

/// How long to wait after a rebuild attempt before trying again.
///
/// Runtime stream errors (device lost, backend glitches after suspend/resume)
/// can arrive in bursts, and any single attempt is enough to re-negotiate the
/// device. This prevents a noisy device from spinning the player in a rebuild
/// loop.
pub(crate) const RECOVERY_COOLDOWN: Duration = Duration::from_secs(2);

/// After this many *consecutive* failed rebuilds, give up and surface the
/// error to the user (DeviceLost screen) instead of retrying forever.
pub(crate) const MAX_CONSECUTIVE_FAILURES: u32 = 3;

/// Gating policy for automatic audio-device recovery.
///
/// When a device stream reports an error (e.g. after a suspend/resume cycle
/// corrupted the output device), the player rebuilds its output stream. This
/// policy rate-limits those rebuilds so a noisy device cannot spin the player,
/// and gives up after repeated failures so the user isn't stuck in an endless
/// retry loop.
#[derive(Debug, Clone)]
pub struct RecoveryPolicy {
    last_attempt: Option<Instant>,
    consecutive_failures: u32,
}

impl Default for RecoveryPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl RecoveryPolicy {
    /// Creates a policy that has never attempted a recovery.
    pub fn new() -> Self {
        Self {
            last_attempt: None,
            consecutive_failures: 0,
        }
    }

    /// Whether an automatic rebuild may be attempted right now.
    pub fn should_attempt(&self, now: Instant) -> bool {
        match self.last_attempt {
            Some(last) => now.saturating_duration_since(last) >= RECOVERY_COOLDOWN,
            None => true,
        }
    }

    /// Records that a rebuild was attempted (regardless of outcome) at `now`.
    pub fn record_attempt(&mut self, now: Instant) {
        self.last_attempt = Some(now);
    }

    /// Records that a rebuild succeeded, resetting the consecutive-failure count.
    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
    }

    /// Records that a rebuild failed.
    pub fn record_failure(&mut self) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
    }

    /// Whether too many consecutive failures happened and automatic recovery
    /// should stop (the caller should surface the error to the user instead).
    pub fn exhausted(&self) -> bool {
        self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> Instant {
        Instant::now()
    }

    #[test]
    fn allows_first_attempt() {
        let policy = RecoveryPolicy::new();
        assert!(policy.should_attempt(now()));
        assert!(!policy.exhausted());
    }

    #[test]
    fn cooldown_blocks_rapid_retries() {
        let mut policy = RecoveryPolicy::new();
        let t0 = now();
        assert!(policy.should_attempt(t0));
        policy.record_attempt(t0);
        policy.record_failure();

        // A second error arriving before the cooldown elapsed must be ignored.
        assert!(!policy.should_attempt(t0 + Duration::from_secs(1)));
        // Once the cooldown elapsed, retrying is allowed again.
        assert!(policy.should_attempt(t0 + RECOVERY_COOLDOWN));
    }

    #[test]
    fn cooldown_elapsed_allows_retry_after_failure() {
        let mut policy = RecoveryPolicy::new();
        let t0 = now();
        policy.record_attempt(t0);
        policy.record_failure();

        assert!(policy.should_attempt(t0 + Duration::from_secs(5)));
        assert!(!policy.exhausted());
    }

    #[test]
    fn consecutive_failures_exhaust_recovery() {
        let mut policy = RecoveryPolicy::new();
        let mut t = now();
        for _ in 0..MAX_CONSECUTIVE_FAILURES {
            // Skip past the cooldown so every failure counts.
            t += Duration::from_secs(30);
            policy.record_attempt(t);
            policy.record_failure();
        }
        assert!(policy.exhausted());
    }

    #[test]
    fn success_resets_failure_count() {
        let mut policy = RecoveryPolicy::new();
        let mut t = now();

        // Two consecutive failures: not exhausted yet.
        t += Duration::from_secs(30);
        policy.record_attempt(t);
        policy.record_failure();
        t += Duration::from_secs(30);
        policy.record_attempt(t);
        policy.record_failure();
        assert!(!policy.exhausted());

        // A success resets the count, so one more failure stays under the cap.
        policy.record_success();
        t += Duration::from_secs(30);
        policy.record_attempt(t);
        policy.record_failure();
        assert!(!policy.exhausted());
    }

    #[test]
    fn failures_separated_by_success_never_exhaust() {
        let mut policy = RecoveryPolicy::new();
        let mut t = now();
        // 20 failure cycles, each followed by a successful recovery.
        // As long as recoveries succeed in between, the policy keeps trying.
        for _ in 0..20 {
            t += Duration::from_secs(30);
            policy.record_attempt(t);
            policy.record_failure();
            policy.record_success();
        }
        assert!(!policy.exhausted());
    }
}
