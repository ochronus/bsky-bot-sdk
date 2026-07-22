//! Time-based scheduling: run bot actions on a fixed interval or a cron schedule.
//!
//! A [`Schedule`] says *when* a job fires; the job itself is any async closure
//! `Fn(Context) -> Future<Output = Result<()>>`. Register jobs on the
//! [`BotBuilder`](crate::BotBuilder) with
//! [`every`](crate::BotBuilder::every), [`cron`](crate::BotBuilder::cron), or
//! [`schedule`](crate::BotBuilder::schedule); they run concurrently with the
//! notification loop and stop cleanly when the bot shuts down.
//!
//! ```
//! use std::time::Duration;
//! use bsky_bot_sdk::Schedule;
//!
//! # fn demo() -> bsky_bot_sdk::Result<()> {
//! let _every = Schedule::every(Duration::from_secs(3600)); // hourly
//! let _cron = Schedule::cron("0 12 * * *")?;               // 12:00 UTC daily
//! let _macro = Schedule::cron("@daily")?;                  // 00:00 UTC daily
//! let _parsed: Schedule = "@every 30m".parse()?;           // simple interval syntax
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Local, Utc};
use croner::Cron;

use crate::context::Context;
use crate::error::{Error, Result};
use crate::handler::BoxFuture;

/// The timezone a cron [`Schedule`] is evaluated in.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Tz {
    /// Coordinated Universal Time. The default, and the least surprising choice
    /// for a server that may run in any region.
    #[default]
    Utc,
    /// The host machine's local timezone (as reported by the OS).
    Local,
}

/// When a scheduled job fires.
///
/// Build one with [`Schedule::every`] for a fixed interval, [`Schedule::cron`]
/// for a cron expression (UTC), [`Schedule::cron_local`] for local time, or by
/// parsing a string (see the [`FromStr`] impl).
#[derive(Clone, Debug)]
pub struct Schedule {
    kind: Kind,
}

#[derive(Clone, Debug)]
enum Kind {
    /// Fire once per `Duration`, measured from the previous fire (or startup).
    Interval(Duration),
    /// Fire at each matching cron instant, evaluated in `tz`. Boxed because a
    /// parsed `Cron` is large relative to the `Interval` variant.
    Cron { cron: Box<Cron>, tz: Tz },
}

impl Schedule {
    /// Fire every `interval`, starting one `interval` after the bot starts (never
    /// immediately on boot).
    ///
    /// The interval is measured from the end of the previous run, so a slow job
    /// does not pile up overlapping executions.
    pub fn every(interval: Duration) -> Self {
        Self {
            kind: Kind::Interval(interval),
        }
    }

    /// Parse a cron expression evaluated in **UTC**.
    ///
    /// Accepts standard 5-field (`min hour dom mon dow`) and 6-field
    /// (`sec min hour dom mon dow`) expressions, plus the macros `@yearly`
    /// (`@annually`), `@monthly`, `@weekly`, `@daily` (`@midnight`), and
    /// `@hourly`.
    ///
    /// # Errors
    /// Returns [`Error::InvalidInput`] if the expression cannot be parsed.
    pub fn cron(expr: &str) -> Result<Self> {
        Self::cron_in(expr, Tz::Utc)
    }

    /// Like [`cron`](Self::cron), but evaluated in the host's local timezone.
    ///
    /// # Errors
    /// Returns [`Error::InvalidInput`] if the expression cannot be parsed.
    pub fn cron_local(expr: &str) -> Result<Self> {
        Self::cron_in(expr, Tz::Local)
    }

    /// Parse a cron expression evaluated in an explicit [`Tz`].
    ///
    /// # Errors
    /// Returns [`Error::InvalidInput`] if the expression cannot be parsed.
    pub fn cron_in(expr: &str, tz: Tz) -> Result<Self> {
        let pattern = expand_macro(expr).unwrap_or(expr);
        // croner defaults to optional seconds and year, so 5-, 6-, and 7-field
        // expressions all parse without extra configuration.
        let cron = Cron::from_str(pattern).map_err(|err| {
            Error::invalid_input(format!("invalid cron expression {expr:?}: {err}"))
        })?;
        Ok(Self {
            kind: Kind::Cron {
                cron: Box::new(cron),
                tz,
            },
        })
    }

    /// The delay from `now` until this schedule's next fire, or `None` if it can
    /// never fire again (e.g. a cron expression with no future match).
    pub(crate) fn next_delay_from(&self, now: DateTime<Utc>) -> Option<Duration> {
        match &self.kind {
            Kind::Interval(interval) => Some(*interval),
            Kind::Cron { cron, tz } => {
                let next = match tz {
                    Tz::Utc => cron.find_next_occurrence(&now, false).ok()?,
                    // Evaluate in local time, then convert the instant back to UTC
                    // so the returned delay is timezone-agnostic.
                    Tz::Local => cron
                        .find_next_occurrence(&now.with_timezone(&Local), false)
                        .ok()?
                        .with_timezone(&Utc),
                };
                // A negative or zero span (clock skew, sub-second rounding) becomes
                // a tiny non-negative delay rather than an error.
                (next - now).to_std().ok()
            }
        }
    }
}

/// Expand a cron nickname macro to an equivalent 6-field expression. Returns
/// `None` for anything that is not a recognised macro, leaving it for the parser.
fn expand_macro(expr: &str) -> Option<&'static str> {
    match expr.trim() {
        "@yearly" | "@annually" => Some("0 0 0 1 1 *"),
        "@monthly" => Some("0 0 0 1 * *"),
        "@weekly" => Some("0 0 0 * * 0"),
        "@daily" | "@midnight" => Some("0 0 0 * * *"),
        "@hourly" => Some("0 0 * * * *"),
        _ => None,
    }
}

impl FromStr for Schedule {
    type Err = Error;

    /// Parse a schedule from a string:
    ///
    /// - `@every <duration>` — a fixed interval, where `<duration>` is human
    ///   syntax like `30s`, `5m`, or `1h30m` (see [`humantime`]).
    /// - anything else — a cron expression or macro, evaluated in UTC.
    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        if let Some(rest) = s.strip_prefix("@every") {
            let dur = humantime::parse_duration(rest.trim())
                .map_err(|err| Error::invalid_input(format!("invalid interval in {s:?}: {err}")))?;
            return Ok(Self::every(dur));
        }
        Self::cron(s)
    }
}

/// The async task run when a schedule fires: `Fn(Context) -> Future<Result<()>>`.
pub(crate) type TaskFn = Arc<dyn Fn(Context) -> BoxFuture<Result<()>> + Send + Sync>;

/// Erase a concrete async task closure into a [`TaskFn`].
pub(crate) fn boxed_task<F, Fut>(task: F) -> TaskFn
where
    F: Fn(Context) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    Arc::new(move |ctx| Box::pin(task(ctx)))
}

/// A registered scheduled job: a [`Schedule`] paired with its task and a label
/// used in logs.
#[derive(Clone)]
pub(crate) struct ScheduledJob {
    label: String,
    schedule: Schedule,
    task: TaskFn,
}

impl ScheduledJob {
    /// Run this job until `shutdown` flips to `true`: compute the delay to the
    /// next fire, sleep, run the task, repeat. Task errors are logged and never
    /// stop the loop.
    pub(crate) async fn run(self, ctx: Context, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                break;
            }
            let Some(delay) = self.schedule.next_delay_from(Utc::now()) else {
                tracing::warn!(job = %self.label, "schedule has no future occurrence; stopping job");
                break;
            };
            tokio::select! {
                _ = tokio::time::sleep(delay) => {
                    if let Err(err) = (self.task)(ctx.clone()).await {
                        tracing::error!(job = %self.label, error = %err, "scheduled task failed");
                    }
                }
                _ = shutdown.changed() => break,
            }
        }
    }
}

/// The registry of scheduled jobs for a bot.
#[derive(Default, Clone)]
pub(crate) struct Scheduler {
    jobs: Vec<ScheduledJob>,
}

impl Scheduler {
    /// Register a job. The label is derived from its position for log context.
    pub(crate) fn push(&mut self, schedule: Schedule, task: TaskFn) {
        let label = format!("schedule[{}]", self.jobs.len());
        self.jobs.push(ScheduledJob {
            label,
            schedule,
            task,
        });
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.jobs.len()
    }

    pub(crate) fn jobs(&self) -> &[ScheduledJob] {
        &self.jobs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, s)
            .single()
            .expect("valid timestamp")
    }

    #[test]
    fn every_reports_the_fixed_interval_regardless_of_now() {
        let s = Schedule::every(Duration::from_secs(90));
        assert_eq!(
            s.next_delay_from(at(2026, 7, 22, 10, 30, 0)),
            Some(Duration::from_secs(90))
        );
        // Same interval no matter when we ask.
        assert_eq!(
            s.next_delay_from(at(2026, 1, 1, 0, 0, 0)),
            Some(Duration::from_secs(90))
        );
    }

    #[test]
    fn cron_five_field_hourly_next_is_top_of_the_next_hour() {
        let s = Schedule::cron("0 * * * *").expect("valid 5-field cron");
        // From 10:30:00, the next `minute 0` is 11:00:00 → 30 minutes away.
        let delay = s.next_delay_from(at(2026, 7, 22, 10, 30, 0)).unwrap();
        assert_eq!(delay, Duration::from_secs(30 * 60));
    }

    #[test]
    fn cron_six_field_with_seconds_is_accepted() {
        let s = Schedule::cron("30 0 12 * * *").expect("valid 6-field cron");
        // From 12:00:00 the next 12:00:30 is 30 seconds away.
        let delay = s.next_delay_from(at(2026, 7, 22, 12, 0, 0)).unwrap();
        assert_eq!(delay, Duration::from_secs(30));
    }

    #[test]
    fn daily_macro_fires_within_the_next_day() {
        let s = Schedule::cron("@daily").expect("@daily is a valid macro");
        // From 10:30 the next midnight is 13h30m away.
        let delay = s.next_delay_from(at(2026, 7, 22, 10, 30, 0)).unwrap();
        assert_eq!(delay, Duration::from_secs((13 * 60 + 30) * 60));
    }

    #[test]
    fn invalid_cron_is_rejected() {
        assert!(matches!(
            Schedule::cron("not a cron expression"),
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            Schedule::cron("99 99 99 99 99"),
            Err(Error::InvalidInput(_))
        ));
    }

    #[test]
    fn from_str_at_every_parses_a_human_interval() {
        let s: Schedule = "@every 5m".parse().expect("valid interval");
        assert_eq!(
            s.next_delay_from(at(2026, 7, 22, 10, 30, 0)),
            Some(Duration::from_secs(5 * 60))
        );
    }

    #[test]
    fn from_str_bare_expression_is_treated_as_cron() {
        let s: Schedule = "0 12 * * *".parse().expect("valid cron");
        assert!(s.next_delay_from(at(2026, 7, 22, 0, 0, 0)).is_some());
    }

    #[test]
    fn from_str_rejects_a_bad_interval() {
        assert!(matches!(
            "@every nonsense".parse::<Schedule>(),
            Err(Error::InvalidInput(_))
        ));
    }

    #[test]
    fn scheduler_labels_jobs_by_position() {
        let mut sched = Scheduler::default();
        assert!(sched.is_empty());
        sched.push(
            Schedule::every(Duration::from_secs(1)),
            boxed_task(|_ctx| async move { Ok(()) }),
        );
        sched.push(
            Schedule::every(Duration::from_secs(2)),
            boxed_task(|_ctx| async move { Ok(()) }),
        );
        assert_eq!(sched.len(), 2);
        assert_eq!(sched.jobs()[0].label, "schedule[0]");
        assert_eq!(sched.jobs()[1].label, "schedule[1]");
    }
}
