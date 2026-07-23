//! A command router for `@bot <command> <args>` style interactions.
//!
//! Many bots expose a small command language — `@mybot weather London`,
//! `!roll 2d6`, a DM saying `subscribe`. This module turns that pattern into a
//! dispatch table: register a handler per command name and the router parses each
//! incoming mention (or direct message), matches the command word, and invokes the
//! right handler with the parsed [`Command`].
//!
//! Wire it up on the builder with
//! [`command`](crate::BotBuilder::command) /
//! [`dm_command`](crate::BotBuilder::dm_command); the middleware chain
//! ([`before`](crate::BotBuilder::before) / [`after`](crate::BotBuilder::after) and
//! the [`block_authors`](crate::BotBuilder::block_authors)-style filters) composes
//! with it.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use crate::context::Context;
use crate::dm::DirectMessage;
use crate::error::Result;
use crate::event::Notification;
use crate::handler::BoxFuture;

/// A parsed command: the command word plus its arguments.
///
/// Produced by the [command router](crate::BotBuilder::command) from a message
/// like `@mybot weather London today` → name `weather`, args `["London",
/// "today"]`, rest `"London today"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    name: String,
    args: Vec<String>,
    rest: String,
}

impl Command {
    /// The command word, as typed (case preserved). Matching against registered
    /// commands is case-insensitive.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The whitespace-separated arguments after the command word.
    pub fn args(&self) -> &[String] {
        &self.args
    }

    /// The `i`-th argument, if present.
    pub fn arg(&self, i: usize) -> Option<&str> {
        self.args.get(i).map(String::as_str)
    }

    /// Everything after the command word, verbatim and trimmed — handy for
    /// free-form commands (`echo hello   world` → `"hello   world"`).
    pub fn rest(&self) -> &str {
        &self.rest
    }
}

/// Parse a command out of message `text`.
///
/// - `prefix`: if `Some("!")`, the command portion must begin with that prefix
///   (which is stripped); if `None`, the first word *is* the command.
/// - `strip_handles`: when `true` (mentions), any leading run of `@mention`
///   tokens is removed first, so `@mybot help` addresses the command `help`.
///
/// Returns `None` when there is no command (empty text, a bare mention with no
/// word, or a required prefix that is absent).
fn parse_command(text: &str, prefix: Option<&str>, strip_handles: bool) -> Option<Command> {
    let mut s = text.trim();

    if strip_handles {
        // Strip a leading run of `@mention` tokens (and the space after each).
        while let Some(rest) = s.strip_prefix('@') {
            let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
            if end == 0 {
                break; // a lone '@' is not a mention token
            }
            s = rest[end..].trim_start();
        }
    }

    if let Some(prefix) = prefix {
        s = s.strip_prefix(prefix)?.trim_start();
    }

    if s.is_empty() {
        return None;
    }

    let name_end = s.find(char::is_whitespace).unwrap_or(s.len());
    let name = s[..name_end].to_string();
    let rest = s[name_end..].trim().to_string();
    let args = rest.split_whitespace().map(str::to_string).collect();

    Some(Command { name, args, rest })
}

// ---------------------------------------------------------------------------
// Routers
// ---------------------------------------------------------------------------

pub(crate) type CommandHandlerFn =
    Arc<dyn Fn(Context, Notification, Command) -> BoxFuture<Result<()>> + Send + Sync>;

pub(crate) type DmCommandHandlerFn =
    Arc<dyn Fn(Context, DirectMessage, Command) -> BoxFuture<Result<()>> + Send + Sync>;

/// Erase a concrete async command handler into a [`CommandHandlerFn`].
pub(crate) fn boxed_command_handler<F, Fut>(handler: F) -> CommandHandlerFn
where
    F: Fn(Context, Notification, Command) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    Arc::new(move |ctx, notif, cmd| Box::pin(handler(ctx, notif, cmd)))
}

/// Erase a concrete async DM command handler into a [`DmCommandHandlerFn`].
pub(crate) fn boxed_dm_command_handler<F, Fut>(handler: F) -> DmCommandHandlerFn
where
    F: Fn(Context, DirectMessage, Command) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    Arc::new(move |ctx, dm, cmd| Box::pin(handler(ctx, dm, cmd)))
}

/// The command dispatch table for notifications (mentions).
#[derive(Default, Clone)]
pub(crate) struct CommandRouter {
    prefix: Option<String>,
    commands: HashMap<String, CommandHandlerFn>,
    fallback: Option<CommandHandlerFn>,
}

impl CommandRouter {
    pub(crate) fn set_prefix(&mut self, prefix: String) {
        self.prefix = Some(prefix);
    }

    pub(crate) fn register(&mut self, name: &str, handler: CommandHandlerFn) {
        self.commands.insert(name.to_lowercase(), handler);
    }

    pub(crate) fn set_fallback(&mut self, handler: CommandHandlerFn) {
        self.fallback = Some(handler);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.commands.is_empty() && self.fallback.is_none()
    }

    /// Parse and dispatch a mention. Unmatched commands go to the fallback (if
    /// any); a message that is not a command at all is ignored.
    pub(crate) async fn dispatch(&self, ctx: Context, notif: Notification) -> Result<()> {
        let Some(text) = notif.text() else {
            return Ok(());
        };
        let Some(cmd) = parse_command(&text, self.prefix.as_deref(), true) else {
            return Ok(());
        };
        match self.commands.get(&cmd.name.to_lowercase()) {
            Some(handler) => handler(ctx, notif, cmd).await,
            None => match &self.fallback {
                Some(fallback) => fallback(ctx, notif, cmd).await,
                None => Ok(()),
            },
        }
    }
}

/// The command dispatch table for direct messages.
#[derive(Default, Clone)]
pub(crate) struct DmCommandRouter {
    prefix: Option<String>,
    commands: HashMap<String, DmCommandHandlerFn>,
    fallback: Option<DmCommandHandlerFn>,
}

impl DmCommandRouter {
    pub(crate) fn set_prefix(&mut self, prefix: String) {
        self.prefix = Some(prefix);
    }

    pub(crate) fn register(&mut self, name: &str, handler: DmCommandHandlerFn) {
        self.commands.insert(name.to_lowercase(), handler);
    }

    pub(crate) fn set_fallback(&mut self, handler: DmCommandHandlerFn) {
        self.fallback = Some(handler);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.commands.is_empty() && self.fallback.is_none()
    }

    /// Parse and dispatch a direct message. A DM carries no mention, so the first
    /// word is the command (unless a prefix is configured).
    pub(crate) async fn dispatch(&self, ctx: Context, dm: DirectMessage) -> Result<()> {
        let Some(cmd) = parse_command(dm.text(), self.prefix.as_deref(), false) else {
            return Ok(());
        };
        match self.commands.get(&cmd.name.to_lowercase()) {
            Some(handler) => handler(ctx, dm, cmd).await,
            None => match &self.fallback {
                Some(fallback) => fallback(ctx, dm, cmd).await,
                None => Ok(()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parser (pure) -----------------------------------------------------

    #[test]
    fn mention_command_strips_the_leading_handle() {
        let cmd = parse_command("@mybot.bsky.social weather London today", None, true)
            .expect("a command");
        assert_eq!(cmd.name(), "weather");
        assert_eq!(cmd.args(), ["London", "today"]);
        assert_eq!(cmd.rest(), "London today");
        assert_eq!(cmd.arg(0), Some("London"));
        assert_eq!(cmd.arg(2), None);
    }

    #[test]
    fn multiple_leading_mentions_are_all_stripped() {
        let cmd = parse_command("@mybot @someone roll 2d6", None, true)
            .expect("a command after mentions");
        assert_eq!(cmd.name(), "roll");
        assert_eq!(cmd.args(), ["2d6"]);
    }

    #[test]
    fn a_bare_mention_with_no_word_is_not_a_command() {
        assert_eq!(parse_command("@mybot", None, true), None);
        assert_eq!(parse_command("@mybot   ", None, true), None);
    }

    #[test]
    fn a_prefix_is_required_when_configured_and_then_stripped() {
        // With a "!" prefix, plain text is not a command…
        assert_eq!(parse_command("weather London", Some("!"), false), None);
        // …but a prefixed one is, with the prefix removed from the name.
        let cmd = parse_command("!weather London", Some("!"), false).expect("prefixed command");
        assert_eq!(cmd.name(), "weather");
        assert_eq!(cmd.args(), ["London"]);
    }

    #[test]
    fn prefix_combines_with_mention_stripping() {
        let cmd = parse_command("@mybot !ping", Some("!"), true).expect("prefixed after mention");
        assert_eq!(cmd.name(), "ping");
        assert!(cmd.args().is_empty());
    }

    #[test]
    fn dm_first_word_is_the_command_without_a_prefix() {
        let cmd = parse_command("subscribe daily", None, false).expect("a dm command");
        assert_eq!(cmd.name(), "subscribe");
        assert_eq!(cmd.args(), ["daily"]);
    }

    #[test]
    fn empty_or_whitespace_text_is_never_a_command() {
        assert_eq!(parse_command("", None, false), None);
        assert_eq!(parse_command("    ", None, true), None);
    }

    #[test]
    fn rest_preserves_internal_spacing_but_name_matching_is_case_insensitive() {
        let cmd = parse_command("Echo  hello   world", None, false).expect("a command");
        // The name is preserved as typed…
        assert_eq!(cmd.name(), "Echo");
        // …and `rest` keeps the original internal spacing.
        assert_eq!(cmd.rest(), "hello   world");
    }

    // --- routers (dispatch via the mock harness) ---------------------------

    use crate::testkit::MockBot;
    use std::sync::Mutex;

    #[tokio::test]
    async fn router_invokes_the_matching_command_case_insensitively() {
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut router = CommandRouter::default();
        let seen_h = Arc::clone(&seen);
        router.register(
            "Weather",
            boxed_command_handler(move |_ctx, _n, cmd| {
                let seen = Arc::clone(&seen_h);
                async move {
                    seen.lock().unwrap().push(format!("weather:{}", cmd.rest()));
                    Ok(())
                }
            }),
        );

        let bot = MockBot::new().await;
        // Typed in a different case than registered — must still match.
        router
            .dispatch(
                bot.context(),
                bot.mention("alice.test", "@mockbot WEATHER Paris"),
            )
            .await
            .expect("dispatch ok");

        assert_eq!(&*seen.lock().unwrap(), &["weather:Paris"]);
    }

    #[tokio::test]
    async fn router_routes_unknown_commands_to_the_fallback() {
        let hits = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut router = CommandRouter::default();
        let known = Arc::clone(&hits);
        router.register(
            "ping",
            boxed_command_handler(move |_c, _n, _cmd| {
                let hits = Arc::clone(&known);
                async move {
                    hits.lock().unwrap().push("ping".into());
                    Ok(())
                }
            }),
        );
        let fb = Arc::clone(&hits);
        router.set_fallback(boxed_command_handler(move |_c, _n, cmd| {
            let hits = Arc::clone(&fb);
            async move {
                hits.lock()
                    .unwrap()
                    .push(format!("fallback:{}", cmd.name()));
                Ok(())
            }
        }));

        let bot = MockBot::new().await;
        router
            .dispatch(bot.context(), bot.mention("a.test", "@mockbot ping"))
            .await
            .unwrap();
        router
            .dispatch(bot.context(), bot.mention("a.test", "@mockbot frobnicate"))
            .await
            .unwrap();

        assert_eq!(
            &*hits.lock().unwrap(),
            &["ping", "fallback:frobnicate"],
            "known command hits its handler; unknown falls through to the fallback",
        );
    }

    #[tokio::test]
    async fn router_ignores_a_mention_that_is_not_a_command() {
        let hit = Arc::new(Mutex::new(false));
        let mut router = CommandRouter::default();
        let flag = Arc::clone(&hit);
        router.set_fallback(boxed_command_handler(move |_c, _n, _cmd| {
            let flag = Arc::clone(&flag);
            async move {
                *flag.lock().unwrap() = true;
                Ok(())
            }
        }));

        let bot = MockBot::new().await;
        // A bare mention with no command word must not even reach the fallback.
        router
            .dispatch(bot.context(), bot.mention("a.test", "@mockbot"))
            .await
            .unwrap();
        assert!(
            !*hit.lock().unwrap(),
            "no command word → no dispatch at all"
        );
    }

    #[tokio::test]
    async fn dm_router_dispatches_on_the_first_word() {
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut router = DmCommandRouter::default();
        let seen_h = Arc::clone(&seen);
        router.register(
            "subscribe",
            boxed_dm_command_handler(move |_c, _dm, cmd| {
                let seen = Arc::clone(&seen_h);
                async move {
                    seen.lock().unwrap().push(format!("sub:{}", cmd.rest()));
                    Ok(())
                }
            }),
        );

        let bot = MockBot::new().await;
        let dm = bot.direct_message("did:plc:sender000000000000000000", "c1", "subscribe daily");
        router.dispatch(bot.context(), dm).await.unwrap();
        assert_eq!(&*seen.lock().unwrap(), &["sub:daily"]);
    }
}
