//! Exercises the public test harness (`bsky_bot_sdk::testkit`) the way a bot
//! author would: define handlers, drive them with fixtures, assert on the
//! recorded actions — with no network. Doubles as a check that the harness's
//! public surface is actually usable from outside the crate.

use bsky_bot_sdk::prelude::*;
use bsky_bot_sdk::testkit::MockBot;

/// A handler under test: like every mention and reply with a fixed thank-you.
async fn like_and_thank(ctx: Context, notif: Notification) -> Result<()> {
    ctx.like(&notif).await?;
    ctx.reply_to(&notif, "thanks for the mention!").await?;
    Ok(())
}

#[tokio::test]
async fn handler_likes_then_replies() {
    let bot = MockBot::new().await;

    like_and_thank(bot.context(), bot.mention("alice.test", "hey @you"))
        .await
        .expect("handler should succeed offline");

    // A like and a reply, both created.
    assert_eq!(bot.created_in("app.bsky.feed.like").len(), 1, "one like");
    assert_eq!(bot.posts(), vec!["thanks for the mention!"], "one reply");

    // Nothing else slipped out.
    assert_eq!(
        bot.created().len(),
        2,
        "exactly two records created (like + post)"
    );
}

/// A follow-back handler, tested in isolation.
async fn follow_back(ctx: Context, notif: Notification) -> Result<()> {
    ctx.follow_back(&notif).await?;
    Ok(())
}

#[tokio::test]
async fn follow_back_only_follows() {
    let bot = MockBot::new().await;
    follow_back(bot.context(), bot.follow("bob.test"))
        .await
        .expect("handler ok");

    assert_eq!(bot.created_in("app.bsky.graph.follow").len(), 1);
    assert!(bot.posts().is_empty(), "a follow-back posts nothing");
}

/// A DM echo handler, tested against a synthesized incoming message.
async fn echo(ctx: Context, dm: DirectMessage) -> Result<()> {
    ctx.send_dm_to_convo(dm.convo_id(), format!("echo: {}", dm.text()))
        .await?;
    Ok(())
}

#[tokio::test]
async fn dm_echo_sends_the_message_back() {
    let bot = MockBot::new().await;
    echo(
        bot.context(),
        bot.direct_message("did:plc:friend0000000000000000000", "convo-42", "ping"),
    )
    .await
    .expect("handler ok");

    let sent: Vec<_> = bot
        .requests()
        .into_iter()
        .filter(|r| r.nsid == "chat.bsky.convo.sendMessage")
        .collect();
    assert_eq!(sent.len(), 1);
    let text = sent[0].json().unwrap()["message"]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(text, "echo: ping");
}
