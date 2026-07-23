//! Declaring the bot account as **automated** with a profile self-label.
//!
//! Bluesky's [bot guidelines] recommend that automated accounts add a `bot`
//! self-label to their profile, so people and moderation tooling can recognize
//! them at a glance. It's a single, cheap, high-goodwill signal that also lowers
//! the chance of being mistaken for a spam account.
//!
//! The label lives in the account's `app.bsky.actor.profile` record (record key
//! `self`) as a [`com.atproto.label.defs#selfLabels`][self-labels] value. Apply
//! it declaratively with [`automated_label`](crate::BotBuilder::automated_label)
//! on the builder, or at runtime with
//! [`set_automated_label`](crate::Context::set_automated_label). Both preserve
//! every other profile field (display name, description, avatar, …) and any other
//! self-labels the account already carries.
//!
//! [bot guidelines]: https://docs.bsky.app/docs/starter-templates/bots
//! [self-labels]: https://atproto.com/specs/label

use atrium_api::app::bsky::actor::profile::RecordLabelsRefs;
use atrium_api::com::atproto::label::defs::{SelfLabelData, SelfLabelsData};
use atrium_api::types::Union;

/// The self-label value that marks an account as an automated bot.
///
/// This is the exact string the Bluesky bot guidelines prescribe — `"bot"` —
/// written into the profile record's `com.atproto.label.defs#selfLabels` values.
/// The user-facing API talks about "automated" accounts; on the wire the value is
/// `bot`.
pub const BOT_SELF_LABEL: &str = "bot";

/// Whether a profile's `labels` field already carries the [`BOT_SELF_LABEL`].
///
/// Returns `false` for `None`, for an empty self-label set, and for a labels
/// value in the open-union's unknown form (which never holds Bluesky self-labels).
pub(crate) fn has_bot_label(labels: &Option<Union<RecordLabelsRefs>>) -> bool {
    matches!(
        labels,
        Some(Union::Refs(RecordLabelsRefs::ComAtprotoLabelDefsSelfLabels(sl)))
            if sl.data.values.iter().any(|v| v.data.val == BOT_SELF_LABEL)
    )
}

/// Return the `labels` field that results from adding (`present = true`) or
/// removing (`present = false`) the [`BOT_SELF_LABEL`], preserving every other
/// self-label and its order.
///
/// - Adding is idempotent: a profile that already carries the label is unchanged.
/// - Removing the only self-label yields `None`, so the field is omitted from the
///   record entirely rather than serialized as an empty list.
/// - An unrecognized (unknown-union) labels value is treated as carrying no
///   self-labels; adding the bot label replaces it with a proper self-labels set.
pub(crate) fn set_bot_label(
    labels: Option<Union<RecordLabelsRefs>>,
    present: bool,
) -> Option<Union<RecordLabelsRefs>> {
    let mut values: Vec<String> = match labels {
        Some(Union::Refs(RecordLabelsRefs::ComAtprotoLabelDefsSelfLabels(sl))) => {
            sl.data.values.into_iter().map(|v| v.data.val).collect()
        }
        _ => Vec::new(),
    };

    let has = values.iter().any(|v| v == BOT_SELF_LABEL);
    match (present, has) {
        (true, false) => values.push(BOT_SELF_LABEL.to_owned()),
        (false, true) => values.retain(|v| v != BOT_SELF_LABEL),
        // Already in the desired state.
        _ => {}
    }

    if values.is_empty() {
        return None;
    }

    let self_labels = SelfLabelsData {
        values: values
            .into_iter()
            .map(|val| SelfLabelData { val }.into())
            .collect(),
    };
    Some(Union::Refs(
        RecordLabelsRefs::ComAtprotoLabelDefsSelfLabels(Box::new(self_labels.into())),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a `labels` union from a list of raw self-label values.
    fn labels_with(vals: &[&str]) -> Option<Union<RecordLabelsRefs>> {
        if vals.is_empty() {
            return None;
        }
        let self_labels = SelfLabelsData {
            values: vals
                .iter()
                .map(|v| {
                    SelfLabelData {
                        val: (*v).to_owned(),
                    }
                    .into()
                })
                .collect(),
        };
        Some(Union::Refs(
            RecordLabelsRefs::ComAtprotoLabelDefsSelfLabels(Box::new(self_labels.into())),
        ))
    }

    /// Extract the ordered list of raw self-label values from a `labels` union.
    fn vals_of(labels: &Option<Union<RecordLabelsRefs>>) -> Vec<String> {
        match labels {
            Some(Union::Refs(RecordLabelsRefs::ComAtprotoLabelDefsSelfLabels(sl))) => {
                sl.data.values.iter().map(|v| v.data.val.clone()).collect()
            }
            _ => Vec::new(),
        }
    }

    #[test]
    fn wire_value_is_bot() {
        // Guards against a silent drift away from the value Bluesky's guidelines
        // (and its appview) actually recognize.
        assert_eq!(BOT_SELF_LABEL, "bot");
    }

    #[test]
    fn adds_bot_label_to_a_profile_with_none() {
        let out = set_bot_label(None, true);
        assert!(has_bot_label(&out));
        assert_eq!(vals_of(&out), vec!["bot"]);
    }

    #[test]
    fn adding_is_idempotent() {
        let out = set_bot_label(labels_with(&["bot"]), true);
        assert_eq!(
            vals_of(&out),
            vec!["bot"],
            "adding an already-present label must not duplicate it",
        );
    }

    #[test]
    fn adds_bot_label_preserving_other_self_labels() {
        let out = set_bot_label(labels_with(&["!no-unauthenticated"]), true);
        assert_eq!(
            vals_of(&out),
            vec!["!no-unauthenticated", "bot"],
            "existing self-labels must survive, in order, with bot appended",
        );
    }

    #[test]
    fn removes_bot_label_preserving_other_self_labels() {
        let out = set_bot_label(labels_with(&["!no-unauthenticated", "bot"]), false);
        assert_eq!(
            vals_of(&out),
            vec!["!no-unauthenticated"],
            "removal must drop only the bot label",
        );
        assert!(!has_bot_label(&out));
    }

    #[test]
    fn removing_the_only_label_yields_none() {
        let out = set_bot_label(labels_with(&["bot"]), false);
        assert!(
            out.is_none(),
            "an empty self-label set must be omitted, not serialized as []",
        );
    }

    #[test]
    fn removing_an_absent_label_is_a_noop() {
        let out = set_bot_label(labels_with(&["!no-unauthenticated"]), false);
        assert_eq!(vals_of(&out), vec!["!no-unauthenticated"]);
    }

    #[test]
    fn has_bot_label_detects_presence_absence_and_none() {
        assert!(has_bot_label(&labels_with(&["bot"])));
        assert!(has_bot_label(&labels_with(&["!no-unauthenticated", "bot"])));
        assert!(!has_bot_label(&labels_with(&["!no-unauthenticated"])));
        assert!(!has_bot_label(&None));
    }
}
