//! Rich media & embeds.
//!
//! A fluent [`PostBuilder`] — obtained from [`Context::compose`](crate::Context::compose)
//! — attaches images, video, external link cards, and quote/record embeds to a
//! post. Every builder method is synchronous and merely records intent; all the
//! network work (blob uploads, OpenGraph fetches, video processing) happens once,
//! at the end, in [`PostBuilder::send`].
//!
//! ## Alt text is required, by type
//!
//! [`image`](PostBuilder::image) and [`video`](PostBuilder::video) take the alt
//! text as a *required positional argument*. There is no way to attach an image
//! without describing it — omitting alt text is a compile error, not a lint. The
//! fediverse treats missing alt text as an accessibility defect; this SDK makes
//! the accessible path the only path.
//!
//! ## Works on any PDS
//!
//! Blobs are uploaded to the bot's *own* PDS via `com.atproto.repo.uploadBlob`,
//! so images and link-card thumbnails work identically on `bsky.social` and on
//! self-hosted / third-party PDSes. Video uses the `getServiceAuth` →
//! `video.bsky.app` pipeline, with the auth token audience set to the bot's own
//! PDS DID (`did:web:<pds-host>`) — the form the video service requires for
//! third-party PDS accounts.
//!
//! ```no_run
//! # use bsky_bot_sdk::prelude::*;
//! # async fn f(ctx: Context, notif: Notification) -> Result<()> {
//! ctx.compose()
//!     .text("a cat, and the post that started it all")
//!     .image(std::fs::read("cat.png")?, "A ginger cat asleep on a keyboard")
//!     .quote(&notif)
//!     .send()
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::time::Duration;

use atrium_api::app::bsky::embed::{external, images, record, record_with_media, video};
use atrium_api::app::bsky::feed::post;
use atrium_api::app::bsky::video::defs as video_defs;
use atrium_api::com::atproto::repo::{create_record, strong_ref};
use atrium_api::com::atproto::server::get_service_auth;
use atrium_api::types::string::{Datetime, Did, Language, Nsid};
use atrium_api::types::{BlobRef, TypedBlobRef, Union};
use bsky_sdk::rich_text::RichText;

use crate::context::Context;
use crate::error::{Error, Result};
use crate::event::Notification;

/// Maximum number of images Bluesky allows in a single post.
pub const MAX_IMAGES: usize = 4;

/// Base URL of the Bluesky video service.
const VIDEO_SERVICE_BASE: &str = "https://video.bsky.app";
/// Cap on the amount of HTML we read when scraping a link card's `<head>`.
const LINK_CARD_HTML_LIMIT: usize = 1024 * 1024;
/// Cap on the size of a link-card thumbnail we will re-upload. Matches the PDS's
/// hard limit for an `external.thumb` blob (1 MB); larger preview images are
/// skipped, and the card renders with its title and description only.
const LINK_CARD_THUMB_LIMIT: usize = 1_000_000;
/// Timeout for a single outbound HTTP request (OpenGraph / video service).
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);
/// How long to poll the video service before giving up.
const VIDEO_POLL_ATTEMPTS: usize = 60;
/// Delay between video-service status polls.
const VIDEO_POLL_INTERVAL: Duration = Duration::from_secs(2);

// --- pending (pre-upload) media -------------------------------------------

struct PendingImage {
    bytes: Vec<u8>,
    alt: String,
    mime: Option<String>,
}

struct PendingVideo {
    bytes: Vec<u8>,
    alt: String,
}

enum PendingExternal {
    /// A URL whose OpenGraph card should be fetched at send time.
    Fetch(String),
    /// A fully-specified card (no fetching).
    Ready(external::ExternalData),
}

/// Resolved media, ready to be placed into a post's `embed` union.
enum Media {
    Images(images::Main),
    Video(video::Main),
    External(external::Main),
}

/// A fluent builder for a post with optional rich media and embeds.
///
/// Construct one with [`Context::compose`](crate::Context::compose), chain the
/// content you want, then call [`send`](PostBuilder::send). Builder methods are
/// cheap and synchronous; nothing is uploaded or posted until `send().await`.
#[must_use = "a PostBuilder does nothing until you call `.send().await`"]
pub struct PostBuilder {
    ctx: Context,
    text: String,
    reply: Option<post::ReplyRef>,
    langs: Option<Vec<Language>>,
    images: Vec<PendingImage>,
    video: Option<PendingVideo>,
    external: Option<PendingExternal>,
    quote: Option<strong_ref::Main>,
}

impl PostBuilder {
    pub(crate) fn new(ctx: Context) -> Self {
        Self {
            ctx,
            text: String::new(),
            reply: None,
            langs: None,
            images: Vec::new(),
            video: None,
            external: None,
            quote: None,
        }
    }

    /// Set (or replace) the post's text. Facets — mentions, links, hashtags — are
    /// detected automatically at send time.
    pub fn text(mut self, text: impl Into<String>) -> Self {
        self.text = text.into();
        self
    }

    /// Attach an image with **required** alt text (repeatable, up to
    /// [`MAX_IMAGES`]). The MIME type is sniffed from the image bytes; use
    /// [`image_with`](Self::image_with) to declare it explicitly.
    pub fn image(mut self, bytes: impl Into<Vec<u8>>, alt: impl Into<String>) -> Self {
        self.images.push(PendingImage {
            bytes: bytes.into(),
            alt: alt.into(),
            mime: None,
        });
        self
    }

    /// Attach an image with required alt text and an explicit MIME type (e.g.
    /// `"image/png"`), for the rare case where sniffing the bytes is not enough.
    pub fn image_with(
        mut self,
        bytes: impl Into<Vec<u8>>,
        alt: impl Into<String>,
        mime: impl Into<String>,
    ) -> Self {
        self.images.push(PendingImage {
            bytes: bytes.into(),
            alt: alt.into(),
            mime: Some(mime.into()),
        });
        self
    }

    /// Attach an MP4 video with **required** alt text. Uploading a video routes
    /// through the Bluesky video service (transcoding + captions), so `send()`
    /// blocks until processing completes or times out.
    ///
    /// A post may carry only one media kind — video excludes images and external
    /// cards.
    pub fn video(mut self, bytes: impl Into<Vec<u8>>, alt: impl Into<String>) -> Self {
        self.video = Some(PendingVideo {
            bytes: bytes.into(),
            alt: alt.into(),
        });
        self
    }

    /// Attach an external link "card". At send time the URL is fetched, its
    /// OpenGraph metadata (title, description, image) is parsed, and any preview
    /// image is uploaded as the card's thumbnail.
    ///
    /// A post may carry only one media kind — a link card excludes images and
    /// video (but can be combined with a [`quote`](Self::quote)).
    pub fn link_card(mut self, url: impl Into<String>) -> Self {
        self.external = Some(PendingExternal::Fetch(url.into()));
        self
    }

    /// Attach an external link card with explicit title/description and no
    /// fetching. Use this when you already have the metadata, or to avoid an
    /// outbound HTTP request.
    pub fn external(
        mut self,
        uri: impl Into<String>,
        title: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        self.external = Some(PendingExternal::Ready(external::ExternalData {
            description: description.into(),
            thumb: None,
            title: title.into(),
            uri: uri.into(),
        }));
        self
    }

    /// Quote-post the record that generated a notification (e.g. the post that
    /// mentioned the bot). Combine with media for a quote-with-media post.
    pub fn quote(mut self, notif: &Notification) -> Self {
        self.quote = Some(notif.subject_ref());
        self
    }

    /// Quote-post an arbitrary record by strong ref.
    pub fn quote_ref(mut self, subject: strong_ref::Main) -> Self {
        self.quote = Some(subject);
        self
    }

    /// Make this post a reply to a notification, threading correctly (its `root`
    /// is inherited from the parent's thread root when present).
    pub fn reply_to(mut self, notif: &Notification) -> Self {
        let parent = notif.subject_ref();
        let root = notif
            .as_post()
            .and_then(|p| p.reply.map(|r| r.root.clone()))
            .unwrap_or_else(|| parent.clone());
        self.reply = Some(post::ReplyRefData { parent, root }.into());
        self
    }

    /// Make this post a reply with explicit `parent` and `root` strong refs.
    pub fn reply(mut self, parent: strong_ref::Main, root: strong_ref::Main) -> Self {
        self.reply = Some(post::ReplyRefData { parent, root }.into());
        self
    }

    /// Declare the post's language(s) as BCP-47 tags (e.g. `"en"`, `"pt-BR"`).
    /// Invalid tags are silently skipped.
    pub fn langs<I, S>(mut self, langs: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let parsed: Vec<Language> = langs
            .into_iter()
            .filter_map(|s| s.as_ref().parse().ok())
            .collect();
        self.langs = if parsed.is_empty() {
            None
        } else {
            Some(parsed)
        };
        self
    }

    /// Upload any attached media, then publish the post.
    ///
    /// Errors if more than one media kind is attached, if more than
    /// [`MAX_IMAGES`] images are attached, or if any upload / fetch fails.
    pub async fn send(self) -> Result<create_record::Output> {
        let PostBuilder {
            ctx,
            text,
            reply,
            langs,
            images,
            video,
            external,
            quote,
        } = self;

        // A post's `embed` holds exactly one media kind. Reject ambiguous combos
        // up front rather than silently dropping one.
        let media_kinds = usize::from(!images.is_empty())
            + usize::from(video.is_some())
            + usize::from(external.is_some());
        if media_kinds > 1 {
            return Err(Error::invalid_input(
                "a post can carry only one media kind: images, video, or an external card",
            ));
        }
        if images.len() > MAX_IMAGES {
            return Err(Error::invalid_input(format!(
                "a post can carry at most {MAX_IMAGES} images (got {})",
                images.len()
            )));
        }

        let media = if !images.is_empty() {
            Some(Media::Images(upload_images(&ctx, images).await?))
        } else if let Some(v) = video {
            Some(Media::Video(upload_video(&ctx, v).await?))
        } else if let Some(ext) = external {
            Some(Media::External(build_external(&ctx, ext).await?))
        } else {
            None
        };

        let embed = assemble_embed(media, quote);

        let (text, facets) = if text.is_empty() {
            (String::new(), None)
        } else {
            let rich = RichText::new_with_detect_facets(&text).await?;
            (rich.text, rich.facets)
        };

        let record = post::RecordData {
            created_at: Datetime::now(),
            embed,
            entities: None,
            facets,
            labels: None,
            langs,
            reply,
            tags: None,
            text,
        };
        ctx.post_record(record).await
    }
}

// --- embed assembly (pure, unit-tested) -----------------------------------

/// Combine at most one media kind and an optional quote into a post's `embed`
/// union, choosing `record` vs `recordWithMedia` as appropriate. Returns `None`
/// for a plain text post.
fn assemble_embed(
    media: Option<Media>,
    quote: Option<strong_ref::Main>,
) -> Option<Union<post::RecordEmbedRefs>> {
    use post::RecordEmbedRefs as E;

    match (media, quote) {
        (None, None) => None,
        (Some(m), None) => Some(Union::Refs(match m {
            Media::Images(x) => E::AppBskyEmbedImagesMain(Box::new(x)),
            Media::Video(x) => E::AppBskyEmbedVideoMain(Box::new(x)),
            Media::External(x) => E::AppBskyEmbedExternalMain(Box::new(x)),
        })),
        (None, Some(r)) => Some(Union::Refs(E::AppBskyEmbedRecordMain(Box::new(
            record::MainData { record: r }.into(),
        )))),
        (Some(m), Some(r)) => {
            use record_with_media::MainMediaRefs as M;
            let media = Union::Refs(match m {
                Media::Images(x) => M::AppBskyEmbedImagesMain(Box::new(x)),
                Media::Video(x) => M::AppBskyEmbedVideoMain(Box::new(x)),
                Media::External(x) => M::AppBskyEmbedExternalMain(Box::new(x)),
            });
            let record = record::MainData { record: r }.into();
            Some(Union::Refs(E::AppBskyEmbedRecordWithMediaMain(Box::new(
                record_with_media::MainData { media, record }.into(),
            ))))
        }
    }
}

// --- images ----------------------------------------------------------------

async fn upload_images(ctx: &Context, imgs: Vec<PendingImage>) -> Result<images::Main> {
    let mut out = Vec::with_capacity(imgs.len());
    for img in imgs {
        let mime = img
            .mime
            .or_else(|| sniff_image_mime(&img.bytes).map(str::to_string));
        let mut blob = ctx.upload_blob(img.bytes).await?;
        // atrium uploads blobs with a `*/*` content type, so the mime a PDS
        // records is unreliable across implementations. Stamp the sniffed /
        // declared mime onto the ref so the embed renders everywhere.
        if let Some(m) = &mime {
            set_blob_mime(&mut blob, m);
        }
        out.push(
            images::ImageData {
                alt: img.alt,
                aspect_ratio: None,
                image: blob,
            }
            .into(),
        );
    }
    Ok(images::MainData { images: out }.into())
}

/// Sniff a supported image MIME type from magic bytes. Bluesky accepts PNG,
/// JPEG, GIF, and WebP; anything else returns `None`.
fn sniff_image_mime(bytes: &[u8]) -> Option<&'static str> {
    match bytes {
        [0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, ..] => Some("image/png"),
        [0xFF, 0xD8, 0xFF, ..] => Some("image/jpeg"),
        [b'G', b'I', b'F', b'8', ..] => Some("image/gif"),
        [
            b'R',
            b'I',
            b'F',
            b'F',
            _,
            _,
            _,
            _,
            b'W',
            b'E',
            b'B',
            b'P',
            ..,
        ] => Some("image/webp"),
        _ => None,
    }
}

/// Overwrite the recorded MIME type of a (typed) blob ref.
fn set_blob_mime(blob: &mut BlobRef, mime: &str) {
    if let BlobRef::Typed(TypedBlobRef::Blob(b)) = blob {
        b.mime_type = mime.to_string();
    }
}

// --- external link cards ---------------------------------------------------

async fn build_external(ctx: &Context, ext: PendingExternal) -> Result<external::Main> {
    let data = match ext {
        PendingExternal::Ready(data) => data,
        PendingExternal::Fetch(url) => fetch_link_card(ctx, url).await?,
    };
    Ok(external::MainData {
        external: data.into(),
    }
    .into())
}

async fn fetch_link_card(ctx: &Context, url: String) -> Result<external::ExternalData> {
    let http = http_client()?;
    let resp = http
        .get(&url)
        .send()
        .await
        .map_err(Error::http)?
        .error_for_status()
        .map_err(Error::http)?;
    let final_url = resp.url().to_string();
    let body = resp.text().await.map_err(Error::http)?;
    let head = &body[..body.len().min(LINK_CARD_HTML_LIMIT)];

    let meta = extract_link_meta(head);
    let uri = meta.url.clone().unwrap_or(final_url.clone());
    let title = meta.title.unwrap_or_default();
    let description = meta.description.unwrap_or_default();

    let thumb = match meta.image {
        Some(img) => {
            let img_url = resolve_url(&final_url, &img);
            // A missing / oversized / broken preview image must not fail the
            // whole card — fall back to a thumbnail-less card.
            match fetch_and_upload_thumb(ctx, &http, &img_url).await {
                Ok(blob) => Some(blob),
                Err(err) => {
                    tracing::debug!(url = %img_url, %err, "link-card thumbnail skipped");
                    None
                }
            }
        }
        None => None,
    };

    Ok(external::ExternalData {
        description,
        thumb,
        title,
        uri,
    })
}

async fn fetch_and_upload_thumb(
    ctx: &Context,
    http: &reqwest::Client,
    url: &str,
) -> Result<BlobRef> {
    let resp = http
        .get(url)
        .send()
        .await
        .map_err(Error::http)?
        .error_for_status()
        .map_err(Error::http)?;
    let bytes = resp.bytes().await.map_err(Error::http)?;
    if bytes.len() > LINK_CARD_THUMB_LIMIT {
        return Err(Error::http(format!(
            "link-card thumbnail is {} bytes, over the {LINK_CARD_THUMB_LIMIT}-byte limit",
            bytes.len()
        )));
    }
    let mime = sniff_image_mime(&bytes).map(str::to_string);
    let mut blob = ctx.upload_blob(bytes.to_vec()).await?;
    if let Some(m) = &mime {
        set_blob_mime(&mut blob, m);
    }
    Ok(blob)
}

/// Parsed OpenGraph / HTML metadata for a link card.
#[derive(Debug, Default, PartialEq, Eq)]
struct LinkMeta {
    title: Option<String>,
    description: Option<String>,
    image: Option<String>,
    url: Option<String>,
}

/// Extract link-card metadata from HTML, preferring OpenGraph, then Twitter
/// cards, then plain `<title>` / `<meta name=description>`.
fn extract_link_meta(html: &str) -> LinkMeta {
    let mut og_title = None;
    let mut og_desc = None;
    let mut og_image = None;
    let mut og_url = None;
    let mut tw_title = None;
    let mut tw_desc = None;
    let mut tw_image = None;
    let mut name_desc = None;

    // Walk every `<meta ...>` tag and bucket by its property/name key.
    let bytes = html.as_bytes();
    let mut i = 0;
    while let Some(rel) = html[i..].find("<meta") {
        let start = i + rel;
        let end = match html[start..].find('>') {
            Some(e) => start + e,
            None => break,
        };
        let tag = &html[start..end];
        let key = tag_attr(tag, "property")
            .or_else(|| tag_attr(tag, "name"))
            .map(|k| k.to_ascii_lowercase());
        if let Some(key) = key
            && let Some(content) = tag_attr(tag, "content")
        {
            let content = decode_entities(&content);
            // An empty `content` (e.g. `<meta property="og:title" content="">`,
            // which real sites do ship) must not shadow a later fallback.
            if content.trim().is_empty() {
                i = end + 1;
                continue;
            }
            match key.as_str() {
                "og:title" => og_title = og_title.or(Some(content)),
                "og:description" => og_desc = og_desc.or(Some(content)),
                "og:image" | "og:image:url" | "og:image:secure_url" => {
                    og_image = og_image.or(Some(content))
                }
                "og:url" => og_url = og_url.or(Some(content)),
                "twitter:title" => tw_title = tw_title.or(Some(content)),
                "twitter:description" => tw_desc = tw_desc.or(Some(content)),
                "twitter:image" | "twitter:image:src" => tw_image = tw_image.or(Some(content)),
                "description" => name_desc = name_desc.or(Some(content)),
                _ => {}
            }
        }
        i = end + 1;
        if i >= bytes.len() {
            break;
        }
    }

    let title = og_title.or(tw_title).or_else(|| extract_title_tag(html));
    let description = og_desc.or(tw_desc).or(name_desc);
    let image = og_image.or(tw_image);

    LinkMeta {
        title,
        description,
        image,
        url: og_url,
    }
}

/// Read the value of a single attribute out of one HTML tag, tolerating any
/// attribute order and single or double quotes.
fn tag_attr(tag: &str, attr: &str) -> Option<String> {
    let lower = tag.to_ascii_lowercase();
    let mut from = 0;
    loop {
        let rel = lower[from..].find(attr)?;
        let name_start = from + rel;
        // Ensure this is a whole attribute name, not a substring of another.
        let prev_ok = name_start == 0
            || !lower.as_bytes()[name_start - 1].is_ascii_alphanumeric()
                && lower.as_bytes()[name_start - 1] != b'-'
                && lower.as_bytes()[name_start - 1] != b':';
        let after = name_start + attr.len();
        let rest = lower[after..].trim_start();
        if prev_ok && rest.starts_with('=') {
            // Parse the value from the original (case-preserving) tag.
            let eq = tag[after..].find('=')? + after + 1;
            let value = tag[eq..].trim_start();
            let mut chars = value.char_indices();
            return match chars.next() {
                Some((_, q @ ('"' | '\''))) => {
                    let vstart = q.len_utf8();
                    let vend = value[vstart..].find(q)? + vstart;
                    Some(value[vstart..vend].to_string())
                }
                // Unquoted attribute value: read up to whitespace or `>`.
                Some(_) => {
                    let vend = value
                        .find(|c: char| c.is_whitespace() || c == '>')
                        .unwrap_or(value.len());
                    Some(value[..vend].to_string())
                }
                None => None,
            };
        }
        from = after;
    }
}

/// Extract the text of the document's `<title>` element, if any.
fn extract_title_tag(html: &str) -> Option<String> {
    let lower = html.to_ascii_lowercase();
    let open = lower.find("<title")?;
    let gt = lower[open..].find('>')? + open + 1;
    let close = lower[gt..].find("</title>")? + gt;
    // Collapse the runs of whitespace real pages pad their titles with.
    let text = html[gt..close]
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if text.is_empty() {
        None
    } else {
        Some(decode_entities(&text))
    }
}

/// Decode the handful of HTML entities that commonly appear in OpenGraph text.
fn decode_entities(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        let tail = &rest[amp..];
        let semi = tail.find(';').filter(|&p| p <= 10);
        match semi {
            Some(p) => {
                let entity = &tail[1..p];
                let decoded = match entity {
                    "amp" => Some('&'),
                    "lt" => Some('<'),
                    "gt" => Some('>'),
                    "quot" => Some('"'),
                    "apos" | "#39" | "#x27" | "#X27" => Some('\''),
                    "nbsp" => Some('\u{a0}'),
                    _ if entity.starts_with("#x") || entity.starts_with("#X") => {
                        u32::from_str_radix(&entity[2..], 16)
                            .ok()
                            .and_then(char::from_u32)
                    }
                    _ if entity.starts_with('#') => {
                        entity[1..].parse::<u32>().ok().and_then(char::from_u32)
                    }
                    _ => None,
                };
                match decoded {
                    Some(c) => {
                        out.push(c);
                        rest = &tail[p + 1..];
                    }
                    None => {
                        out.push('&');
                        rest = &tail[1..];
                    }
                }
            }
            None => {
                out.push('&');
                rest = &tail[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// Resolve a possibly-relative URL (e.g. an `og:image`) against the page URL.
fn resolve_url(base: &str, maybe_relative: &str) -> String {
    match reqwest::Url::parse(base).and_then(|b| b.join(maybe_relative)) {
        Ok(u) => u.to_string(),
        Err(_) => maybe_relative.to_string(),
    }
}

// --- video (via the Bluesky video service) --------------------------------

async fn upload_video(ctx: &Context, v: PendingVideo) -> Result<video::Main> {
    let blob = upload_video_blob(ctx, v.bytes).await?;
    Ok(video::MainData {
        alt: Some(v.alt),
        aspect_ratio: None,
        captions: None,
        video: blob,
    }
    .into())
}

/// Drive the video service: mint a service-auth token, upload the MP4, then poll
/// for the processed blob. Works on any PDS: the token's audience is the bot's
/// *own PDS DID* (`did:web:<pds-host>`) — which is what `video.bsky.app` requires
/// for third-party / self-hosted PDS accounts — rather than the video service.
async fn upload_video_blob(ctx: &Context, bytes: Vec<u8>) -> Result<BlobRef> {
    let did = ctx.did().to_string();
    let http = http_client()?;
    let pds_did = pds_service_did(ctx).await?;

    // The upload token is bound to `com.atproto.repo.uploadBlob` (the video
    // service accepts the same service-auth token shape the PDS uses for blobs),
    // not `app.bsky.video.uploadVideo`.
    let upload_token = service_auth(ctx, &pds_did, "com.atproto.repo.uploadBlob").await?;
    let name = content_name(&bytes);
    let upload_url =
        format!("{VIDEO_SERVICE_BASE}/xrpc/app.bsky.video.uploadVideo?did={did}&name={name}");

    let resp = http
        .post(&upload_url)
        .bearer_auth(&upload_token)
        .header(reqwest::header::CONTENT_TYPE, "video/mp4")
        .body(bytes)
        .send()
        .await
        .map_err(Error::http)?;

    // A 409 means this exact video was already uploaded; its job is still in the
    // body, so parse regardless of status and let the poll loop resolve it.
    let job_bytes = resp.bytes().await.map_err(Error::http)?;
    let mut job: video_defs::JobStatus = serde_json::from_slice(&job_bytes).map_err(|_| {
        Error::VideoUpload(format!(
            "unexpected response from video service: {}",
            String::from_utf8_lossy(&job_bytes)
                .chars()
                .take(200)
                .collect::<String>()
        ))
    })?;

    if let Some(blob) = finished_blob(&job)? {
        return Ok(blob);
    }

    let status_token = service_auth(ctx, &pds_did, "app.bsky.video.getJobStatus").await?;
    for _ in 0..VIDEO_POLL_ATTEMPTS {
        tokio::time::sleep(VIDEO_POLL_INTERVAL).await;
        let status_url = format!(
            "{VIDEO_SERVICE_BASE}/xrpc/app.bsky.video.getJobStatus?jobId={}",
            job.job_id
        );
        let resp = http
            .get(&status_url)
            .bearer_auth(&status_token)
            .send()
            .await
            .map_err(Error::http)?;
        let body = resp.bytes().await.map_err(Error::http)?;
        let wrapper: JobStatusResponse = serde_json::from_slice(&body).map_err(|_| {
            Error::VideoUpload(format!(
                "unexpected job-status response: {}",
                String::from_utf8_lossy(&body)
                    .chars()
                    .take(200)
                    .collect::<String>()
            ))
        })?;
        job = wrapper.job_status;
        if let Some(blob) = finished_blob(&job)? {
            return Ok(blob);
        }
    }

    Err(Error::VideoUpload(format!(
        "video processing did not complete within {}s",
        VIDEO_POLL_ATTEMPTS as u64 * VIDEO_POLL_INTERVAL.as_secs()
    )))
}

/// The `getJobStatus` response wraps the status in a `jobStatus` field.
#[derive(serde::Deserialize)]
struct JobStatusResponse {
    #[serde(rename = "jobStatus")]
    job_status: video_defs::JobStatus,
}

/// Inspect a job status: `Ok(Some(blob))` when finished successfully, `Ok(None)`
/// while still processing, `Err` when the service reports failure.
fn finished_blob(job: &video_defs::JobStatus) -> Result<Option<BlobRef>> {
    match job.state.as_str() {
        "JOB_STATE_COMPLETED" => job.blob.clone().map(Some).ok_or_else(|| {
            Error::VideoUpload("video job completed but returned no blob".to_string())
        }),
        "JOB_STATE_FAILED" => Err(Error::VideoUpload(
            job.error
                .clone()
                .or_else(|| job.message.clone())
                .unwrap_or_else(|| "video processing failed".to_string()),
        )),
        // Any other state (encoding, scanning, …) means keep waiting. Some jobs
        // also carry a finished blob early; surface it if present.
        _ => Ok(job.blob.clone()),
    }
}

/// Derive the bot's PDS service DID (`did:web:<host>`) from the agent's current
/// endpoint. This is the audience the video service expects on upload tokens.
async fn pds_service_did(ctx: &Context) -> Result<String> {
    let endpoint = ctx.agent().get_endpoint().await;
    let host = reqwest::Url::parse(&endpoint)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .ok_or_else(|| {
            Error::invalid_input(format!("cannot derive PDS DID from endpoint {endpoint:?}"))
        })?;
    Ok(format!("did:web:{host}"))
}

/// Request a service-auth token for `aud`, bound to one video-service method.
async fn service_auth(ctx: &Context, aud: &str, lxm: &str) -> Result<String> {
    let params = get_service_auth::ParametersData {
        aud: aud.parse::<Did>().map_err(Error::invalid_input)?,
        exp: None,
        lxm: Some(lxm.parse::<Nsid>().map_err(Error::invalid_input)?),
    }
    .into();
    let out = ctx
        .agent()
        .api
        .com
        .atproto
        .server
        .get_service_auth(params)
        .await?;
    Ok(out.data.token)
}

/// A deterministic, content-derived filename for the video upload (FNV-1a). Same
/// bytes → same name, so a retried upload maps to the same job.
fn content_name(bytes: &[u8]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}.mp4")
}

// --- shared HTTP client ----------------------------------------------------

fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(concat!("bsky-bot-sdk/", env!("CARGO_PKG_VERSION")))
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(Error::http)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- fixtures ----------------------------------------------------------

    fn blob(mime: &str) -> BlobRef {
        let v = serde_json::json!({
            "$type": "blob",
            "ref": { "$link": "bafkreibme22gw2h7y2h7tg2fhqotaqjucnbc24deqo72b6mkl2egezxhvy" },
            "mimeType": mime,
            "size": 12345,
        });
        serde_json::from_value(v).expect("valid blob fixture")
    }

    fn strong() -> strong_ref::Main {
        strong_ref::MainData {
            cid: "bafyreiclp443lavogvhj3d2ob2cxbfuscni2k5jk7bebjzg7khl3esabwq"
                .parse()
                .expect("valid cid"),
            uri: "at://did:plc:alice000000000000000000/app.bsky.feed.post/abc".to_string(),
        }
        .into()
    }

    fn one_image() -> images::Main {
        images::MainData {
            images: vec![
                images::ImageData {
                    alt: "a described image".to_string(),
                    aspect_ratio: None,
                    image: blob("image/png"),
                }
                .into(),
            ],
        }
        .into()
    }

    fn one_video() -> video::Main {
        video::MainData {
            alt: Some("a described video".to_string()),
            aspect_ratio: None,
            captions: None,
            video: blob("video/mp4"),
        }
        .into()
    }

    fn one_external() -> external::Main {
        external::MainData {
            external: external::ExternalData {
                description: "d".to_string(),
                thumb: None,
                title: "t".to_string(),
                uri: "https://example.com".to_string(),
            }
            .into(),
        }
        .into()
    }

    // --- embed assembly: one behavior per test -----------------------------

    #[test]
    fn plain_post_has_no_embed() {
        assert!(assemble_embed(None, None).is_none());
    }

    #[test]
    fn images_only_yields_images_embed() {
        let embed = assemble_embed(Some(Media::Images(one_image())), None).unwrap();
        assert!(matches!(
            embed,
            Union::Refs(post::RecordEmbedRefs::AppBskyEmbedImagesMain(_))
        ));
    }

    #[test]
    fn video_only_yields_video_embed() {
        let embed = assemble_embed(Some(Media::Video(one_video())), None).unwrap();
        assert!(matches!(
            embed,
            Union::Refs(post::RecordEmbedRefs::AppBskyEmbedVideoMain(_))
        ));
    }

    #[test]
    fn external_only_yields_external_embed() {
        let embed = assemble_embed(Some(Media::External(one_external())), None).unwrap();
        assert!(matches!(
            embed,
            Union::Refs(post::RecordEmbedRefs::AppBskyEmbedExternalMain(_))
        ));
    }

    #[test]
    fn quote_only_yields_record_embed() {
        let embed = assemble_embed(None, Some(strong())).unwrap();
        match embed {
            Union::Refs(post::RecordEmbedRefs::AppBskyEmbedRecordMain(r)) => {
                assert_eq!(r.record.uri, strong().uri);
            }
            _ => panic!("expected a record embed"),
        }
    }

    #[test]
    fn quote_plus_images_yields_record_with_media() {
        let embed = assemble_embed(Some(Media::Images(one_image())), Some(strong())).unwrap();
        match embed {
            Union::Refs(post::RecordEmbedRefs::AppBskyEmbedRecordWithMediaMain(rwm)) => {
                assert_eq!(rwm.record.record.uri, strong().uri);
                assert!(matches!(
                    rwm.media,
                    Union::Refs(record_with_media::MainMediaRefs::AppBskyEmbedImagesMain(_))
                ));
            }
            _ => panic!("expected recordWithMedia"),
        }
    }

    #[test]
    fn quote_plus_external_yields_record_with_media() {
        let embed = assemble_embed(Some(Media::External(one_external())), Some(strong())).unwrap();
        match embed {
            Union::Refs(post::RecordEmbedRefs::AppBskyEmbedRecordWithMediaMain(rwm)) => {
                assert!(matches!(
                    rwm.media,
                    Union::Refs(record_with_media::MainMediaRefs::AppBskyEmbedExternalMain(
                        _
                    ))
                ));
            }
            _ => panic!("expected recordWithMedia"),
        }
    }

    // --- mime sniffing -----------------------------------------------------

    #[test]
    fn sniffs_common_image_formats() {
        assert_eq!(
            sniff_image_mime(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 0, 0]),
            Some("image/png")
        );
        assert_eq!(
            sniff_image_mime(&[0xFF, 0xD8, 0xFF, 0xE0]),
            Some("image/jpeg")
        );
        assert_eq!(sniff_image_mime(b"GIF89a....."), Some("image/gif"));
        assert_eq!(
            sniff_image_mime(b"RIFF\0\0\0\0WEBPVP8 "),
            Some("image/webp")
        );
    }

    #[test]
    fn unknown_bytes_sniff_to_none() {
        assert_eq!(sniff_image_mime(b"not an image"), None);
        assert_eq!(sniff_image_mime(&[]), None);
    }

    #[test]
    fn blob_mime_is_overwritten() {
        let mut b = blob("*/*");
        set_blob_mime(&mut b, "image/png");
        match b {
            BlobRef::Typed(TypedBlobRef::Blob(inner)) => assert_eq!(inner.mime_type, "image/png"),
            _ => panic!("expected a typed blob"),
        }
    }

    // --- OpenGraph parsing -------------------------------------------------

    #[test]
    fn parses_standard_open_graph() {
        let html = r#"<html><head>
            <meta property="og:title" content="Hello World">
            <meta property="og:description" content="A test page">
            <meta property="og:image" content="https://cdn.example.com/img.png">
            <meta property="og:url" content="https://example.com/canonical">
        </head></html>"#;
        let meta = extract_link_meta(html);
        assert_eq!(meta.title.as_deref(), Some("Hello World"));
        assert_eq!(meta.description.as_deref(), Some("A test page"));
        assert_eq!(
            meta.image.as_deref(),
            Some("https://cdn.example.com/img.png")
        );
        assert_eq!(meta.url.as_deref(), Some("https://example.com/canonical"));
    }

    #[test]
    fn parses_reversed_attribute_order() {
        let html =
            r#"<meta content="Reversed" property="og:title"><meta content="d" name="description">"#;
        let meta = extract_link_meta(html);
        assert_eq!(meta.title.as_deref(), Some("Reversed"));
        assert_eq!(meta.description.as_deref(), Some("d"));
    }

    #[test]
    fn single_quotes_and_entities_are_handled() {
        let html = r#"<meta property='og:title' content='Ben &amp; Jerry&#39;s'>"#;
        let meta = extract_link_meta(html);
        assert_eq!(meta.title.as_deref(), Some("Ben & Jerry's"));
    }

    #[test]
    fn falls_back_to_title_tag_and_name_description() {
        let html = r#"<head><title>Plain Title</title>
            <meta name="description" content="plain desc"></head>"#;
        let meta = extract_link_meta(html);
        assert_eq!(meta.title.as_deref(), Some("Plain Title"));
        assert_eq!(meta.description.as_deref(), Some("plain desc"));
        assert_eq!(meta.image, None);
    }

    #[test]
    fn open_graph_wins_over_twitter_and_title() {
        let html = r#"<head><title>Title Tag</title>
            <meta name="twitter:title" content="Twitter Title">
            <meta property="og:title" content="OG Title"></head>"#;
        let meta = extract_link_meta(html);
        assert_eq!(meta.title.as_deref(), Some("OG Title"));
    }

    #[test]
    fn twitter_card_used_when_no_open_graph() {
        let html = r#"<meta name="twitter:image" content="https://x.example/a.jpg">"#;
        let meta = extract_link_meta(html);
        assert_eq!(meta.image.as_deref(), Some("https://x.example/a.jpg"));
    }

    #[test]
    fn empty_content_falls_through_to_fallback() {
        // Real pages (e.g. rust-lang.org) ship an empty og:title; the `<title>`
        // must win rather than the card ending up titleless.
        let html = r#"<head>
            <meta property="og:title" content="">
            <meta property="og:description" content="the real description">
            <title>   Real   Title   </title></head>"#;
        let meta = extract_link_meta(html);
        assert_eq!(meta.title.as_deref(), Some("Real Title"));
        assert_eq!(meta.description.as_deref(), Some("the real description"));
    }

    #[test]
    fn title_tag_whitespace_is_collapsed() {
        let html = "<title>\n    Rust    Programming\n  Language  </title>";
        assert_eq!(
            extract_title_tag(html).as_deref(),
            Some("Rust Programming Language")
        );
    }

    #[test]
    fn attribute_key_substrings_do_not_false_match() {
        // `data-name` must not satisfy a lookup for `name`.
        let html = r#"<meta data-name="og:title" content="ignored"><title>Real</title>"#;
        let meta = extract_link_meta(html);
        assert_eq!(meta.title.as_deref(), Some("Real"));
    }

    // --- entity decoding ---------------------------------------------------

    #[test]
    fn decodes_named_and_numeric_entities() {
        assert_eq!(decode_entities("a &amp; b"), "a & b");
        assert_eq!(decode_entities("&lt;tag&gt;"), "<tag>");
        assert_eq!(decode_entities("&#39;q&quot;"), "'q\"");
        assert_eq!(decode_entities("&#x41;"), "A");
        assert_eq!(decode_entities("no entities"), "no entities");
        // A bare ampersand is preserved, not swallowed.
        assert_eq!(decode_entities("Tom & Jerry"), "Tom & Jerry");
    }

    // --- relative URL resolution -------------------------------------------

    #[test]
    fn resolves_relative_image_urls() {
        assert_eq!(
            resolve_url("https://example.com/page/", "/img.png"),
            "https://example.com/img.png"
        );
        assert_eq!(
            resolve_url("https://example.com/a/b", "c.png"),
            "https://example.com/a/c.png"
        );
        assert_eq!(
            resolve_url("https://example.com/", "https://cdn.example.com/x.png"),
            "https://cdn.example.com/x.png"
        );
    }

    // --- video helpers -----------------------------------------------------

    #[test]
    fn content_name_is_deterministic_and_content_addressed() {
        assert_eq!(content_name(b"abc"), content_name(b"abc"));
        assert_ne!(content_name(b"abc"), content_name(b"abd"));
        assert!(content_name(b"abc").ends_with(".mp4"));
    }

    #[test]
    fn completed_job_yields_blob() {
        let job: video_defs::JobStatus = serde_json::from_value(serde_json::json!({
            "jobId": "j1",
            "did": "did:plc:alice000000000000000000",
            "state": "JOB_STATE_COMPLETED",
            "blob": { "$type": "blob",
                "ref": { "$link": "bafkreibme22gw2h7y2h7tg2fhqotaqjucnbc24deqo72b6mkl2egezxhvy" },
                "mimeType": "video/mp4", "size": 999 },
        }))
        .unwrap();
        assert!(finished_blob(&job).unwrap().is_some());
    }

    #[test]
    fn failed_job_is_an_error() {
        let job: video_defs::JobStatus = serde_json::from_value(serde_json::json!({
            "jobId": "j1",
            "did": "did:plc:alice000000000000000000",
            "state": "JOB_STATE_FAILED",
            "error": "too big",
        }))
        .unwrap();
        assert!(finished_blob(&job).is_err());
    }

    #[test]
    fn in_progress_job_is_pending() {
        let job: video_defs::JobStatus = serde_json::from_value(serde_json::json!({
            "jobId": "j1",
            "did": "did:plc:alice000000000000000000",
            "state": "JOB_STATE_ENCODING",
        }))
        .unwrap();
        assert!(finished_blob(&job).unwrap().is_none());
    }
}
