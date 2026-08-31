use serde_json::Value;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

const MAX_DEPTH: usize = 32;
const MAX_BLOCKS: usize = 10_000;
const MAX_PAGES: usize = 10_000;
const MAX_CURSOR_BYTES: usize = 512;
const MAX_BLOCK_ID_BYTES: usize = 128;
const MAX_RENDERED_BYTES: usize = 1024 * 1024;
const MAX_SOURCE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct BlockPage {
    pub(crate) blocks: Vec<Value>,
    pub(crate) next_cursor: Option<String>,
    pub(crate) response_bytes: usize,
}

pub(crate) trait BlockSource: Send + Sync {
    fn block_page<'a>(
        &'a self,
        parent_id: &'a str,
        cursor: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<BlockPage, String>> + Send + 'a>>;
}

pub(crate) async fn render_page(source: &dyn BlockSource, page_id: &str) -> Result<String, String> {
    validate_id(page_id)?;
    let mut output = String::new();
    let mut stack = vec![Frame::new(page_id.to_owned(), 0, String::new())];
    let mut block_count = 0_usize;
    let mut page_count = 0_usize;
    let mut source_bytes = 0_usize;
    let mut traversed = HashSet::from([page_id.to_owned()]);

    while let Some(frame) = stack.last_mut() {
        if frame.index == frame.blocks.len() {
            if frame.finished {
                stack.pop();
                continue;
            }
            page_count += 1;
            if page_count > MAX_PAGES {
                return Err(limit_error());
            }
            let page = source
                .block_page(&frame.parent_id, frame.cursor.as_deref())
                .await?;
            source_bytes = source_bytes.saturating_add(page.response_bytes);
            if source_bytes > MAX_SOURCE_BYTES {
                return Err(limit_error());
            }
            if let Some(cursor) = &page.next_cursor {
                if cursor.is_empty()
                    || cursor.len() > MAX_CURSOR_BYTES
                    || !frame.cursors.insert(cursor.clone())
                {
                    return Err("Notion returned an invalid block cursor".to_owned());
                }
            }
            frame.cursor = page.next_cursor;
            frame.finished = frame.cursor.is_none();
            frame.blocks = page.blocks;
            frame.index = 0;
            continue;
        }

        let block = frame.blocks[frame.index].clone();
        frame.index += 1;
        block_count += 1;
        if block_count > MAX_BLOCKS {
            return Err(limit_error());
        }
        let rendered = render_block(&block, &frame.indent)?;
        append_bounded(&mut output, &rendered)?;

        if should_traverse(&block)? {
            let id = required_string(&block, "id")?;
            validate_id(id)?;
            if !traversed.insert(id.to_owned()) {
                return Err("Notion task content contains a traversal cycle".to_owned());
            }
            let depth = frame.depth + 1;
            if depth > MAX_DEPTH {
                return Err(limit_error());
            }
            let child = Frame::new(id.to_owned(), depth, child_indent(&block, &frame.indent)?);
            stack.push(child);
        }
    }
    Ok(output.trim_end().to_owned())
}

struct Frame {
    parent_id: String,
    depth: usize,
    indent: String,
    cursor: Option<String>,
    cursors: HashSet<String>,
    blocks: Vec<Value>,
    index: usize,
    finished: bool,
}

impl Frame {
    fn new(parent_id: String, depth: usize, indent: String) -> Self {
        Self {
            parent_id,
            depth,
            indent,
            cursor: None,
            cursors: HashSet::new(),
            blocks: Vec::new(),
            index: 0,
            finished: false,
        }
    }
}

fn should_traverse(block: &Value) -> Result<bool, String> {
    let has_children = block
        .get("has_children")
        .and_then(Value::as_bool)
        .ok_or_else(|| "Notion returned an invalid block response".to_owned())?;
    if !has_children {
        return Ok(false);
    }
    let kind = required_string(block, "type")?;
    Ok(matches!(
        kind,
        "paragraph"
            | "heading_1"
            | "heading_2"
            | "heading_3"
            | "heading_4"
            | "bulleted_list_item"
            | "numbered_list_item"
            | "to_do"
            | "toggle"
            | "quote"
            | "callout"
            | "column_list"
            | "column"
            | "synced_block"
            | "table"
            | "template"
            | "meeting_notes"
            | "tab"
    ))
}

fn child_indent(block: &Value, current: &str) -> Result<String, String> {
    let kind = required_string(block, "type")?;
    if matches!(kind, "bulleted_list_item" | "to_do" | "toggle") {
        Ok(format!("{current}  "))
    } else if kind == "numbered_list_item" {
        Ok(format!("{current}   "))
    } else if matches!(kind, "quote" | "callout") {
        Ok(format!("{current}> "))
    } else {
        Ok(current.to_owned())
    }
}

fn render_block(block: &Value, indent: &str) -> Result<String, String> {
    if block.get("object").and_then(Value::as_str) != Some("block") {
        return Err("Notion returned an invalid block response".to_owned());
    }
    let kind = required_string(block, "type")?;
    let body = block.get(kind).and_then(Value::as_object);
    let rich = |field: &str| -> Result<String, String> {
        let values = body
            .and_then(|value| value.get(field))
            .and_then(Value::as_array)
            .ok_or_else(|| "Notion returned an invalid block response".to_owned())?;
        render_rich_text(values)
    };
    let markdown = match kind {
        "paragraph" => format!("{}\n\n", rich("rich_text")?),
        "heading_1" => format!("# {}\n\n", rich("rich_text")?),
        "heading_2" => format!("## {}\n\n", rich("rich_text")?),
        "heading_3" => format!("### {}\n\n", rich("rich_text")?),
        "heading_4" => format!("#### {}\n\n", rich("rich_text")?),
        "bulleted_list_item" => format!("- {}\n", continue_lines("  ", &rich("rich_text")?)),
        "numbered_list_item" => format!("1. {}\n", continue_lines("   ", &rich("rich_text")?)),
        "to_do" => {
            let checked = body
                .and_then(|value| value.get("checked"))
                .and_then(Value::as_bool)
                .ok_or_else(|| "Notion returned an invalid block response".to_owned())?;
            format!(
                "- [{}] {}\n",
                if checked { "x" } else { " " },
                continue_lines("  ", &rich("rich_text")?)
            )
        }
        "toggle" => format!(
            "- **Toggle:** {}\n",
            continue_lines("  ", &rich("rich_text")?)
        ),
        "quote" => format!("> {}\n", continue_lines("> ", &rich("rich_text")?)),
        "callout" => format!("> {}\n> \n", continue_lines("> ", &rich("rich_text")?)),
        "code" => {
            let language = body
                .and_then(|value| value.get("language"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let content = rich_plain(body, "rich_text")?;
            let fence = code_fence(&content);
            format!(
                "{fence}{}\n{}\n{fence}\n\n",
                escape_code_info(language),
                content
            )
        }
        "equation" => format!(
            "$$\n{}\n$$\n\n",
            body.and_then(|v| v.get("expression"))
                .and_then(Value::as_str)
                .ok_or_else(|| "Notion returned an invalid block response".to_owned())?
        ),
        "divider" => "---\n\n".to_owned(),
        "table_row" => {
            let cells = body
                .and_then(|value| value.get("cells"))
                .and_then(Value::as_array)
                .ok_or_else(|| "Notion returned an invalid block response".to_owned())?;
            let rendered = cells
                .iter()
                .map(|cell| {
                    cell.as_array()
                        .ok_or_else(|| "Notion returned an invalid block response".to_owned())
                        .and_then(|parts| render_rich_text(parts))
                        .map(|value| value.replace("\r\n", "<br>").replace(['\r', '\n'], "<br>"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            format!("- **Table row:** {}\n", rendered.join(" · "))
        }
        "child_page" => format!("[Child page: {}]\n\n", safe_label(body, "title")),
        "child_database" => format!("[Child database: {}]\n\n", safe_label(body, "title")),
        "link_to_page" => "[Linked Notion page]\n\n".to_owned(),
        "bookmark" | "embed" | "file" | "image" | "video" | "audio" | "pdf" | "link_preview" => {
            format!("[Attachment: {kind}]\n\n")
        }
        "unsupported" => format!(
            "[Unsupported block: {}]\n\n",
            safe_label(body, "block_type")
        ),
        "column_list" | "column" | "synced_block" | "table" | "template" | "tab" => String::new(),
        "breadcrumb" => "[Breadcrumb]\n\n".to_owned(),
        "table_of_contents" => "[Table of contents]\n\n".to_owned(),
        "meeting_notes" => {
            let title = body
                .and_then(|value| value.get("title"))
                .and_then(Value::as_array)
                .map(|parts| render_rich_text(parts))
                .transpose()?
                .filter(|title| !title.is_empty());
            match title {
                Some(title) => format!("[Meeting notes: {title}]\n\n"),
                None => "[Meeting notes]\n\n".to_owned(),
            }
        }
        other => format!("[Unsupported block: {}]\n\n", escape_markdown(other)),
    };
    Ok(prefix_lines(indent, &markdown))
}

fn prefix_lines(prefix: &str, value: &str) -> String {
    if prefix.is_empty() || value.is_empty() {
        return value.to_owned();
    }
    let mut output = String::with_capacity(value.len() + prefix.len());
    output.push_str(prefix);
    for (index, line) in value.split_inclusive('\n').enumerate() {
        if index > 0 {
            output.push_str(prefix);
        }
        output.push_str(line);
    }
    output
}

fn continue_lines(prefix: &str, value: &str) -> String {
    if prefix.is_empty() || value.is_empty() {
        return value.to_owned();
    }
    let mut output = String::with_capacity(value.len() + prefix.len());
    for (index, line) in value.split_inclusive('\n').enumerate() {
        if index > 0 {
            output.push_str(prefix);
        }
        output.push_str(line);
    }
    output
}

fn render_rich_text(parts: &[Value]) -> Result<String, String> {
    parts.iter().map(render_rich_part).collect()
}

fn render_rich_part(part: &Value) -> Result<String, String> {
    let kind = required_string(part, "type")?;
    let plain = required_string(part, "plain_text")?;
    let mut text = if kind == "mention" {
        format!("[Mention: {}]", escape_markdown(plain))
    } else if kind == "equation" {
        format!("${}$", plain.replace('$', "\\$"))
    } else {
        escape_markdown(plain)
    };
    let annotations = part.get("annotations").and_then(Value::as_object);
    if annotations
        .and_then(|a| a.get("code"))
        .and_then(Value::as_bool)
        == Some(true)
    {
        text = code_span(plain);
    } else {
        if annotations
            .and_then(|a| a.get("bold"))
            .and_then(Value::as_bool)
            == Some(true)
        {
            text = format!("**{text}**");
        }
        if annotations
            .and_then(|a| a.get("italic"))
            .and_then(Value::as_bool)
            == Some(true)
        {
            text = format!("*{text}*");
        }
        if annotations
            .and_then(|a| a.get("strikethrough"))
            .and_then(Value::as_bool)
            == Some(true)
        {
            text = format!("~~{text}~~");
        }
        if annotations
            .and_then(|a| a.get("underline"))
            .and_then(Value::as_bool)
            == Some(true)
        {
            text = format!("<u>{text}</u>");
        }
    }
    if kind == "text" {
        if let Some(href) = part
            .get("href")
            .and_then(Value::as_str)
            .filter(|value| safe_url(value))
        {
            text = format!("[{text}]({})", href.replace(')', "%29"));
        }
    }
    Ok(text)
}

fn rich_plain(
    body: Option<&serde_json::Map<String, Value>>,
    field: &str,
) -> Result<String, String> {
    body.and_then(|value| value.get(field))
        .and_then(Value::as_array)
        .ok_or_else(|| "Notion returned an invalid block response".to_owned())?
        .iter()
        .map(|part| required_string(part, "plain_text").map(str::to_owned))
        .collect()
}

fn required_string<'a>(value: &'a Value, field: &str) -> Result<&'a str, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| "Notion returned an invalid block response".to_owned())
}

fn safe_label(body: Option<&serde_json::Map<String, Value>>, field: &str) -> String {
    body.and_then(|value| value.get(field))
        .and_then(Value::as_str)
        .map(escape_markdown)
        .unwrap_or_else(|| "untitled".to_owned())
}

fn validate_id(value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > MAX_BLOCK_ID_BYTES || !value.is_ascii() {
        Err("Notion returned an invalid block response".to_owned())
    } else {
        Ok(())
    }
}

fn safe_url(value: &str) -> bool {
    value.len() <= 2048
        && (value.starts_with("https://") || value.starts_with("http://"))
        && !value.chars().any(char::is_whitespace)
}

fn escape_markdown(value: &str) -> String {
    value
        .chars()
        .flat_map(|c| {
            if "\\`*_{}[]<>()#+-.!|>".contains(c) {
                vec!['\\', c]
            } else {
                vec![c]
            }
        })
        .collect()
}

fn escape_code_info(value: &str) -> String {
    value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '_' | '.'))
        .take(64)
        .collect()
}

fn code_fence(content: &str) -> String {
    "`".repeat(longest_backtick_run(content).saturating_add(1).max(3))
}

fn code_span(content: &str) -> String {
    let delimiter = "`".repeat(longest_backtick_run(content).saturating_add(1).max(1));
    let touches_boundary = content.starts_with('`')
        || content.starts_with(' ')
        || content.ends_with('`')
        || content.ends_with(' ');
    let needs_padding = touches_boundary && !content.chars().all(|character| character == ' ');
    if needs_padding {
        format!("{delimiter} {content} {delimiter}")
    } else {
        format!("{delimiter}{content}{delimiter}")
    }
}

fn longest_backtick_run(content: &str) -> usize {
    content
        .chars()
        .fold((0_usize, 0_usize), |(longest, current), character| {
            if character == '`' {
                (longest.max(current + 1), current + 1)
            } else {
                (longest, 0)
            }
        })
        .0
}

fn append_bounded(output: &mut String, value: &str) -> Result<(), String> {
    if output.len().saturating_add(value.len()) > MAX_RENDERED_BYTES {
        return Err(limit_error());
    }
    output.push_str(value);
    Ok(())
}

fn limit_error() -> String {
    "Notion task content exceeds the traversal limit".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct FakeBlocks {
        pages: HashMap<(String, Option<String>), BlockPage>,
        calls: Mutex<Vec<(String, Option<String>)>>,
    }

    impl BlockSource for FakeBlocks {
        fn block_page<'a>(
            &'a self,
            parent_id: &'a str,
            cursor: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = Result<BlockPage, String>> + Send + 'a>> {
            Box::pin(async move {
                let key = (parent_id.to_owned(), cursor.map(str::to_owned));
                self.calls.lock().unwrap().push(key.clone());
                self.pages
                    .get(&key)
                    .map(|value| BlockPage {
                        blocks: value.blocks.clone(),
                        next_cursor: value.next_cursor.clone(),
                        response_bytes: value.response_bytes,
                    })
                    .ok_or_else(|| "unexpected fake block request".to_owned())
            })
        }
    }

    fn fake(entries: Vec<((&str, Option<&str>), BlockPage)>) -> FakeBlocks {
        FakeBlocks {
            pages: entries
                .into_iter()
                .map(|((id, cursor), value)| ((id.to_owned(), cursor.map(str::to_owned)), value))
                .collect(),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn page(blocks: Vec<Value>, cursor: Option<&str>) -> BlockPage {
        BlockPage {
            blocks,
            next_cursor: cursor.map(str::to_owned),
            response_bytes: 1,
        }
    }

    fn text(value: &str) -> Value {
        json!({
            "type": "text", "plain_text": value, "href": null,
            "annotations": {"bold": false, "italic": false, "strikethrough": false, "underline": false, "code": false}
        })
    }

    fn rich(id: &str, kind: &str, value: &str, children: bool) -> Value {
        let mut block = json!({
            "object": "block", "id": id, "type": kind, "has_children": children
        });
        block[kind] = json!({"rich_text": [text(value)]});
        block
    }

    #[tokio::test]
    async fn paginates_nested_children_in_stable_depth_first_order() {
        let source = fake(vec![
            (
                ("page-placeholder", None),
                page(
                    vec![rich("parent-placeholder", "paragraph", "parent", true)],
                    Some("root-cursor"),
                ),
            ),
            (
                ("page-placeholder", Some("root-cursor")),
                page(
                    vec![rich("last-placeholder", "paragraph", "last", false)],
                    None,
                ),
            ),
            (
                ("parent-placeholder", None),
                page(
                    vec![rich("one-placeholder", "bulleted_list_item", "one", false)],
                    Some("child-cursor"),
                ),
            ),
            (
                ("parent-placeholder", Some("child-cursor")),
                page(
                    vec![rich("two-placeholder", "bulleted_list_item", "two", false)],
                    None,
                ),
            ),
        ]);

        assert_eq!(
            render_page(&source, "page-placeholder").await.unwrap(),
            "parent\n\n- one\n- two\nlast"
        );
        assert_eq!(
            *source.calls.lock().unwrap(),
            vec![
                ("page-placeholder".to_owned(), None),
                ("parent-placeholder".to_owned(), None),
                (
                    "parent-placeholder".to_owned(),
                    Some("child-cursor".to_owned())
                ),
                (
                    "page-placeholder".to_owned(),
                    Some("root-cursor".to_owned())
                ),
            ]
        );
    }

    #[tokio::test]
    async fn renders_representative_blocks_links_annotations_and_mentions() {
        let styled = json!({
            "type":"text", "plain_text":"bold", "href":"https://example.invalid/path",
            "annotations":{"bold":true,"italic":false,"strikethrough":false,"underline":false,"code":false}
        });
        let mention = json!({
            "type":"mention", "plain_text":"Page placeholder", "href":"https://notion.so/private-placeholder",
            "mention":{"type":"page","page":{"id":"private-placeholder"}},
            "annotations":{"bold":false,"italic":false,"strikethrough":false,"underline":false,"code":false}
        });
        let source = fake(vec![(
            ("page-placeholder", None),
            page(
                vec![
                    rich("heading-placeholder", "heading_2", "Plan", false),
                    json!({"object":"block","id":"paragraph-placeholder","type":"paragraph","has_children":false,"paragraph":{"rich_text":[styled, mention]}}),
                    json!({"object":"block","id":"todo-placeholder","type":"to_do","has_children":false,"to_do":{"rich_text":[text("ship")],"checked":true}}),
                    json!({"object":"block","id":"code-placeholder","type":"code","has_children":false,"code":{"rich_text":[text("let x = 1;")],"language":"rust"}}),
                    json!({"object":"block","id":"divider-placeholder","type":"divider","has_children":false,"divider":{}}),
                ],
                None,
            ),
        )]);

        assert_eq!(
            render_page(&source, "page-placeholder").await.unwrap(),
            "## Plan\n\n[**bold**](https://example.invalid/path)[Mention: Page placeholder]\n\n- [x] ship\n```rust\nlet x = 1;\n```\n\n---"
        );
    }

    #[tokio::test]
    async fn represents_non_traversed_content_without_fetching_targets() {
        let source = fake(vec![(
            ("page-placeholder", None),
            page(
                vec![
                    json!({"object":"block","id":"child-placeholder","type":"child_page","has_children":true,"child_page":{"title":"Linked page"}}),
                    json!({"object":"block","id":"file-placeholder","type":"file","has_children":true,"file":{"file":{"url":"https://private.invalid/signed"}}}),
                    json!({"object":"block","id":"link-placeholder","type":"link_to_page","has_children":true,"link_to_page":{"page_id":"private-page-placeholder"}}),
                    json!({"object":"block","id":"unsupported-placeholder","type":"unsupported","has_children":true,"unsupported":{"block_type":"button"}}),
                ],
                None,
            ),
        )]);

        assert_eq!(
            render_page(&source, "page-placeholder").await.unwrap(),
            "[Child page: Linked page]\n\n[Attachment: file]\n\n[Linked Notion page]\n\n[Unsupported block: button]"
        );
        assert_eq!(source.calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn traverses_meeting_note_children_without_following_pointer_fields() {
        let source = fake(vec![
            (
                ("page-placeholder", None),
                page(
                    vec![json!({
                        "object":"block", "id":"meeting-placeholder", "type":"meeting_notes", "has_children":true,
                        "meeting_notes": {
                            "title": [text("Standup")],
                            "children": {"summary_block_id":"unfetched-summary-placeholder"}
                        }
                    })],
                    None,
                ),
            ),
            (
                ("meeting-placeholder", None),
                page(
                    vec![rich("note-placeholder", "paragraph", "first note", false)],
                    None,
                ),
            ),
        ]);

        assert_eq!(
            render_page(&source, "page-placeholder").await.unwrap(),
            "[Meeting notes: Standup]\n\nfirst note"
        );
        assert_eq!(
            *source.calls.lock().unwrap(),
            vec![
                ("page-placeholder".to_owned(), None),
                ("meeting-placeholder".to_owned(), None),
            ]
        );
    }

    #[tokio::test]
    async fn traverses_tab_children_in_stable_depth_first_order() {
        let source = fake(vec![
            (
                ("page-placeholder", None),
                page(
                    vec![
                        json!({"object":"block","id":"tab-placeholder","type":"tab","has_children":true,"tab":{}}),
                        rich("after-placeholder", "paragraph", "after tab", false),
                    ],
                    None,
                ),
            ),
            (
                ("tab-placeholder", None),
                page(
                    vec![rich("inside-placeholder", "paragraph", "inside tab", false)],
                    None,
                ),
            ),
        ]);

        assert_eq!(
            render_page(&source, "page-placeholder").await.unwrap(),
            "inside tab\n\nafter tab"
        );
        assert_eq!(
            *source.calls.lock().unwrap(),
            vec![
                ("page-placeholder".to_owned(), None),
                ("tab-placeholder".to_owned(), None),
            ]
        );
    }

    #[tokio::test]
    async fn requires_boolean_has_children_even_when_no_traversal_is_needed() {
        for invalid in [
            json!({"object":"block","id":"missing-placeholder","type":"divider","divider":{}}),
            json!({"object":"block","id":"wrong-placeholder","type":"divider","has_children":"false","divider":{}}),
        ] {
            let source = fake(vec![(
                ("page-placeholder", None),
                page(vec![invalid], None),
            )]);
            assert_eq!(
                render_page(&source, "page-placeholder").await.unwrap_err(),
                "Notion returned an invalid block response"
            );
        }

        let source = fake(vec![(
            ("page-placeholder", None),
            page(
                vec![
                    json!({"object":"block","id":"valid-placeholder","type":"divider","has_children":false,"divider":{}}),
                ],
                None,
            ),
        )]);
        assert_eq!(
            render_page(&source, "page-placeholder").await.unwrap(),
            "---"
        );
    }

    #[tokio::test]
    async fn code_fence_is_longer_than_every_embedded_backtick_run() {
        let source = fake(vec![(
            ("page-placeholder", None),
            page(
                vec![json!({
                    "object":"block", "id":"code-placeholder", "type":"code", "has_children":false,
                    "code":{"rich_text":[text("before ``` after ```` end")],"language":"rust"}
                })],
                None,
            ),
        )]);

        assert_eq!(
            render_page(&source, "page-placeholder").await.unwrap(),
            "`````rust\nbefore ``` after ```` end\n`````"
        );
    }

    #[tokio::test]
    async fn prefixes_every_fenced_and_blank_line_nested_under_a_quote() {
        let source = fake(vec![
            (
                ("page-placeholder", None),
                page(
                    vec![
                        rich("quote-placeholder", "quote", "quoted", true),
                        rich("after-placeholder", "paragraph", "after", false),
                    ],
                    None,
                ),
            ),
            (
                ("quote-placeholder", None),
                page(
                    vec![
                        json!({
                            "object":"block", "id":"code-placeholder", "type":"code", "has_children":false,
                            "code":{"rich_text":[text("first\nsecond")],"language":"rust"}
                        }),
                        json!({
                            "object":"block", "id":"equation-placeholder", "type":"equation", "has_children":false,
                            "equation":{"expression":"x +\ny"}
                        }),
                    ],
                    None,
                ),
            ),
        ]);

        assert_eq!(
            render_page(&source, "page-placeholder").await.unwrap(),
            "> quoted\n> ```rust\n> first\n> second\n> ```\n> \n> $$\n> x +\n> y\n> $$\n> \nafter"
        );
    }

    #[tokio::test]
    async fn preserves_multiline_content_and_blank_frames_in_nested_lists() {
        let source = fake(vec![
            (
                ("page-placeholder", None),
                page(
                    vec![
                        rich(
                            "list-placeholder",
                            "bulleted_list_item",
                            "parent\ncontinuation",
                            true,
                        ),
                        rich("after-placeholder", "paragraph", "after", false),
                    ],
                    None,
                ),
            ),
            (
                ("list-placeholder", None),
                page(
                    vec![rich(
                        "child-placeholder",
                        "paragraph",
                        "child one\nchild two",
                        false,
                    )],
                    None,
                ),
            ),
        ]);

        assert_eq!(
            render_page(&source, "page-placeholder").await.unwrap(),
            "- parent\n  continuation\n  child one\n  child two\n  \nafter"
        );
    }

    #[tokio::test]
    async fn numbered_items_use_three_space_continuation_and_child_frames() {
        let source = fake(vec![
            (
                ("page-placeholder", None),
                page(
                    vec![
                        rich(
                            "numbered-placeholder",
                            "numbered_list_item",
                            "parent\ncontinuation",
                            true,
                        ),
                        rich("after-placeholder", "paragraph", "after", false),
                    ],
                    None,
                ),
            ),
            (
                ("numbered-placeholder", None),
                page(
                    vec![rich(
                        "child-placeholder",
                        "paragraph",
                        "child one\nchild two",
                        false,
                    )],
                    None,
                ),
            ),
        ]);

        assert_eq!(
            render_page(&source, "page-placeholder").await.unwrap(),
            "1. parent\n   continuation\n   child one\n   child two\n   \nafter"
        );
    }

    #[tokio::test]
    async fn callout_children_keep_every_fenced_line_inside_the_quote_frame() {
        let source = fake(vec![
            (
                ("page-placeholder", None),
                page(
                    vec![
                        rich("callout-placeholder", "callout", "notice\ncontinued", true),
                        rich("after-placeholder", "paragraph", "after", false),
                    ],
                    None,
                ),
            ),
            (
                ("callout-placeholder", None),
                page(
                    vec![json!({
                        "object":"block", "id":"code-placeholder", "type":"code", "has_children":false,
                        "code":{"rich_text":[text("first\nsecond")],"language":"text"}
                    })],
                    None,
                ),
            ),
        ]);

        assert_eq!(
            render_page(&source, "page-placeholder").await.unwrap(),
            "> notice\n> continued\n> \n> ```text\n> first\n> second\n> ```\n> \nafter"
        );
    }

    #[tokio::test]
    async fn inline_code_uses_longer_delimiters_and_commonmark_boundary_padding() {
        let inline = |value: &str| {
            json!({
                "type":"text", "plain_text":value, "href":null,
                "annotations":{"bold":false,"italic":false,"strikethrough":false,"underline":false,"code":true}
            })
        };
        let source = fake(vec![(
            ("page-placeholder", None),
            page(
                vec![json!({
                    "object":"block", "id":"paragraph-placeholder", "type":"paragraph", "has_children":false,
                    "paragraph":{"rich_text":[
                        inline("a `` b"), text(" / "), inline("`edge`"), text(" / "),
                        inline(" spaced "), text(" / "), inline(" leading"), text(" / "),
                        inline("trailing "), text(" / "), inline(" "), text(" / "), inline("plain")
                    ]}
                })],
                None,
            ),
        )]);

        assert_eq!(
            render_page(&source, "page-placeholder").await.unwrap(),
            "```a `` b``` / `` `edge` `` / `  spaced  ` / `  leading ` / ` trailing  ` / ` ` / `plain`"
        );
    }

    #[tokio::test]
    async fn table_rows_use_a_valid_structural_list_representation() {
        let source = fake(vec![(
            ("page-placeholder", None),
            page(
                vec![json!({
                    "object":"block", "id":"row-placeholder", "type":"table_row", "has_children":false,
                    "table_row":{"cells":[[text("left | value")],[text("line one\nline two")]]}
                })],
                None,
            ),
        )]);

        assert_eq!(
            render_page(&source, "page-placeholder").await.unwrap(),
            "- **Table row:** left \\| value · line one<br>line two"
        );
    }

    #[tokio::test]
    async fn rejects_cursor_and_child_cycles() {
        let cursor_cycle = fake(vec![
            (("page-placeholder", None), page(Vec::new(), Some("same"))),
            (
                ("page-placeholder", Some("same")),
                page(Vec::new(), Some("same")),
            ),
        ]);
        assert_eq!(
            render_page(&cursor_cycle, "page-placeholder")
                .await
                .unwrap_err(),
            "Notion returned an invalid block cursor"
        );

        let child_cycle = fake(vec![
            (
                ("page-placeholder", None),
                page(
                    vec![rich("child-placeholder", "paragraph", "child", true)],
                    None,
                ),
            ),
            (
                ("child-placeholder", None),
                page(
                    vec![rich("child-placeholder", "paragraph", "cycle", true)],
                    None,
                ),
            ),
        ]);
        assert_eq!(
            render_page(&child_cycle, "page-placeholder")
                .await
                .unwrap_err(),
            "Notion task content contains a traversal cycle"
        );
    }
}
