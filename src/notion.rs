mod content;

use self::content::{BlockPage, BlockSource};
use crate::config::{JournalProperties, JournalValues, NotionConfig, TaskProperties};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const NOTION_API_VERSION: &str = "2026-03-11";
const NOTION_API_ROOT: &str = "https://api.notion.com/";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_NOTION_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_REVISION_BYTES: usize = 64;
const MAX_CURSOR_BYTES: usize = 512;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskState {
    pub page_id: String,
    pub revision: TaskRevision,
    pub data_source_id: Option<String>,
    pub status: Option<String>,
    pub in_trash: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskRevision {
    value: String,
    instant: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingTaskPage {
    pub tasks: Vec<TaskState>,
    pub next_cursor: Option<String>,
    pub response_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialJournalAttempt {
    pub run_id: String,
    pub task_page_id: String,
    pub executor: String,
    pub started_at: String,
}

impl TaskRevision {
    pub(crate) fn parse(value: &str) -> Result<Self, String> {
        if value.is_empty() || value.len() > MAX_REVISION_BYTES || !value.is_ascii() {
            return Err("Notion returned an invalid task response".to_owned());
        }
        let instant = OffsetDateTime::parse(value, &Rfc3339)
            .map_err(|_| "Notion returned an invalid task response".to_owned())?;
        Ok(Self {
            value: value.to_owned(),
            instant,
        })
    }

    pub(crate) fn instant(&self) -> OffsetDateTime {
        self.instant
    }
}

pub trait NotionAdapter: Send + Sync + 'static {
    fn query_pending_tasks<'a>(
        &'a self,
        _cursor: Option<&'a str>,
        _pending_status: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<PendingTaskPage, String>> + Send + 'a>> {
        Box::pin(async { Err("Notion Pending task queries are unavailable".to_owned()) })
    }

    fn refetch_task<'a>(
        &'a self,
        page_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<TaskState, String>> + Send + 'a>>;

    fn render_task<'a>(
        &'a self,
        page_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>>;

    fn update_task_status<'a>(
        &'a self,
        _page_id: &'a str,
        _status: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Err("Notion task status updates are unavailable".to_owned()) })
    }

    fn create_initial_journal<'a>(
        &'a self,
        _attempt: &'a InitialJournalAttempt,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Err("Notion journal creation is unavailable".to_owned()) })
    }

    fn find_journal_by_run_id<'a>(
        &'a self,
        _run_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<InitialJournalAttempt>, String>> + Send + 'a>>
    {
        Box::pin(async { Err("Notion journal lookup is unavailable".to_owned()) })
    }
}

#[derive(Clone)]
pub struct NotionHttpClient {
    client: reqwest::Client,
    api_root: reqwest::Url,
    properties: TaskProperties,
    task_data_source_id: String,
    journal_data_source_id: String,
    journal_properties: JournalProperties,
    journal_values: JournalValues,
    max_response_bytes: usize,
}

impl NotionHttpClient {
    pub fn new(
        config: &NotionConfig,
        properties: &TaskProperties,
        journal_properties: &JournalProperties,
        journal_values: &JournalValues,
    ) -> Result<Self, String> {
        let api_root = reqwest::Url::parse(NOTION_API_ROOT)
            .map_err(|_| "cannot initialize Notion API client".to_owned())?;
        Self::with_options(
            config,
            properties,
            journal_properties,
            journal_values,
            api_root,
            REQUEST_TIMEOUT,
            MAX_NOTION_RESPONSE_BYTES,
        )
    }

    fn with_options(
        config: &NotionConfig,
        properties: &TaskProperties,
        journal_properties: &JournalProperties,
        journal_values: &JournalValues,
        api_root: reqwest::Url,
        timeout: Duration,
        max_response_bytes: usize,
    ) -> Result<Self, String> {
        let mut headers = HeaderMap::new();
        let mut authorization = HeaderValue::from_str(&format!("Bearer {}", config.token))
            .map_err(|_| "configured Notion token is invalid".to_owned())?;
        authorization.set_sensitive(true);
        headers.insert(AUTHORIZATION, authorization);
        headers.insert(
            "notion-version",
            HeaderValue::from_static(NOTION_API_VERSION),
        );
        let root_certificates = webpki_root_certs::TLS_SERVER_ROOT_CERTS
            .iter()
            .map(|certificate| reqwest::Certificate::from_der(certificate.as_ref()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "cannot initialize Notion API client".to_owned())?;
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(timeout)
            .tls_certs_only(root_certificates)
            .build()
            .map_err(|_| "cannot initialize Notion API client".to_owned())?;
        Ok(Self {
            client,
            api_root,
            properties: properties.clone(),
            task_data_source_id: config.task_data_source_id.clone(),
            journal_data_source_id: config.journal_data_source_id.clone(),
            journal_properties: journal_properties.clone(),
            journal_values: journal_values.clone(),
            max_response_bytes,
        })
    }

    async fn retrieve(&self, page_id: &str) -> Result<TaskState, String> {
        let mut url = self.api_root.clone();
        url.path_segments_mut()
            .map_err(|_| "cannot initialize Notion API client".to_owned())?
            .extend(["v1", "pages", page_id]);
        let mut response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|_| "cannot refetch task from Notion".to_owned())?;
        if !response.status().is_success() {
            return Err("Notion refused the task refetch".to_owned());
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.max_response_bytes as u64)
        {
            return Err("Notion task response exceeds the size limit".to_owned());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| "cannot read the Notion task response".to_owned())?
        {
            if body.len().saturating_add(chunk.len()) > self.max_response_bytes {
                return Err("Notion task response exceeds the size limit".to_owned());
            }
            body.extend_from_slice(&chunk);
        }
        let page = serde_json::from_slice::<Value>(&body)
            .map_err(|_| "Notion returned an invalid task response".to_owned())?;
        parse_task(page_id, &page, &self.properties)
    }

    async fn query_pending(
        &self,
        cursor: Option<&str>,
        pending_status: &str,
    ) -> Result<PendingTaskPage, String> {
        let mut url = self.api_root.clone();
        url.path_segments_mut()
            .map_err(|_| "cannot initialize Notion API client".to_owned())?
            .extend(["v1", "data_sources", &self.task_data_source_id, "query"]);
        let mut body = serde_json::json!({
            "page_size": 100,
            "filter": {
                "property": self.properties.status,
                "status": { "equals": pending_status }
            }
        });
        if let Some(cursor) = cursor {
            body["start_cursor"] = Value::String(cursor.to_owned());
        }
        let mut response = self
            .client
            .post(url)
            .json(&body)
            .send()
            .await
            .map_err(|_| "cannot query Pending tasks from Notion".to_owned())?;
        if !response.status().is_success() {
            return Err("Notion refused the Pending task query".to_owned());
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.max_response_bytes as u64)
        {
            return Err("Notion Pending task response exceeds the size limit".to_owned());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| "cannot read the Notion Pending task response".to_owned())?
        {
            if bytes.len().saturating_add(chunk.len()) > self.max_response_bytes {
                return Err("Notion Pending task response exceeds the size limit".to_owned());
            }
            bytes.extend_from_slice(&chunk);
        }
        let value = serde_json::from_slice::<Value>(&bytes)
            .map_err(|_| "Notion returned an invalid Pending task response".to_owned())?;
        parse_pending_task_page(&value, &self.properties, bytes.len())
    }

    async fn retrieve_block_page(
        &self,
        parent_id: &str,
        cursor: Option<&str>,
    ) -> Result<BlockPage, String> {
        let mut url = self.api_root.clone();
        url.path_segments_mut()
            .map_err(|_| "cannot initialize Notion API client".to_owned())?
            .extend(["v1", "blocks", parent_id, "children"]);
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("page_size", "100");
            if let Some(cursor) = cursor {
                query.append_pair("start_cursor", cursor);
            }
        }
        let (value, response_bytes) = self
            .get_json(
                url,
                "cannot retrieve task content from Notion",
                "Notion refused the task content request",
                "Notion task content response exceeds the size limit",
                "cannot read the Notion task content response",
                "Notion returned an invalid block response",
            )
            .await?;
        let mut page = parse_block_page(&value)?;
        page.response_bytes = response_bytes;
        Ok(page)
    }

    async fn get_json(
        &self,
        url: reqwest::Url,
        send_error: &str,
        status_error: &str,
        size_error: &str,
        read_error: &str,
        parse_error: &str,
    ) -> Result<(Value, usize), String> {
        let mut response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|_| send_error.to_owned())?;
        if !response.status().is_success() {
            return Err(status_error.to_owned());
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.max_response_bytes as u64)
        {
            return Err(size_error.to_owned());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| read_error.to_owned())? {
            if body.len().saturating_add(chunk.len()) > self.max_response_bytes {
                return Err(size_error.to_owned());
            }
            body.extend_from_slice(&chunk);
        }
        let value = serde_json::from_slice(&body).map_err(|_| parse_error.to_owned())?;
        Ok((value, body.len()))
    }

    async fn update_status(&self, page_id: &str, status: &str) -> Result<(), String> {
        let mut url = self.api_root.clone();
        url.path_segments_mut()
            .map_err(|_| "cannot initialize Notion API client".to_owned())?
            .extend(["v1", "pages", page_id]);
        let response = self
            .client
            .patch(url)
            .json(&serde_json::json!({
                "properties": { &self.properties.status: { "status": { "name": status } } }
            }))
            .send()
            .await
            .map_err(|_| "cannot update task status in Notion".to_owned())?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err("Notion refused the task status update".to_owned())
        }
    }

    async fn create_journal(&self, attempt: &InitialJournalAttempt) -> Result<(), String> {
        let url = self
            .api_root
            .join("v1/pages")
            .map_err(|_| "cannot initialize Notion API client".to_owned())?;
        let p = &self.journal_properties;
        let response = self.client.post(url).json(&serde_json::json!({
            "parent": { "type": "data_source_id", "data_source_id": self.journal_data_source_id },
            "properties": {
                &p.run_id: { "title": [{ "text": { "content": attempt.run_id } }] },
                &p.task: { "relation": [{ "id": attempt.task_page_id }] },
                &p.executor: { "select": { "name": self.journal_values.executor } },
                &p.started_at: { "date": { "start": attempt.started_at } }
            }
        })).send().await.map_err(|_| "journal creation outcome is ambiguous".to_owned())?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err("Notion refused journal creation".to_owned())
        }
    }

    async fn lookup_journal(&self, run_id: &str) -> Result<Option<InitialJournalAttempt>, String> {
        let mut url = self.api_root.clone();
        url.path_segments_mut()
            .map_err(|_| "cannot initialize Notion API client".to_owned())?
            .extend(["v1", "data_sources", &self.journal_data_source_id, "query"]);
        let p = &self.journal_properties;
        let mut response = self
            .client
            .post(url)
            .json(&serde_json::json!({
                "page_size": 2,
                "filter": { "property": p.run_id, "title": { "equals": run_id } }
            }))
            .send()
            .await
            .map_err(|_| "cannot look up journal attempt in Notion".to_owned())?;
        if !response.status().is_success() {
            return Err("Notion refused the journal lookup".to_owned());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| "cannot read the Notion journal lookup".to_owned())?
        {
            if body.len().saturating_add(chunk.len()) > self.max_response_bytes {
                return Err("Notion journal lookup exceeds the size limit".to_owned());
            }
            body.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&body)
            .map_err(|_| "Notion returned an invalid journal lookup".to_owned())?;
        let results = value
            .get("results")
            .and_then(Value::as_array)
            .ok_or_else(|| "Notion returned an invalid journal lookup".to_owned())?;
        if results.len() > 1 {
            return Err("Notion returned duplicate journal attempts for one run ID".to_owned());
        }
        results
            .first()
            .map(|page| parse_initial_journal(page, p))
            .transpose()
    }
}

impl NotionAdapter for NotionHttpClient {
    fn query_pending_tasks<'a>(
        &'a self,
        cursor: Option<&'a str>,
        pending_status: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<PendingTaskPage, String>> + Send + 'a>> {
        Box::pin(self.query_pending(cursor, pending_status))
    }

    fn refetch_task<'a>(
        &'a self,
        page_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<TaskState, String>> + Send + 'a>> {
        Box::pin(self.retrieve(page_id))
    }

    fn render_task<'a>(
        &'a self,
        page_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
        Box::pin(content::render_page(self, page_id))
    }

    fn update_task_status<'a>(
        &'a self,
        page_id: &'a str,
        status: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(self.update_status(page_id, status))
    }
    fn create_initial_journal<'a>(
        &'a self,
        attempt: &'a InitialJournalAttempt,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(self.create_journal(attempt))
    }
    fn find_journal_by_run_id<'a>(
        &'a self,
        run_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<InitialJournalAttempt>, String>> + Send + 'a>>
    {
        Box::pin(self.lookup_journal(run_id))
    }
}

fn parse_initial_journal(
    page: &Value,
    properties: &JournalProperties,
) -> Result<InitialJournalAttempt, String> {
    let values = page
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| "Notion returned an invalid journal lookup".to_owned())?;
    let text = |name: &str, kind: &str| {
        values
            .get(name)?
            .get(kind)?
            .as_array()?
            .first()?
            .pointer("/plain_text")
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    let run_id = text(&properties.run_id, "title")
        .ok_or_else(|| "Notion returned an invalid journal lookup".to_owned())?;
    let task_page_id = values
        .get(&properties.task)
        .and_then(|v| v.get("relation"))
        .and_then(Value::as_array)
        .and_then(|a| a.first())
        .and_then(|v| v.get("id"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| "Notion returned an invalid journal lookup".to_owned())?;
    let executor = values
        .get(&properties.executor)
        .and_then(|v| v.pointer("/select/name"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| "Notion returned an invalid journal lookup".to_owned())?;
    let started_at = values
        .get(&properties.started_at)
        .and_then(|v| v.pointer("/date/start"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| "Notion returned an invalid journal lookup".to_owned())?;
    Ok(InitialJournalAttempt {
        run_id,
        task_page_id,
        executor,
        started_at,
    })
}

impl BlockSource for NotionHttpClient {
    fn block_page<'a>(
        &'a self,
        parent_id: &'a str,
        cursor: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<BlockPage, String>> + Send + 'a>> {
        Box::pin(self.retrieve_block_page(parent_id, cursor))
    }
}

fn parse_block_page(value: &Value) -> Result<BlockPage, String> {
    if value.get("object").and_then(Value::as_str) != Some("list")
        || value.get("type").and_then(Value::as_str) != Some("block")
    {
        return Err("Notion returned an invalid block response".to_owned());
    }
    let results = value
        .get("results")
        .and_then(Value::as_array)
        .ok_or_else(|| "Notion returned an invalid block response".to_owned())?;
    if results.len() > 100 {
        return Err("Notion returned an invalid block response".to_owned());
    }
    let has_more = value
        .get("has_more")
        .and_then(Value::as_bool)
        .ok_or_else(|| "Notion returned an invalid block response".to_owned())?;
    let next_cursor = match value.get("next_cursor") {
        Some(Value::String(cursor)) if has_more => Some(cursor.clone()),
        Some(Value::Null) if !has_more => None,
        _ => return Err("Notion returned an invalid block response".to_owned()),
    };
    Ok(BlockPage {
        blocks: results.clone(),
        next_cursor,
        response_bytes: 0,
    })
}

fn parse_task(
    requested_page_id: &str,
    page: &Value,
    properties: &TaskProperties,
) -> Result<TaskState, String> {
    if page.get("object").and_then(Value::as_str) != Some("page")
        || page.get("id").and_then(Value::as_str) != Some(requested_page_id)
    {
        return Err("Notion returned an invalid task response".to_owned());
    }
    let values = page
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| "Notion returned an invalid task response".to_owned())?;
    let revision = page
        .get("last_edited_time")
        .and_then(Value::as_str)
        .ok_or_else(|| "Notion returned an invalid task response".to_owned())?;
    Ok(TaskState {
        page_id: requested_page_id.to_owned(),
        revision: TaskRevision::parse(revision)?,
        data_source_id: page
            .pointer("/parent/data_source_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        status: status_name(values.get(&properties.status)),
        in_trash: page
            .get("in_trash")
            .and_then(Value::as_bool)
            .ok_or_else(|| "Notion returned an invalid task response".to_owned())?,
    })
}

fn parse_pending_task_page(
    value: &Value,
    properties: &TaskProperties,
    response_bytes: usize,
) -> Result<PendingTaskPage, String> {
    let invalid = || "Notion returned an invalid Pending task response".to_owned();
    if value.get("object").and_then(Value::as_str) != Some("list")
        || value.get("type").and_then(Value::as_str) != Some("page_or_data_source")
    {
        return Err(invalid());
    }
    let results = value
        .get("results")
        .and_then(Value::as_array)
        .ok_or_else(&invalid)?;
    if results.len() > 100 {
        return Err(invalid());
    }
    let tasks = results
        .iter()
        .map(|page| {
            let page_id = page
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(&invalid)?;
            parse_task(page_id, page, properties).map_err(|_| invalid())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = value
        .get("has_more")
        .and_then(Value::as_bool)
        .ok_or_else(&invalid)?;
    let next_cursor = match value.get("next_cursor") {
        Some(Value::String(cursor))
            if has_more
                && !cursor.is_empty()
                && cursor.len() <= MAX_CURSOR_BYTES
                && cursor.is_ascii()
                && !cursor.chars().any(char::is_control) =>
        {
            Some(cursor.clone())
        }
        Some(Value::Null) if !has_more => None,
        _ => return Err(invalid()),
    };
    Ok(PendingTaskPage {
        tasks,
        next_cursor,
        response_bytes,
    })
}

fn status_name(property: Option<&Value>) -> Option<String> {
    property?
        .get("status")?
        .get("name")?
        .as_str()
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn properties() -> TaskProperties {
        TaskProperties {
            title: "Name".to_owned(),
            status: "Status field".to_owned(),
        }
    }

    fn journal_properties() -> JournalProperties {
        JournalProperties {
            run_id: "Run ID".to_owned(),
            task: "Task".to_owned(),
            executor: "Executor".to_owned(),
            started_at: "Started at".to_owned(),
        }
    }

    fn journal_values() -> JournalValues {
        JournalValues {
            executor: "Codex".to_owned(),
        }
    }

    fn config() -> NotionConfig {
        NotionConfig {
            token: "secret-placeholder".to_owned(),
            task_data_source_id: "source-placeholder".to_owned(),
            journal_data_source_id: "journal-placeholder".to_owned(),
        }
    }

    async fn fake_server(
        response: Vec<u8>,
        delay: Duration,
    ) -> (reqwest::Url, tokio::task::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut chunk = [0_u8; 1024];
                let read = stream.read(&mut chunk).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if let Some(header_end) =
                    request.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    let headers =
                        String::from_utf8_lossy(&request[..header_end]).to_ascii_lowercase();
                    let content_length = headers
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length: "))
                        .and_then(|value| value.parse::<usize>().ok())
                        .unwrap_or(0);
                    if request.len() >= header_end + 4 + content_length {
                        break;
                    }
                }
            }
            tokio::time::sleep(delay).await;
            let _ = stream.write_all(&response).await;
            request
        });
        (
            reqwest::Url::parse(&format!("http://{address}/")).unwrap(),
            task,
        )
    }

    fn page_body(id: &str) -> String {
        json!({
            "object": "page",
            "id": id,
            "last_edited_time": "2026-01-01T00:00:00Z",
            "parent": {"type": "data_source_id", "data_source_id": "source-placeholder"},
            "in_trash": false,
            "properties": {
                "Status field": {"type": "status", "status": {"name": "Pending"}}
            }
        })
        .to_string()
    }

    fn response(status: &str, body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn request_json(request: &[u8]) -> Value {
        let body_start = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap()
            + 4;
        serde_json::from_slice(&request[body_start..]).unwrap()
    }

    #[test]
    fn parses_only_the_configured_status_and_ignores_executor_like_properties() {
        let page = json!({
            "object": "page",
            "id": "page-placeholder",
            "last_edited_time": "2026-01-01T00:00:00Z",
            "parent": {"type": "data_source_id", "data_source_id": "source-placeholder"},
            "in_trash": false,
            "properties": {
                "Executor": {"type": "select", "select": {"name": "Other"}},
                "Status field": {"type": "status", "status": {"name": "Pending"}},
                "Prompt": {"type": "rich_text", "rich_text": [{"plain_text": "private"}]}
            }
        });
        let properties = properties();

        assert_eq!(
            parse_task("page-placeholder", &page, &properties).unwrap(),
            TaskState {
                page_id: "page-placeholder".to_owned(),
                revision: TaskRevision::parse("2026-01-01T00:00:00Z").unwrap(),
                data_source_id: Some("source-placeholder".to_owned()),
                status: Some("Pending".to_owned()),
                in_trash: false,
            }
        );
    }

    #[test]
    fn requires_a_bounded_authoritative_revision() {
        let mut page = json!({
            "object": "page",
            "id": "page-placeholder",
            "parent": {"type": "data_source_id", "data_source_id": "source-placeholder"},
            "properties": {}
        });
        assert!(parse_task("page-placeholder", &page, &properties()).is_err());
        page["last_edited_time"] = Value::String("x".repeat(MAX_REVISION_BYTES + 1));
        assert!(parse_task("page-placeholder", &page, &properties()).is_err());
    }

    #[test]
    fn requires_an_authoritative_boolean_trash_state() {
        let mut page = serde_json::from_str::<Value>(&page_body("page-placeholder")).unwrap();
        page.as_object_mut().unwrap().remove("in_trash");
        assert_eq!(
            parse_task("page-placeholder", &page, &properties()).unwrap_err(),
            "Notion returned an invalid task response"
        );
        page["in_trash"] = Value::String("false".to_owned());
        assert_eq!(
            parse_task("page-placeholder", &page, &properties()).unwrap_err(),
            "Notion returned an invalid task response"
        );
    }

    #[tokio::test]
    async fn production_client_encodes_path_and_sends_required_secret_headers() {
        let page_id = "page/with space";
        let body = page_body(page_id);
        let (root, server) = fake_server(response("200 OK", &body), Duration::ZERO).await;
        let client = NotionHttpClient::with_options(
            &config(),
            &properties(),
            &journal_properties(),
            &journal_values(),
            root,
            Duration::from_secs(1),
            4096,
        )
        .unwrap();

        assert_eq!(client.retrieve(page_id).await.unwrap().page_id, page_id);
        let request = String::from_utf8(server.await.unwrap()).unwrap();
        let lower = request.to_ascii_lowercase();
        assert!(request.starts_with("GET /v1/pages/page%2Fwith%20space HTTP/1.1\r\n"));
        assert!(lower.contains("authorization: bearer secret-placeholder\r\n"));
        assert!(lower.contains("notion-version: 2026-03-11\r\n"));
    }

    #[tokio::test]
    async fn block_client_encodes_cursor_and_uses_current_headers_and_page_limit() {
        let body = json!({
            "object": "list", "type": "block", "block": {}, "results": [],
            "has_more": false, "next_cursor": null
        })
        .to_string();
        let (root, server) = fake_server(response("200 OK", &body), Duration::ZERO).await;
        let client = NotionHttpClient::with_options(
            &config(),
            &properties(),
            &journal_properties(),
            &journal_values(),
            root,
            Duration::from_secs(1),
            4096,
        )
        .unwrap();

        client
            .retrieve_block_page("block/placeholder", Some("cursor +/placeholder"))
            .await
            .unwrap();
        let request = String::from_utf8(server.await.unwrap()).unwrap();
        let lower = request.to_ascii_lowercase();
        assert!(request.starts_with(
            "GET /v1/blocks/block%2Fplaceholder/children?page_size=100&start_cursor=cursor+%2B%2Fplaceholder HTTP/1.1\r\n"
        ));
        assert!(lower.contains("authorization: bearer secret-placeholder\r\n"));
        assert!(lower.contains("notion-version: 2026-03-11\r\n"));
    }

    #[tokio::test]
    async fn pending_query_uses_configured_source_status_cursor_and_page_limit() {
        let body = json!({
            "object": "list",
            "type": "page_or_data_source",
            "results": [serde_json::from_str::<Value>(&page_body("page-placeholder")).unwrap()],
            "has_more": true,
            "next_cursor": "next-placeholder"
        })
        .to_string();
        let (root, server) = fake_server(response("200 OK", &body), Duration::ZERO).await;
        let client = NotionHttpClient::with_options(
            &config(),
            &properties(),
            &journal_properties(),
            &journal_values(),
            root,
            Duration::from_secs(1),
            4096,
        )
        .unwrap();

        let page = client
            .query_pending(Some("cursor-placeholder"), "Awaiting")
            .await
            .unwrap();

        assert_eq!(page.tasks.len(), 1);
        assert_eq!(page.next_cursor.as_deref(), Some("next-placeholder"));
        let request = server.await.unwrap();
        assert!(
            String::from_utf8_lossy(&request)
                .starts_with("POST /v1/data_sources/source-placeholder/query HTTP/1.1\r\n")
        );
        assert_eq!(
            request_json(&request),
            json!({
                "page_size": 100,
                "start_cursor": "cursor-placeholder",
                "filter": {
                    "property": "Status field",
                    "status": {"equals": "Awaiting"}
                }
            })
        );
    }

    #[test]
    fn pending_query_page_requires_bounded_results_and_cursor() {
        let valid = serde_json::from_str::<Value>(&page_body("page-placeholder")).unwrap();
        let too_many = json!({
            "object": "list",
            "type": "page_or_data_source",
            "results": vec![valid.clone(); 101],
            "has_more": false,
            "next_cursor": null
        });
        assert!(parse_pending_task_page(&too_many, &properties(), 1).is_err());

        let oversized_cursor = json!({
            "object": "list",
            "type": "page_or_data_source",
            "results": [valid],
            "has_more": true,
            "next_cursor": "x".repeat(MAX_CURSOR_BYTES + 1)
        });
        assert!(parse_pending_task_page(&oversized_cursor, &properties(), 1).is_err());
    }

    #[tokio::test]
    async fn block_client_bounds_each_response_without_exposing_content() {
        let private = "private-content-placeholder".repeat(8);
        let (root, server) = fake_server(response("200 OK", &private), Duration::ZERO).await;
        let client = NotionHttpClient::with_options(
            &config(),
            &properties(),
            &journal_properties(),
            &journal_values(),
            root,
            Duration::from_secs(1),
            32,
        )
        .unwrap();

        let error = client
            .retrieve_block_page("block-placeholder", None)
            .await
            .unwrap_err();
        assert_eq!(error, "Notion task content response exceeds the size limit");
        assert!(!error.contains("private-content-placeholder"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn production_client_writes_running_with_the_configured_status_property() {
        let (root, server) = fake_server(response("200 OK", "{}"), Duration::ZERO).await;
        let client = NotionHttpClient::with_options(
            &config(),
            &properties(),
            &journal_properties(),
            &journal_values(),
            root,
            Duration::from_secs(1),
            4096,
        )
        .unwrap();

        client
            .update_status("page/placeholder", "Running")
            .await
            .unwrap();

        let request = server.await.unwrap();
        let head = String::from_utf8_lossy(&request);
        assert!(head.starts_with("PATCH /v1/pages/page%2Fplaceholder HTTP/1.1\r\n"));
        assert_eq!(
            request_json(&request),
            json!({"properties": {"Status field": {"status": {"name": "Running"}}}})
        );
    }

    #[tokio::test]
    async fn production_client_creates_one_initial_journal_with_configured_fields() {
        let (root, server) = fake_server(response("200 OK", "{}"), Duration::ZERO).await;
        let client = NotionHttpClient::with_options(
            &config(),
            &properties(),
            &journal_properties(),
            &journal_values(),
            root,
            Duration::from_secs(1),
            4096,
        )
        .unwrap();
        let attempt = InitialJournalAttempt {
            run_id: "run-placeholder".to_owned(),
            task_page_id: "task-placeholder".to_owned(),
            executor: "Codex".to_owned(),
            started_at: "2026-01-01T00:00:00Z".to_owned(),
        };

        client.create_journal(&attempt).await.unwrap();

        let request = server.await.unwrap();
        let head = String::from_utf8_lossy(&request);
        assert!(head.starts_with("POST /v1/pages HTTP/1.1\r\n"));
        assert_eq!(
            request_json(&request),
            json!({
                "parent": {"type": "data_source_id", "data_source_id": "journal-placeholder"},
                "properties": {
                    "Run ID": {"title": [{"text": {"content": "run-placeholder"}}]},
                    "Task": {"relation": [{"id": "task-placeholder"}]},
                    "Executor": {"select": {"name": "Codex"}},
                    "Started at": {"date": {"start": "2026-01-01T00:00:00Z"}}
                }
            })
        );
    }

    #[tokio::test]
    async fn production_client_queries_and_reads_back_exactly_one_run_id() {
        let body = json!({"object": "list", "type": "page_or_data_source", "results": [{"object": "page", "properties": {
            "Run ID": {"title": [{"plain_text": "run-placeholder"}]},
            "Task": {"relation": [{"id": "task-placeholder"}]},
            "Executor": {"select": {"name": "Codex"}},
            "Started at": {"date": {"start": "2026-01-01T00:00:00Z"}}
        }}], "has_more": false, "next_cursor": null}).to_string();
        let (root, server) = fake_server(response("200 OK", &body), Duration::ZERO).await;
        let client = NotionHttpClient::with_options(
            &config(),
            &properties(),
            &journal_properties(),
            &journal_values(),
            root,
            Duration::from_secs(1),
            4096,
        )
        .unwrap();

        let found = client
            .lookup_journal("run-placeholder")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(found.run_id, "run-placeholder");
        assert_eq!(found.task_page_id, "task-placeholder");
        let request = server.await.unwrap();
        assert!(
            String::from_utf8_lossy(&request)
                .starts_with("POST /v1/data_sources/journal-placeholder/query HTTP/1.1\r\n")
        );
        assert_eq!(
            request_json(&request),
            json!({"page_size": 2, "filter": {"property": "Run ID", "title": {"equals": "run-placeholder"}}})
        );
    }

    #[tokio::test]
    async fn journal_lookup_rejects_duplicates_malformed_and_oversized_responses() {
        for (body, limit, expected) in [
            (
                json!({"results": [{}, {}]}).to_string(),
                4096,
                "Notion returned duplicate journal attempts for one run ID",
            ),
            (
                json!({"unexpected": []}).to_string(),
                4096,
                "Notion returned an invalid journal lookup",
            ),
            (
                "x".repeat(65),
                64,
                "Notion journal lookup exceeds the size limit",
            ),
        ] {
            let (root, server) = fake_server(response("200 OK", &body), Duration::ZERO).await;
            let client = NotionHttpClient::with_options(
                &config(),
                &properties(),
                &journal_properties(),
                &journal_values(),
                root,
                Duration::from_secs(1),
                limit,
            )
            .unwrap();
            assert_eq!(
                client.lookup_journal("run-placeholder").await.unwrap_err(),
                expected
            );
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn production_client_maps_status_timeout_and_size_errors_without_secrets() {
        let (root, status_server) = fake_server(
            response("403 Forbidden", "private response"),
            Duration::ZERO,
        )
        .await;
        let status_client = NotionHttpClient::with_options(
            &config(),
            &properties(),
            &journal_properties(),
            &journal_values(),
            root,
            Duration::from_secs(1),
            4096,
        )
        .unwrap();
        let status_error = status_client
            .retrieve("page-placeholder")
            .await
            .unwrap_err();
        assert_eq!(status_error, "Notion refused the task refetch");
        assert!(!status_error.contains("secret-placeholder"));
        status_server.await.unwrap();

        let (root, timeout_server) = fake_server(
            response("200 OK", &page_body("page-placeholder")),
            Duration::from_millis(100),
        )
        .await;
        let timeout_client = NotionHttpClient::with_options(
            &config(),
            &properties(),
            &journal_properties(),
            &journal_values(),
            root,
            Duration::from_millis(20),
            4096,
        )
        .unwrap();
        let timeout_error = timeout_client
            .retrieve("page-placeholder")
            .await
            .unwrap_err();
        assert_eq!(timeout_error, "cannot refetch task from Notion");
        assert!(!timeout_error.contains("secret-placeholder"));
        timeout_server.await.unwrap();

        let oversized = "x".repeat(65);
        let (root, size_server) = fake_server(response("200 OK", &oversized), Duration::ZERO).await;
        let size_client = NotionHttpClient::with_options(
            &config(),
            &properties(),
            &journal_properties(),
            &journal_values(),
            root,
            Duration::from_secs(1),
            64,
        )
        .unwrap();
        assert_eq!(
            size_client.retrieve("page-placeholder").await.unwrap_err(),
            "Notion task response exceeds the size limit"
        );
        size_server.await.unwrap();

        let chunked = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\n12345\r\n5\r\n67890\r\n0\r\n\r\n".to_vec();
        let (root, chunked_server) = fake_server(chunked, Duration::ZERO).await;
        let chunked_client = NotionHttpClient::with_options(
            &config(),
            &properties(),
            &journal_properties(),
            &journal_values(),
            root,
            Duration::from_secs(1),
            8,
        )
        .unwrap();
        assert_eq!(
            chunked_client
                .retrieve("page-placeholder")
                .await
                .unwrap_err(),
            "Notion task response exceeds the size limit"
        );
        chunked_server.await.unwrap();
    }
}
