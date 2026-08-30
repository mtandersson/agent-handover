use crate::config::{NotionConfig, TaskProperties};
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskState {
    pub page_id: String,
    pub revision: TaskRevision,
    pub data_source_id: Option<String>,
    pub executor: Option<String>,
    pub status: Option<String>,
    pub in_trash: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskRevision {
    value: String,
    instant: OffsetDateTime,
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
    fn refetch_task<'a>(
        &'a self,
        page_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<TaskState, String>> + Send + 'a>>;
}

pub struct NotionHttpClient {
    client: reqwest::Client,
    api_root: reqwest::Url,
    properties: TaskProperties,
    max_response_bytes: usize,
}

impl NotionHttpClient {
    pub fn new(config: &NotionConfig, properties: &TaskProperties) -> Result<Self, String> {
        let api_root = reqwest::Url::parse(NOTION_API_ROOT)
            .map_err(|_| "cannot initialize Notion API client".to_owned())?;
        Self::with_options(
            config,
            properties,
            api_root,
            REQUEST_TIMEOUT,
            MAX_NOTION_RESPONSE_BYTES,
        )
    }

    fn with_options(
        config: &NotionConfig,
        properties: &TaskProperties,
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
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(timeout)
            .build()
            .map_err(|_| "cannot initialize Notion API client".to_owned())?;
        Ok(Self {
            client,
            api_root,
            properties: properties.clone(),
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
}

impl NotionAdapter for NotionHttpClient {
    fn refetch_task<'a>(
        &'a self,
        page_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<TaskState, String>> + Send + 'a>> {
        Box::pin(self.retrieve(page_id))
    }
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
        executor: select_name(values.get(&properties.executor)),
        status: status_name(values.get(&properties.status)),
        in_trash: page
            .get("in_trash")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

fn select_name(property: Option<&Value>) -> Option<String> {
    property?
        .get("select")?
        .get("name")?
        .as_str()
        .map(str::to_owned)
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
            executor: "Executor field".to_owned(),
            status: "Status field".to_owned(),
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
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
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
                "Executor field": {"type": "select", "select": {"name": "Codex"}},
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

    #[test]
    fn parses_only_the_configured_select_and_status_properties() {
        let page = json!({
            "object": "page",
            "id": "page-placeholder",
            "last_edited_time": "2026-01-01T00:00:00Z",
            "parent": {"type": "data_source_id", "data_source_id": "source-placeholder"},
            "in_trash": false,
            "properties": {
                "Executor field": {"type": "select", "select": {"name": "Codex"}},
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
                executor: Some("Codex".to_owned()),
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

    #[tokio::test]
    async fn production_client_encodes_path_and_sends_required_secret_headers() {
        let page_id = "page/with space";
        let body = page_body(page_id);
        let (root, server) = fake_server(response("200 OK", &body), Duration::ZERO).await;
        let client = NotionHttpClient::with_options(
            &config(),
            &properties(),
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
    async fn production_client_maps_status_timeout_and_size_errors_without_secrets() {
        let (root, status_server) = fake_server(
            response("403 Forbidden", "private response"),
            Duration::ZERO,
        )
        .await;
        let status_client = NotionHttpClient::with_options(
            &config(),
            &properties(),
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
