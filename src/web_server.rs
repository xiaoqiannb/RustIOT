//! 内置 HTTP 调试页面服务。
//!
//! 使用 axum 提供静态文件服务，HTML 由 rust-embed 在编译时嵌入二进制，
//! 因此打包后 exe 自包含，无需外部资源文件。

use anyhow::{Context, Result};
use axum::{routing::get, Router};
use rust_embed::Embed;
use std::net::SocketAddr;
use tracing::info;

/// 编译期嵌入的静态资源目录。
/// 路径是相对于 crate 根目录的（即 Cargo.toml 所在目录）。
#[derive(Embed)]
#[folder = "src/assets/"]
struct Assets;

/// HTTP 调试页面服务器。
pub struct WebServer {
    bind: SocketAddr,
}

impl WebServer {
    pub fn new(bind: SocketAddr) -> Self {
        Self { bind }
    }

    pub async fn run(self) -> Result<()> {
        // 尝试用 ServeDir 热重载（开发时方便），
        // 如果 assets 目录不存在（release 构建通常就是这样），fallback 到嵌入版本。
        let app = Router::new()
            .route("/", get(index_html));

        let listener = tokio::net::TcpListener::bind(self.bind)
            .await
            .with_context(|| format!("bind web {}", self.bind))?;

        info!(addr = %self.bind, "Web debug server listening → http://{}", self.bind);

        axum::serve(listener, app).await?;
        Ok(())
    }
}

/// 直接从嵌入资源返回 index.html。
async fn index_html() -> impl axum::response::IntoResponse {
    let content = Assets::get("index.html")
        .expect("index.html must exist in assets");
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        content.data.to_vec(),
    )
}