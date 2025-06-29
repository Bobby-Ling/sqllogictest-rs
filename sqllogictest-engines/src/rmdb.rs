use std::net::SocketAddr;
use std::process::Command;
use std::time::Duration;

use async_trait::async_trait;
use sqllogictest::{DBOutput, DefaultColumnType};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_MEM_BUFFER_SIZE: usize = 8192;

#[derive(Debug, Error)]
pub enum RmdbError {
    #[error("连接失败: {0}")]
    ConnectionError(#[from] std::io::Error),
    #[error("SQL执行失败: {0}")]
    SqlError(String),
    #[error("解析响应失败: {0}")]
    ParseError(String),
    #[error("编码错误: {0}")]
    EncodingError(#[from] std::string::FromUtf8Error),
}

type Result<T> = std::result::Result<T, RmdbError>;

/// RMDB连接配置
#[derive(Clone, Debug)]
pub struct RmdbConfig {
    pub host: String,
    pub port: u16,
}

impl RmdbConfig {
    pub fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }

    pub fn with_host(mut self, host: String) -> Self {
        self.host = host;
        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
}

impl Default for RmdbConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8765,
        }
    }
}

/// RMDB引擎，基于TCP socket连接
pub struct Rmdb {
    stream: TcpStream,
    config: RmdbConfig,
}

impl Rmdb {
    /// 连接到RMDB服务器
    pub async fn connect(config: RmdbConfig) -> Result<Self> {
        let addr = SocketAddr::from((
            config.host.parse::<std::net::IpAddr>()
                .map_err(|_| std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("无效的主机地址: {}", config.host)
                ))?,
            config.port,
        ));

        let stream = TcpStream::connect(addr).await?;

        Ok(Self {
            stream,
            config,
        })
    }

    /// 发送SQL命令并接收响应
    async fn send_command(&mut self, command: &str) -> Result<String> {
        // 发送命令（以 \0 结尾）
        let cmd_bytes = format!("{}\0", command);
        self.stream.write_all(cmd_bytes.as_bytes()).await?;

        // 接收响应
        let mut buffer = vec![0u8; MAX_MEM_BUFFER_SIZE];
        let n = self.stream.read(&mut buffer).await?;

        if n == 0 {
            return Err(RmdbError::ConnectionError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "连接已关闭"
            )));
        }

        let mut data = String::from_utf8_lossy(&buffer[..n]).to_string();

        // 处理abort情况
        while data.contains("abort") {
            println!("[Server] {}", data);
            self.stream.write_all(command.as_bytes()).await?;
            let n = self.stream.read(&mut buffer).await?;
            data = String::from_utf8_lossy(&buffer[..n]).to_string();
        }

        Ok(data)
    }

    /// 解析表格格式的响应
    fn parse_table_response(&self, response: &str) -> Result<Vec<Vec<String>>> {
        let lines: Vec<&str> = response.lines().collect();
        let mut rows = Vec::new();

        let mut in_table = false;
        let mut header_found = false;

        for line in lines {
            let trimmed = line.trim();

            // 跳过空行
            if trimmed.is_empty() {
                continue;
            }

            // 检测表格开始（包含 | 符号的行）
            if trimmed.contains('|') {
                if !header_found {
                    // 第一行可能是表头，跳过分隔符行
                    header_found = true;
                    in_table = true;
                    continue;
                } else if trimmed.chars().all(|c| c == '|' || c == '-' || c.is_whitespace()) {
                    // 这是分隔符行，跳过
                    continue;
                } else if in_table {
                    // 这是数据行
                    let row: Vec<String> = trimmed
                        .split('|')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();

                    if !row.is_empty() {
                        rows.push(row);
                    }
                }
            } else if in_table {
                // 表格结束
                break;
            }
        }

        Ok(rows)
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for Rmdb {
    type Error = RmdbError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>> {
        let response = self.send_command(sql).await?;

        // 如果响应包含错误信息
        if response.contains("error") || response.to_lowercase().contains("failed") {
            return Err(RmdbError::SqlError(response));
        }

        // 尝试解析表格响应
        let rows = self.parse_table_response(&response)?;

        if rows.is_empty() {
            // 假设这是一个语句执行完成（没有返回行）
            // 尝试从响应中提取受影响的行数
            let affected_rows = if response.contains("rows affected") || response.contains("row affected") {
                // 简单的解析逻辑，尝试提取数字
                response
                    .split_whitespace()
                    .find_map(|word| word.parse::<u64>().ok())
                    .unwrap_or(0)
            } else {
                0
            };
            Ok(DBOutput::StatementComplete(affected_rows))
        } else {
            // 返回查询结果
            let column_count = rows.first().map(|row| row.len()).unwrap_or(0);
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Any; column_count],
                rows,
            })
        }
    }

    async fn shutdown(&mut self) {
        // 关闭TCP连接
        let _ = self.stream.shutdown().await;
    }

    fn engine_name(&self) -> &str {
        "rmdb"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }

    async fn run_command(command: Command) -> std::io::Result<std::process::Output> {
        tokio::process::Command::from(command).output().await
    }
}