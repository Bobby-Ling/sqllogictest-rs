use std::fmt::Display;
use std::time::Duration;

use async_trait::async_trait;
use clap::ValueEnum;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use sqllogictest_engines::external::ExternalDriver;
use sqllogictest_engines::mysql::{MySql, MySqlConfig};
use sqllogictest_engines::postgres::{PostgresConfig, PostgresExtended, PostgresSimple};
use sqllogictest_engines::rmdb::{Rmdb, RmdbConfig};
use tokio::process::Command;

use super::{DBConfig, Result};

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum EngineType {
    Mysql,
    Postgres,
    PostgresExtended,
    External,
    Rmdb,
}

#[derive(Clone, Debug)]
pub enum EngineConfig {
    MySql,
    Postgres,
    PostgresExtended,
    External(String),
    Rmdb,
}

pub(crate) enum Engines {
    MySql(MySql),
    Postgres(PostgresSimple),
    PostgresExtended(PostgresExtended),
    External(ExternalDriver),
    Rmdb(Rmdb),
}

impl From<&DBConfig> for MySqlConfig {
    fn from(config: &DBConfig) -> Self {
        let (host, port) = config.random_addr();
        let database_url = format!(
            "mysql://{}:{}@{}:{}/{}",
            config.user, config.pass, host, port, config.db
        );

        MySqlConfig::from_url(&database_url).unwrap()
    }
}

impl From<&DBConfig> for PostgresConfig {
    fn from(config: &DBConfig) -> Self {
        let (host, port) = config.random_addr();

        let mut pg_config = PostgresConfig::new();
        pg_config
            .host(host)
            .port(port)
            .dbname(&config.db)
            .user(&config.user)
            .password(&config.pass);
        if let Some(options) = &config.options {
            pg_config.options(options);
        }

        pg_config
    }
}

impl From<&DBConfig> for RmdbConfig {
    fn from(config: &DBConfig) -> Self {
        let (host, port) = config.random_addr();
        RmdbConfig::new(host.to_string(), port)
    }
}

pub(crate) async fn connect(
    engine: &EngineConfig,
    config: &DBConfig,
) -> Result<Engines, EnginesError> {
    Ok(match engine {
        EngineConfig::MySql => Engines::MySql(
            MySql::connect(config.into())
                .await
                .map_err(|e| EnginesError(e.into()))?,
        ),
        EngineConfig::Postgres => Engines::Postgres(
            PostgresSimple::connect(config.into())
                .await
                .map_err(|e| EnginesError(e.into()))?,
        ),
        EngineConfig::PostgresExtended => Engines::PostgresExtended(
            PostgresExtended::connect(config.into())
                .await
                .map_err(|e| EnginesError(e.into()))?,
        ),
        EngineConfig::External(cmd_tmpl) => {
            let (host, port) = config.random_addr();
            let cmd_str = cmd_tmpl
                .replace("{db}", &config.db)
                .replace("{host}", host)
                .replace("{port}", &port.to_string())
                .replace("{user}", &config.user)
                .replace("{pass}", &config.pass);
            let mut cmd = Command::new("bash");
            cmd.args(["-c", &cmd_str]);
            Engines::External(
                ExternalDriver::connect(cmd)
                    .await
                    .map_err(|e| EnginesError(e.into()))?,
            )
        }
        EngineConfig::Rmdb => Engines::Rmdb(
            Rmdb::connect(config.into())
                .await
                .map_err(|e| EnginesError(e.into()))?,
        ),
    })
}

#[derive(Debug)]
pub(crate) struct EnginesError(anyhow::Error);

impl Display for EnginesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for EnginesError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

macro_rules! dispatch_engines {
    ($impl:expr, $inner:ident, $body:tt) => {{
        match $impl {
            Engines::MySql($inner) => $body,
            Engines::Postgres($inner) => $body,
            Engines::PostgresExtended($inner) => $body,
            Engines::External($inner) => $body,
            Engines::Rmdb($inner) => $body,
        }
    }};
}

#[async_trait]
impl AsyncDB for Engines {
    type Error = EnginesError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        dispatch_engines!(self, e, {
            e.run(sql)
                .await
                .map_err(|e| EnginesError(anyhow::Error::from(e)))
        })
    }

    fn engine_name(&self) -> &str {
        dispatch_engines!(self, e, { e.engine_name() })
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }

    async fn run_command(command: std::process::Command) -> std::io::Result<std::process::Output> {
        Command::from(command).output().await
    }

    async fn shutdown(&mut self) {
        dispatch_engines!(self, e, { e.shutdown().await })
    }
}
