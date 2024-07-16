//! crates.io Source
//!
//! Crates.io source first download current crates.io-index zip from GitHub,
//! and then extract downloadable crates from crates.io-index in memory.

use crate::common::{Mission, SnapshotConfig, TransferURL};
use crate::error::Result;
use crate::traits::{SnapshotStorage, SourceStorage};

use crate::metadata::SnapshotMeta;
use async_trait::async_trait;
use serde::Deserialize;
use slog::info;
use std::io::Read;
use structopt::StructOpt;
use bytes::Bytes;
use std::path::Path;

#[derive(Deserialize, Debug)]
pub struct CratesIoPackage {
    name: String,
    vers: String,
    cksum: String,
}

#[derive(Debug, Clone, StructOpt)]
pub struct CratesIo {
    #[structopt(
        long,
        default_value = "https://github.com/rust-lang/crates.io-index/archive/master.zip"
    )]
    pub zip_master: String,
    #[structopt(long, default_value = "master.zip")]
    pub with_local_master: String,
    #[structopt(long, default_value = "https://static.crates.io/crates")]
    pub crates_base: String,
    #[structopt(long)]
    pub debug: bool,
}

async fn get_or_download(conf: &CratesIo, mission: &Mission) -> Result<Bytes> {
    if std::path::Path::new(&conf.with_local_master).exists() {
        info!(mission.logger, "loading crates.io-index zip from {}...", conf.with_local_master);
        let data = std::fs::read(&conf.with_local_master)?;
        Ok(Bytes::from(data))
    } else {
        info!(mission.logger, "downloading crates.io-index from {}...", conf.zip_master);
        let data = mission.client.get(&conf.zip_master).send().await?.bytes().await?;
        std::fs::write(&conf.with_local_master, &data)?;
        Ok(Bytes::from(data))
    }
}

#[async_trait]
impl SnapshotStorage<SnapshotMeta> for CratesIo {
    async fn snapshot(
        &mut self,
        mission: Mission,
        _config: &SnapshotConfig,
    ) -> Result<Vec<SnapshotMeta>> {
        let logger = &mission.logger;
        let progress = &mission.progress;
       // let client = mission.client;

        progress.set_message("fetching crates.io-index zip...");
        let data = get_or_download(&self, &mission).await?;
        let mut data = std::io::Cursor::new(data);
        let mut buf = vec![];
        let mut snapshot = vec![];
        info!(logger, "parsing...");

        let mut idx = 0;
        loop {
            match zip::read::read_zipfile_from_stream(&mut data) {
                Ok(Some(mut file)) => {
                    let mut is_first = true;
                    buf.clear();
                    file.read_to_end(&mut buf)?;

                    let mut de = serde_json::Deserializer::from_reader(&buf[..]);
                    while let Ok(package) = CratesIoPackage::deserialize(&mut de) {
                        // 系统不同
                        let url = Path::new(&package.name).join(format!("{}-{}.crate", package.name, package.vers)).to_str().unwrap().to_string();
                        if is_first {
                            progress.set_message(&url);
                            is_first = false;
                        }
                        idx += 1;
                        progress.inc(1);
                        snapshot.push(SnapshotMeta {
                            key: url,
                            checksum_method: Some(String::from("sha256")),
                            checksum: Some(package.cksum),
                            ..Default::default()
                        });
                    }
                }
                Ok(None) => break,
                Err(e) => return Err(e.into()),
            }
            if self.debug && idx >= 10000 {
                break;
            }
            tokio::task::yield_now().await;
        }

        progress.finish_with_message("done");

        Ok(snapshot)
    }

    fn info(&self) -> String {
        format!("crates.io, {:?}", self)
    }
}

#[async_trait]
impl SourceStorage<SnapshotMeta, TransferURL> for CratesIo {
    async fn get_object(&self, snapshot: &SnapshotMeta, _mission: &Mission) -> Result<TransferURL> {
        Ok(TransferURL(format!(
            "{}/{}",
            self.crates_base, snapshot.key
        )))
    }
}
