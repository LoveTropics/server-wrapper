use std::path::PathBuf;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use sha1::{Digest, Sha1};
use tokio::fs;

use crate::{cache, config, source};

pub async fn load<'a>(
    cache: cache::Entry<'a>,
    path: &PathBuf,
    transform: &config::Transform,
) -> Result<cache::Reference> {
    let bytes = fs::read(&path).await?;

    let mut hasher = Sha1::new();
    hasher.update(&bytes);

    let mut hash = [0u8; 20];
    hash.copy_from_slice(&hasher.finalize());

    use cache::UpdateResult::*;
    match cache.try_update(cache::Token::Sha1(hash)) {
        Mismatch(updater) => {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap()
                .to_owned();

            let bytes = Bytes::from(bytes);

            let file = source::File { name, bytes };

            if let Some(file) = transform.apply(file).await? {
                Ok(updater.update(file).await?)
            } else {
                Err(anyhow!("Missing artifact"))
            }
        }
        Match(reference) => Ok(reference),
    }
}
