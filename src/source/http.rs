use crate::{cache, config, source};
use anyhow::{anyhow, Result};

pub async fn load<'a>(
    client: &reqwest::Client,
    cache: cache::Entry<'a>,
    url: &str,
    transform: &config::Transform,
) -> Result<cache::Reference> {
    let response = client.get(url).send().await?;

    let etag = response
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|etag| etag.to_str().ok());

    let cache_token = match etag {
        Some(etag) => cache::Token::Etag(etag[1..etag.len() - 1].to_owned()),
        None => cache::Token::Unknown,
    };

    use cache::UpdateResult::*;
    match cache.try_update(cache_token) {
        Mismatch(updater) => {
            println!("downloading {}...", url);

            let name = file_name(url).to_owned();
            let bytes = response.bytes().await?;
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

pub(crate) fn file_name(url: &str) -> &str {
    match url.rsplit_once("/") {
        Some((_, name)) => name,
        None => url,
    }
}
