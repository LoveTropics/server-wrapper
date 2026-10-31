use crate::source::http;
use crate::{cache, config, source, Result};
use anyhow::anyhow;
use ristretto_resolver::{ArtifactCoordinate, ResolutionRequest, Resolver};
use std::str::FromStr;

pub async fn load<'a>(
    client: &reqwest::Client,
    maven_resolver: &Resolver,
    cache: cache::Entry<'a>,
    artifact: &str,
    transform: &config::Transform,
) -> Result<cache::Reference> {
    let coordinate = ArtifactCoordinate::from_str(artifact)?;
    let resolution = maven_resolver
        .resolve(&ResolutionRequest::new(coordinate).with_transitive_repositories(true))
        .await?;

    if resolution.roots.len() != 1 {
        return Err(anyhow!("Multiple root artifacts: {:?}", resolution));
    }
    let root_id = resolution.roots[0];

    // TODO: Ignoring dependencies - this sucks. We should do better, but it's a pretty big change
    //  We should be resolving dependencies across *all* maven references
    let artifact = resolution
        .artifacts
        .into_iter()
        .find(|artifact| artifact.node == root_id)
        .ok_or_else(|| anyhow!("Maven dependency not found: {}", artifact))?;

    let cache_token = cache::Token::Maven {
        coordinate: artifact.coordinate,
        file: artifact.file_version,
    };

    use cache::UpdateResult::*;
    match cache.try_update(cache_token) {
        Mismatch(updater) => {
            println!("downloading {}...", artifact.url);

            let response = client.get(&artifact.url).send().await?;

            let name = http::file_name(&artifact.url).to_owned();
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
