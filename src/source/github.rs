use std::cmp;

use serde::Deserialize;

use crate::{cache, config, Error, Result, source};

pub async fn load<'a>(cache: cache::Entry<'a>, owner: &str, repository: &str, filter: Filter, transform: &config::Transform) -> Result<cache::Reference> {
    let latest_artifact = get_latest_artifact(owner, repository, filter).await?;

    if let Some((id, url, name)) = latest_artifact {
        use cache::UpdateResult::*;
        match cache.try_update(cache::Token::ArtifactId(id)) {
            Mismatch(updater) => {
                let name = format!("{}.zip", name);

                let url = reqwest::Url::parse(&url).unwrap();
                let response = octocrab::instance()._get(url, None::<&()>).await?;

                let bytes = response.bytes().await?;
                let file = source::File { name, bytes };

                if let Some(file) = transform.apply(file).await? {
                    Ok(updater.update(file).await?)
                } else {
                    Err(Error::MissingArtifact)
                }
            }
            Match(reference) => Ok(reference)
        }
    } else {
        cache.get_existing().ok_or(Error::MissingArtifact)
    }
}

async fn get_latest_artifact(owner: &str, repository: &str, filter: Filter) -> Result<Option<(usize, String, String)>> {
    // TODO: we're not handling pagination, which means we rely on results being ordered by newest!

    let mut workflow_runs = get_workflow_runs(owner, repository).await?.workflow_runs;
    workflow_runs.sort_by_key(|run| cmp::Reverse(run.updated_at));

    let workflow_runs = workflow_runs.into_iter()
        .filter(|run| filter.test_workflow(&run.name))
        .filter(|run| filter.test_branch(&run.head_branch));

    for run in workflow_runs {
        let mut artifacts = match &run.artifacts_url {
            Some(_) => get_artifacts(owner, repository, &run).await?.artifacts,
            None => continue,
        };
        artifacts.sort_by_key(|artifact| cmp::Reverse(artifact.updated_at));

        let mut artifacts = artifacts.into_iter()
            .filter(|artifact| !artifact.expired)
            .filter(|artifact| filter.test_artifact(&artifact.name));

        for artifact in artifacts {
            if let Some(url) = artifact.archive_download_url {
                return Ok(Some((artifact.id, url, artifact.name)));
            }
        }
    }

    Ok(None)
}

async fn get_workflow_runs(owner: &str, repository: &str) -> Result<WorkflowRunsResponse> {
    let route = format!("repos/{}/{}/actions/runs", owner, repository);
    Ok(octocrab::instance().get(route, None::<&()>).await?)
}

async fn get_artifacts(owner: &str, repository: &str, run: &WorkflowRun) -> Result<ArtifactsResponse> {
    let route = format!("repos/{}/{}/actions/runs/{}/artifacts", owner, repository, run.id);
    Ok(octocrab::instance().get(route, None::<&()>).await?)
}

#[derive(Clone, Debug)]
pub struct Filter {
    pub workflow: Option<String>,
    pub branch: Option<String>,
    pub artifact: Option<String>,
}

impl Filter {
    #[inline]
    pub fn test_workflow(&self, workflow: &str) -> bool {
        self.workflow.as_ref().map(|r| r == workflow).unwrap_or(true)
    }

    #[inline]
    pub fn test_branch(&self, branch: &str) -> bool {
        self.branch.as_ref().map(|r| r == branch).unwrap_or(true)
    }

    #[inline]
    pub fn test_artifact(&self, artifact: &str) -> bool {
        self.artifact.as_ref().map(|r| r == artifact).unwrap_or(true)
    }
}

#[derive(Deserialize, Debug)]
struct WorkflowRunsResponse {
    total_count: usize,
    workflow_runs: Vec<WorkflowRun>,
}

#[derive(Deserialize, Debug)]
struct WorkflowRun {
    id: usize,
    name: String,
    head_branch: String,
    workflow_id: usize,
    artifacts_url: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Deserialize, Debug)]
struct ArtifactsResponse {
    total_count: usize,
    artifacts: Vec<Artifact>,
}

#[derive(Deserialize, Debug)]
struct Artifact {
    id: usize,
    node_id: String,
    name: String,
    size_in_bytes: usize,
    url: String,
    archive_download_url: Option<String>,
    expired: bool,
    created_at: chrono::DateTime<chrono::Utc>,
    expires_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}
