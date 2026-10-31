use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use ristretto_resolver::ArtifactCoordinate;
use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::source;

#[derive(Serialize, Deserialize, Default)]
struct Index {
    entries: Vec<IndexEntry>,
}

async fn read_cache_index<P: AsRef<Path>>(path: P) -> io::Result<Index> {
    let path = path.as_ref();
    if path.exists() {
        let bytes = fs::read(path).await?;
        Ok(serde_json::from_slice(&bytes).expect("malformed cache index"))
    } else {
        Ok(Index::default())
    }
}

async fn write_cache_index<P: AsRef<Path>>(path: P, index: &Index) -> io::Result<()> {
    let path = path.as_ref();
    let bytes = serde_json::to_vec(index).expect("malformed cache index");
    fs::write(path, bytes).await
}

pub struct Loader {
    root: PathBuf,
    old_entries: HashMap<String, IndexEntry>,
    entries: HashMap<String, IndexEntry>,
    used_entries: HashSet<String>,
}

impl Loader {
    pub async fn open<P: Into<PathBuf>>(path: P) -> io::Result<Loader> {
        let root = path.into();
        if !root.exists() {
            tokio::fs::create_dir_all(&root).await?;
        }

        let index = read_cache_index(&root.join("index.json")).await?;
        let entries: HashMap<String, IndexEntry> = index
            .entries
            .into_iter()
            .map(|entry| (entry.key.clone(), entry))
            .collect();

        let old_entries = entries.clone();

        Ok(Loader { root, old_entries, entries, used_entries: HashSet::new() })
    }

    pub fn entry<K: Into<String>>(&mut self, key: K) -> Entry<'_> {
        let key = key.into();
        let current_token = self
            .entries
            .get(&key)
            .map(|entry| entry.token.clone())
            .unwrap_or(Token::Unknown);

        self.used_entries.insert(key.clone());

        Entry {
            loader: self,
            key,
            current_token,
        }
    }

    pub async fn close(mut self) -> io::Result<Vec<Reference>> {
        let stale_entries: Vec<String> = self
            .entries
            .values()
            .filter(|entry| !self.used_entries.contains(&entry.key))
            .map(|entry| entry.key.clone())
            .collect();
        for key in stale_entries {
            let entry = self.entries.remove(&key).unwrap();
            let reference = self.reference_for(&entry);
            fs::remove_file(&reference.path).await?;
        }

        let old_files = self.old_entries.clone().values()
            .map(|entry| self.reference_for(&entry))
            .collect();

        let entries = self.entries.into_values().collect();
        write_cache_index(&self.root.join("index.json"), &Index { entries }).await?;

        Ok(old_files)
    }

    async fn update_entry(
        &mut self,
        key: String,
        token: Token,
        name: String,
        bytes: &[u8],
    ) -> io::Result<Reference> {
        let path = self.path_for(&key);

        fs::write(&path, bytes).await?;

        use std::collections::hash_map::Entry::*;

        match self.entries.entry(key.clone()) {
            Occupied(mut occupied) => {
                let occupied = occupied.get_mut();
                occupied.token = token;
                occupied.file_name = name.clone();
            }
            Vacant(vacant) => {
                vacant.insert(IndexEntry {
                    key,
                    token,
                    file_name: name.clone(),
                });
            }
        }

        Ok(Reference {
            path,
            name,
            changed: true,
        })
    }

    fn get_reference(&self, key: &str) -> Option<Reference> {
        self.entries.get(key).map(|entry| self.reference_for(entry))
    }

    fn reference_for(&self, entry: &IndexEntry) -> Reference {
        let path = self.path_for(&entry.key);
        let name = entry.file_name.clone();
        Reference {
            path,
            name,
            changed: false,
        }
    }

    #[inline]
    fn path_for(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct IndexEntry {
    key: String,
    token: Token,
    file_name: String,
}

pub struct Entry<'a> {
    loader: &'a mut Loader,
    key: String,
    current_token: Token,
}

impl<'a> Entry<'a> {
    pub fn current_token(&self) -> &Token {
        &self.current_token
    }

    pub fn try_update(self, token: Token) -> UpdateResult<'a> {
        if self.current_token != token {
            println!(
                "[{}] cache mismatched! new: {:?}, old: {:?}",
                self.key, token, self.current_token
            );
            UpdateResult::Mismatch(EntryUpdater { entry: self, token })
        } else {
            println!("[{}] cache matched! {:?}", self.key, token);
            let reference = self.loader.get_reference(&self.key).unwrap();
            UpdateResult::Match(reference)
        }
    }

    pub fn get_existing(self) -> Option<Reference> {
        self.loader.get_reference(&self.key)
    }

    async fn update(&mut self, token: Token, name: String, bytes: &[u8]) -> io::Result<Reference> {
        self.loader
            .update_entry(self.key.clone(), token, name, bytes)
            .await
    }
}

pub struct Reference {
    path: PathBuf,
    name: String,
    changed: bool,
}

impl Reference {
    pub async fn copy_to<P: AsRef<Path>>(&self, root: P) -> io::Result<()> {
        fs::copy(&self.path, self.resolve_target_path(root)).await?;
        Ok(())
    }

    pub async fn remove_from<P: AsRef<Path>>(&self, root: P) -> io::Result<()> {
        let target = self.resolve_target_path(root);
        if target.exists() {
            fs::remove_file(target).await
        } else {
            Ok(())
        }
    }

    fn resolve_target_path<P: AsRef<Path>>(&self, root: P) -> PathBuf {
        root.as_ref().join(&self.name)
    }

    pub fn changed(&self) -> bool {
        self.changed
    }
}

pub struct EntryUpdater<'a> {
    entry: Entry<'a>,
    token: Token,
}

impl<'a> EntryUpdater<'a> {
    pub async fn update(mut self, file: source::File) -> io::Result<Reference> {
        self.entry
            .update(self.token, file.name, file.bytes.as_ref())
            .await
    }
}

pub enum UpdateResult<'a> {
    Mismatch(EntryUpdater<'a>),
    Match(Reference),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Token {
    #[serde(rename = "etag")]
    Etag(String),
    #[serde(rename = "artifact")]
    ArtifactId(usize),
    #[serde(rename = "sha1")]
    Sha1([u8; 20]),
    #[serde(rename = "sha512")]
    Sha512(String),
    #[serde(rename = "maven")]
    Maven { coordinate: ArtifactCoordinate, file: String },
    #[serde(rename = "unknown")]
    Unknown,
}

impl PartialEq for Token {
    fn eq(&self, right: &Token) -> bool {
        use Token::*;
        match (self, right) {
            (Etag(left), Etag(right)) => left == right,
            (ArtifactId(left), ArtifactId(right)) => left == right,
            (Sha1(left), Sha1(right)) => left == right,
            (Sha512(left), Sha512(right)) => left == right,
            (
                Maven { coordinate: left_coord, file: left_file },
                Maven { coordinate: right_coord, file: right_file }
            ) => left_coord == right_coord && left_file == right_file,
            (_, _) => false,
        }
    }
}

impl Eq for Token {}
