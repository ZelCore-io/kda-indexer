//! Index schema, tokenizer and the document writer (plan Task 8 "Schema").
//!
//! | Field | Type | Options |
//! |---|---|---|
//! | `hash` | bytes | indexed (raw), stored, fast: the full 16-byte CodeHash |
//! | `code` | text | tokenizer `ngram3` (3-grams of chars, no lowercasing), doc ids only, stored |
//! | `first_h` | u64 | fast: height of the body's first tx |
//! | `last_h` | u64 | fast: a height at or below the body's last tx when the doc was written |
//! | `module` | u64 | fast: 1 for a module or interface deployment |
//!
//! The doc store is zstd-compressed. Field norms are off (no scoring).

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use tantivy::schema::{
    BytesOptions, Field, IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions,
};
use tantivy::store::{Compressor, ZstdCompressor};
use tantivy::tokenizer::{NgramTokenizer, TextAnalyzer};
use tantivy::{
    Index, IndexReader, IndexSettings, IndexWriter, ReloadPolicy, TantivyDocument, Term,
};

use crate::is_module;

/// Tokenizer name of the `code` field (registered on every open; tantivy does not persist it).
pub const NGRAM: &str = "ngram3";

#[derive(Debug, Clone, Copy)]
pub struct Fields {
    pub hash: Field,
    pub code: Field,
    pub first_h: Field,
    pub last_h: Field,
    pub module: Field,
}

pub fn schema() -> Schema {
    let mut b = Schema::builder();
    b.add_bytes_field(
        "hash",
        BytesOptions::default()
            .set_indexed()
            .set_stored()
            .set_fast(),
    );
    let code = TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(NGRAM)
                .set_index_option(IndexRecordOption::Basic)
                .set_fieldnorms(false),
        )
        .set_stored();
    b.add_text_field("code", code);
    let fast = NumericOptions::default().set_fast();
    b.add_u64_field("first_h", fast.clone());
    b.add_u64_field("last_h", fast.clone());
    b.add_u64_field("module", fast);
    b.build()
}

fn fields(s: &Schema) -> Result<Fields> {
    let f = |n: &str| {
        s.get_field(n)
            .with_context(|| format!("search schema has no {n}"))
    };
    Ok(Fields {
        hash: f("hash")?,
        code: f("code")?,
        first_h: f("first_h")?,
        last_h: f("last_h")?,
        module: f("module")?,
    })
}

fn register(index: &Index) -> Result<()> {
    let t = NgramTokenizer::new(3, 3, false)?;
    index.tokenizers().register(NGRAM, TextAnalyzer::from(t));
    Ok(())
}

/// A new, empty index in `dir` (created).
pub fn create(dir: &Path) -> Result<(Index, Fields)> {
    std::fs::create_dir_all(dir)?;
    let settings = IndexSettings {
        docstore_compression: Compressor::Zstd(ZstdCompressor {
            compression_level: None,
        }),
        ..Default::default()
    };
    let index = Index::builder()
        .schema(schema())
        .settings(settings)
        .create_in_dir(dir)
        .with_context(|| format!("create search index in {}", dir.display()))?;
    register(&index)?;
    let f = fields(&index.schema())?;
    Ok((index, f))
}

/// The index in `dir`.
pub fn open(dir: &Path) -> Result<(Index, Fields)> {
    let index = Index::open_in_dir(dir)
        .with_context(|| format!("open search index in {}", dir.display()))?;
    register(&index)?;
    let f = fields(&index.schema())?;
    Ok((index, f))
}

/// One document: a distinct code body.
#[derive(Debug, Clone)]
pub struct Doc {
    pub hash: [u8; 16],
    pub code: String,
    pub first_h: u64,
    pub last_h: u64,
}

pub fn to_doc(f: &Fields, d: &Doc) -> TantivyDocument {
    let mut t = TantivyDocument::default();
    t.add_bytes(f.hash, &d.hash[..]);
    t.add_text(f.code, &d.code);
    t.add_u64(f.first_h, d.first_h);
    t.add_u64(f.last_h, d.last_h);
    t.add_u64(f.module, u64::from(is_module(&d.code)));
    t
}

/// Memory budget per tantivy writer thread must be at least this (tantivy's minimum is 15 MB).
const MIN_HEAP_PER_THREAD: usize = 16 << 20;

pub fn writer(index: &Index, threads: usize, heap_mb: usize) -> Result<IndexWriter> {
    let threads = threads.max(1);
    let heap = (heap_mb << 20).max(MIN_HEAP_PER_THREAD * threads);
    Ok(index.writer_with_num_threads(threads, heap)?)
}

/// The tail stage's writer on the live `search/`: adds a doc only for a hash the index (or
/// this writer since its last commit) does not hold.
pub struct SearchWriter {
    pub index: Index,
    pub fields: Fields,
    writer: IndexWriter,
    reader: IndexReader,
    pending: HashSet<[u8; 16]>,
}

impl SearchWriter {
    pub fn open(dir: &Path, heap_mb: usize) -> Result<SearchWriter> {
        let (index, fields) = open(dir)?;
        let writer = writer(&index, 1, heap_mb)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        Ok(SearchWriter {
            index,
            fields,
            writer,
            reader,
            pending: HashSet::new(),
        })
    }

    pub fn contains(&self, hash: &[u8; 16]) -> Result<bool> {
        if self.pending.contains(hash) {
            return Ok(true);
        }
        let t = Term::from_field_bytes(self.fields.hash, &hash[..]);
        Ok(self.reader.searcher().doc_freq(&t)? > 0)
    }

    /// Adds `d` unless its hash is present; true when added.
    pub fn add(&mut self, d: &Doc) -> Result<bool> {
        if self.contains(&d.hash)? {
            return Ok(false);
        }
        self.writer.add_document(to_doc(&self.fields, d))?;
        self.pending.insert(d.hash);
        Ok(true)
    }

    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// Commit, then make the committed docs visible to `contains`.
    pub fn commit(&mut self) -> Result<()> {
        self.writer.commit()?;
        self.reader.reload()?;
        self.pending.clear();
        Ok(())
    }

    pub fn num_docs(&self) -> u64 {
        self.reader.searcher().num_docs()
    }
}
