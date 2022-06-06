use std::{collections::HashSet, fmt::Debug};

use log::debug;
use registry_provider::*;
use tantivy::{
    collector::TopDocs,
    doc,
    query::{BooleanQuery, Query, QueryParser, TermQuery},
    schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, STRING, TEXT},
    tokenizer::{PreTokenizedString, Tokenizer, WhitespaceTokenizer},
    Index, IndexReader, IndexWriter, ReloadPolicy, Term,
};
use thiserror::Error;
use uuid::Uuid;

fn pre_tokenize_text(text: &str) -> PreTokenizedString {
    let mut tokens = vec![];
    let mut token_stream = WhitespaceTokenizer.token_stream(text);
    while token_stream.advance() {
        tokens.push(token_stream.token().clone());
    }
    PreTokenizedString {
        text: text.to_string(),
        tokens,
    }
}

#[derive(Debug, Error)]
pub enum FtsError {
    #[error(transparent)]
    TantivyError(#[from] tantivy::TantivyError),

    #[error(transparent)]
    QueryParseError(#[from] tantivy::query::QueryParserError),
}

pub struct FtsIndex {
    _schema: Schema,
    reader: IndexReader,
    writer: Option<IndexWriter>,
    index: Index,
    name_field: Field,
    id_field: Field,
    scopes_field: Field,
    type_field: Field,
    body_field: Field,
    enabled: bool,
}

impl Debug for FtsIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtsIndex")
            .field("_schema", &self._schema)
            .field("index", &self.index)
            .field("name_field", &self.name_field)
            .field("id_field", &self.id_field)
            .field("scopes_field", &self.scopes_field)
            .field("type_field", &self.type_field)
            .field("body_field", &self.body_field)
            .field("enabled", &self.enabled)
            .finish()
    }
}

impl FtsIndex {
    pub fn new() -> Self {
        let indexing_option = TextFieldIndexing::default()
            .set_tokenizer("en_stem")
            .set_index_option(IndexRecordOption::Basic);
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("name", TEXT.set_indexing_options(indexing_option.clone()));
        schema_builder.add_text_field("id", STRING.set_stored());
        schema_builder.add_text_field("scopes", TEXT);
        schema_builder.add_text_field("type", STRING);
        schema_builder.add_text_field("body", TEXT.set_indexing_options(indexing_option.clone()));
        let schema = schema_builder.build();
        let name_field = schema.get_field("name").unwrap();
        let id_field = schema.get_field("id").unwrap();
        let scopes_field = schema.get_field("scopes").unwrap();
        let type_field = schema.get_field("type").unwrap();
        let body_field = schema.get_field("body").unwrap();
        let index = Index::create_in_ram(schema.clone());
        Self {
            _schema: schema,
            reader: index
                .reader_builder()
                .reload_policy(ReloadPolicy::OnCommit)
                .try_into()
                .unwrap(),
            writer: None,
            index,
            name_field,
            id_field,
            scopes_field,
            type_field,
            body_field,
            enabled: true,
        }
    }

    #[allow(dead_code)]
    pub fn enable(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    pub fn add_doc<T: ToDoc>(&mut self, d: &T) -> Result<(), FtsError> {
        if self.writer.is_none() {
            self.writer = Some(self.index.writer(30_000_000).unwrap());
        }
        self.writer.as_ref().unwrap().add_document(doc!(
            self.name_field => d.get_name(),
            self.id_field => d.get_id(),
            self.scopes_field => pre_tokenize_text(&d.get_scopes().join(" ")),
            self.type_field => d.get_type(),
            self.body_field => d.get_body(),
        ))?;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), FtsError> {
        if let Some(writer) = &mut self.writer {
            writer.commit()?;
        }
        self.writer = None;
        Ok(())
    }

    pub fn index<T: ToDoc + Debug>(&mut self, doc: &T) -> Result<(), FtsError> {
        if !self.enabled {
            return Ok(());
        }
        self.add_doc(doc)?;
        self.commit()?;
        Ok(())
    }

    pub fn search(
        &self,
        q: &str,
        types: HashSet<String>,
        scope: Option<String>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Uuid>, FtsError> {
        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(
            &self.index,
            vec![self.name_field, self.id_field, self.body_field],
        );
        let query = if types.is_empty() {
            match scope {
                Some(id) => Box::new(BooleanQuery::intersection(vec![
                    query_parser.parse_query(q)?,
                    Box::new(TermQuery::new(
                        Term::from_field_text(self.scopes_field, &id),
                        IndexRecordOption::Basic,
                    )),
                ])),
                None => query_parser.parse_query(q)?,
            }
        } else {
            let type_queries = types
                .into_iter()
                .map(|t| -> Box<dyn Query> {
                    Box::new(TermQuery::new(
                        Term::from_field_text(self.type_field, &t),
                        IndexRecordOption::Basic,
                    ))
                })
                .collect();
            match scope {
                Some(id) => Box::new(BooleanQuery::intersection(vec![
                    query_parser.parse_query(q)?,
                    Box::new(TermQuery::new(
                        Term::from_field_text(self.scopes_field, &id),
                        IndexRecordOption::Basic,
                    )),
                    Box::new(BooleanQuery::union(type_queries)),
                ])),
                None => Box::new(BooleanQuery::intersection(vec![
                    query_parser.parse_query(q)?,
                    Box::new(BooleanQuery::union(type_queries)),
                ])),
            }
        };
        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit).and_offset(offset))?;
        Ok(top_docs
            .into_iter()
            .filter_map(|(_, addr)| {
                let doc = searcher.doc(addr).ok();
                doc.map(|d| {
                    d.into_iter()
                        .find(|f| f.field == self.id_field)
                        .map(|f| {
                            debug!("Found id: {}", f.value.as_text().unwrap_or_default());
                            f.value.as_text().map(|s| Uuid::parse_str(s).ok())
                        })
                        .flatten()
                        .flatten()
                })
                .flatten()
            })
            .collect())
    }
}

impl Default for FtsIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use common_utils::{init_logger, set};
    use registry_provider::ToDoc;

    #[derive(Clone, Debug)]
    struct A {
        name: String,
        id: String,
        scopes: Vec<String>,
        type_: String,
        body: String,
    }

    impl ToDoc for A {
        fn get_name(&self) -> String {
            self.name.to_owned()
        }
        fn get_id(&self) -> String {
            self.id.to_owned()
        }
        fn get_scopes(&self) -> Vec<String> {
            self.scopes.to_owned()
        }
        fn get_type(&self) -> String {
            self.type_.to_owned()
        }
        fn get_body(&self) -> String {
            self.body.to_owned()
        }
    }
    #[test]
    fn scoped_search() {
        init_logger();
        let mut fts = FtsIndex::new();
        let mut docs: HashMap<Uuid, A> = HashMap::new();
        for i in 1..11 {
            let id = Uuid::new_v4();
            let a = A {
                name: format!("some name{}", i),
                id: id.to_string(),
                scopes: vec![format!("scope-{}", i % 2), format!("scope-{}", i % 5)],
                type_: format!("SomeType{}", i % 2),
                body: format!("This is the body of name{}", i),
            };
            docs.insert(id, a.clone());
            fts.add_doc(&a).unwrap();
        }
        fts.commit().unwrap();
        let ids = fts
            .search(
                "body",
                set!["SomeType1".to_string()],
                Some("scope-2".to_string()),
                10,
                0,
            )
            .unwrap();
        for id in ids {
            assert_eq!(docs[&id].type_, "SomeType1");
            assert!(docs[&id].scopes.contains(&"scope-2".to_string()));
        }
    }
}
