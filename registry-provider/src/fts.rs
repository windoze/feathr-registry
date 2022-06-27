use std::fmt::Debug;

use crate::{
    models::{
        AnchorAttributes, AnchorFeatureAttributes, Attributes, DerivedFeatureAttributes,
        EntityProperty, ProjectAttributes, SourceAttributes,
    },
    Entity,
};

/**
 * Convert the entity to FTS doc
 */
pub trait ToDoc {
    fn get_name(&self) -> String;
    fn get_id(&self) -> String;
    fn get_type(&self) -> String;
    fn get_body(&self) -> String;
}

/**
 * Convert property to one doc line in the FTS doc
 */
pub trait ToDocString {
    fn to_doc_string(&self) -> String;
}

impl<T> ToDoc for Entity<T>
where
    T: ToDocString + Clone + Debug + PartialEq + Eq,
{
    fn get_name(&self) -> String {
        vec![process_name(&self.name), process_name(&self.qualified_name)].join("\n")
    }

    fn get_id(&self) -> String {
        self.id.to_string()
    }

    fn get_type(&self) -> String {
        format!("{:?}", self.entity_type)
    }

    fn get_body(&self) -> String {
        self.properties.to_doc_string()
    }
}

impl ToDocString for ProjectAttributes {
    fn to_doc_string(&self) -> String {
        vec![process_name(&self.name), process_name(&self.qualified_name)].join("\n")
    }
}

impl ToDocString for AnchorAttributes {
    fn to_doc_string(&self) -> String {
        vec![process_name(&self.name), process_name(&self.qualified_name)].join("\n")
    }
}

impl ToDocString for SourceAttributes {
    fn to_doc_string(&self) -> String {
        vec![process_name(&self.name), process_name(&self.qualified_name)].join("\n")
    }
}

impl ToDocString for AnchorFeatureAttributes {
    fn to_doc_string(&self) -> String {
        vec![process_name(&self.name), process_name(&self.qualified_name)].join("\n")
    }
}

impl ToDocString for DerivedFeatureAttributes {
    fn to_doc_string(&self) -> String {
        vec![process_name(&self.name), process_name(&self.qualified_name)].join("\n")
    }
}

impl ToDocString for Attributes {
    fn to_doc_string(&self) -> String {
        match self {
            Attributes::AnchorFeature(v) => v.to_doc_string(),
            Attributes::DerivedFeature(v) => v.to_doc_string(),
            Attributes::Anchor(v) => v.to_doc_string(),
            Attributes::Source(v) => v.to_doc_string(),
            Attributes::Project(v) => v.to_doc_string(),
        }
    }
}

impl ToDocString for EntityProperty {
    fn to_doc_string(&self) -> String {
        let mut v = vec![
            self.display_text.to_owned(),
            self.attributes.to_doc_string(),
        ];
        v.extend(self.labels.iter().cloned());
        v.join("\n")
    }
}

/**
 * Keep both original string and processed string
 */
fn process_name(s: &str) -> String {
    format!("{}\n{}", s, s.replace('_', " "))
}
