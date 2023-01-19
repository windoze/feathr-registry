mod entity;
mod edge;
mod attributes;
mod entity_prop;
mod entity_def;

pub use entity::*;
pub use edge::*;
pub use attributes::*;
pub use entity_prop::*;
pub use entity_def::*;

pub const PROJECT_TYPE: &str = "feathr_workspace_v1";
pub const ANCHOR_TYPE: &str = "feathr_anchor_v1";
pub const ANCHOR_FEATURE_TYPE: &str = "feathr_anchor_feature_v1";
pub const DERIVED_FEATURE_TYPE: &str = "feathr_derived_feature_v1";
pub const SOURCE_TYPE: &str = "feathr_source_v1";


#[cfg(test)]
mod tests {
    use crate::{models::*, Entity};

    #[test]
    fn des_trans() {
        let s = r#"{
            "filter": null,
            "agg_func": "AVG",
            "limit": null,
            "group_by": null,
            "window": "90d",
            "def_expr": "cast_float(fare_amount)"
        }"#;

        let t: FeatureTransformation = serde_json::from_str(s).unwrap();
        println!("{:#?}", t);
    }
}
