use std::collections::BTreeMap;

use serde::{de::Deserializer, Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Extraction {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    #[serde(default)]
    pub hyperedges: Vec<serde_json::Value>,
    #[serde(default)]
    pub input_tokens: u32,
    #[serde(default)]
    pub output_tokens: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub file_type: String,
    #[serde(default)]
    pub source_file: String,
    #[serde(default)]
    pub source_location: Option<String>,
    #[serde(default)]
    pub node_type: Option<String>,
    #[serde(default)]
    pub docstring: Option<String>,
    #[serde(default)]
    pub parameters: Vec<String>,
    #[serde(default)]
    pub signature: Option<String>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Edge {
    pub source: String,
    pub target: String,
    pub relation: String,
    pub confidence: String,
    #[serde(default)]
    pub source_file: String,
    #[serde(default, rename = "_src")]
    pub original_source: Option<String>,
    #[serde(default, rename = "_tgt")]
    pub original_target: Option<String>,
    #[serde(default)]
    pub source_location: Option<String>,
    #[serde(default)]
    pub confidence_score: Option<f64>,
    #[serde(skip)]
    pub confidence_score_present: bool,
    #[serde(default = "default_weight")]
    pub weight: f64,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

fn default_weight() -> f64 {
    1.0
}

impl<'de> Deserialize<'de> for Edge {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        fn take_required_string<E>(
            map: &mut BTreeMap<String, serde_json::Value>,
            key: &str,
        ) -> Result<String, E>
        where
            E: serde::de::Error,
        {
            match map.remove(key) {
                Some(serde_json::Value::String(value)) => Ok(value),
                Some(_) => Err(E::custom(format!("expected string for `{key}`"))),
                None => Err(E::custom(format!("missing field `{key}`"))),
            }
        }

        fn take_optional_string<E>(
            map: &mut BTreeMap<String, serde_json::Value>,
            key: &str,
        ) -> Result<Option<String>, E>
        where
            E: serde::de::Error,
        {
            match map.remove(key) {
                Some(serde_json::Value::String(value)) => Ok(Some(value)),
                Some(serde_json::Value::Null) | None => Ok(None),
                Some(_) => Err(E::custom(format!("expected string or null for `{key}`"))),
            }
        }

        fn take_optional_f64<E>(
            map: &mut BTreeMap<String, serde_json::Value>,
            key: &str,
        ) -> Result<(Option<f64>, bool), E>
        where
            E: serde::de::Error,
        {
            match map.remove(key) {
                Some(serde_json::Value::Number(value)) => value
                    .as_f64()
                    .map(|n| (Some(n), true))
                    .ok_or_else(|| E::custom(format!("invalid number for `{key}`"))),
                Some(serde_json::Value::Null) => Ok((None, true)),
                None => Ok((None, false)),
                Some(_) => Err(E::custom(format!("expected number or null for `{key}`"))),
            }
        }

        fn take_weight<E>(map: &mut BTreeMap<String, serde_json::Value>) -> Result<f64, E>
        where
            E: serde::de::Error,
        {
            match map.remove("weight") {
                Some(serde_json::Value::Number(value)) => value
                    .as_f64()
                    .ok_or_else(|| E::custom("invalid number for `weight`")),
                Some(serde_json::Value::Null) | None => Ok(default_weight()),
                Some(_) => Err(E::custom("expected number or null for `weight`")),
            }
        }

        let mut map = BTreeMap::<String, serde_json::Value>::deserialize(deserializer)?;
        let source = take_required_string::<D::Error>(&mut map, "source")?;
        let target = take_required_string::<D::Error>(&mut map, "target")?;
        let relation = take_required_string::<D::Error>(&mut map, "relation")?;
        let confidence = take_required_string::<D::Error>(&mut map, "confidence")?;
        let source_file = take_optional_string::<D::Error>(&mut map, "source_file")?.unwrap_or_default();
        let original_source = take_optional_string::<D::Error>(&mut map, "_src")?;
        let original_target = take_optional_string::<D::Error>(&mut map, "_tgt")?;
        let source_location = take_optional_string::<D::Error>(&mut map, "source_location")?;
        let (confidence_score, confidence_score_present) =
            take_optional_f64::<D::Error>(&mut map, "confidence_score")?;
        let weight = take_weight::<D::Error>(&mut map)?;

        Ok(Self {
            source,
            target,
            relation,
            confidence,
            source_file,
            original_source,
            original_target,
            source_location,
            confidence_score,
            confidence_score_present,
            weight,
            extra: map,
        })
    }
}
