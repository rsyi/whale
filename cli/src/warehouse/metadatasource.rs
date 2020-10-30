use super::errors::ParseMetadataSourceError;
use crate::serialization::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Serialize, Deserialize)]
pub enum MetadataSource {
    Bigquery,
    GitServer,
    Glue,
    Hive,
    HiveMetastore,
    Postgres,
    Presto,
    Redshift,
    Snowflake,
    AmundsenNeo4j,
    BuildScript,
}

impl FromStr for MetadataSource {
    type Err = ParseMetadataSourceError;
    fn from_str(warehouse: &str) -> Result<Self, Self::Err> {
        match warehouse {
            "bigquery" | "bq" | "b" => Ok(MetadataSource::Bigquery),
            "glue" => Ok(MetadataSource::Glue),
            "hive" | "h" => Ok(MetadataSource::Hive),
            "hive_metastore" | "hive-metastore" | "hive metastore" | "hm" => {
                Ok(MetadataSource::HiveMetastore)
            }
            "postgres" | "po" => Ok(MetadataSource::Postgres),
            "presto" | "pr" => Ok(MetadataSource::Presto),
            "redshift" | "r" => Ok(MetadataSource::Redshift),
            "snowflake" | "s" => Ok(MetadataSource::Snowflake),
            "amundsen-neo4j" | "a" => Ok(MetadataSource::AmundsenNeo4j),
            "git" | "g" => Ok(MetadataSource::GitServer),
            "build-script" | "bs" => Ok(MetadataSource::BuildScript),
            _ => Err(ParseMetadataSourceError {}),
        }
    }
}

impl fmt::Display for MetadataSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            MetadataSource::Bigquery => write!(f, "Bigquery"),
            MetadataSource::Glue => write!(f, "Glue"),
            MetadataSource::Hive => write!(f, "Hive"),
            MetadataSource::HiveMetastore => write!(f, "Hive Metastore"),
            MetadataSource::Postgres => write!(f, "Postgres"),
            MetadataSource::Presto => write!(f, "Presto"),
            MetadataSource::Redshift => write!(f, "Redshift"),
            MetadataSource::Snowflake => write!(f, "Snowflake"),
            MetadataSource::AmundsenNeo4j => write!(f, "Amundsen Neo4j"),
            MetadataSource::GitServer => write!(f, "Git Server"),
            MetadataSource::BuildScript => write!(f, "Custom Build Script"),
        }
    }
}
