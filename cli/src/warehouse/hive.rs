use super::{metadatasource::MetadataSource, whutils::get_name};
use crate::serialization::{Deserialize, Serialize, YamlWriter};
use crate::utils;
use colored::*;

#[derive(Serialize, Deserialize)]
pub struct HiveMetastore {
    name: String,
    metadata_source: MetadataSource,
    uri: String,
    port: i32,
    dialect: String,
    username: Option<String>,
    password: Option<String>,
    database: Option<String>,
}

impl HiveMetastore {
    pub fn prompt_add_details() {
        println!(
            "Starting warehouse detail onboarding sequence for {}.",
            "Hive Metastore".yellow()
        );

        // name
        let name: String = get_name();

        // uri
        let uri_msg = "What is the URI for this metastore?";
        let uri: String = utils::get_input_with_message(uri_msg);

        // port
        let port_msg = "Port?";
        let port_str: String = utils::get_input_with_message(port_msg).trim().to_string();
        let port: i32 = port_str.parse::<i32>().unwrap();

        // username
        let username: Option<String>;
        let username_msg = "Username? [default: None]";
        let username_tmp = utils::get_input_with_message(username_msg);
        if username_tmp == "" {
            username = None;
        } else {
            username = Some(username_tmp);
        }

        let password: Option<String>;
        let password_msg = "Password? [default: None]";
        let password_tmp = utils::get_input_with_message(password_msg);
        if password_tmp == "" {
            password = None;
        } else {
            password = Some(password_tmp);
        }

        let dialect_msg = "What is the dialect of this metastore? E.g. postgres. This is the dialect type that is used in the SQLAlchemy connection string. See https://docs.sqlalchemy.org/en/13/core/engines.html for more details.";
        let dialect = utils::get_input_with_message(&dialect_msg);

        let database: Option<String>;
        let database_msg = "Is there a database within this connection that contains the metastore? This usually is the case for hive metastores, and it is usually called 'hive'. If in doubt, enter 'hive'.";
        let database_msg = format!("{} {}", database_msg, "[default: None]");
        let database_tmp = utils::get_input_with_message(&database_msg);
        if database_tmp == "" {
            database = None;
        } else {
            database = Some(database_tmp);
        }

        let compiled_config = HiveMetastore {
            name,
            metadata_source: MetadataSource::HiveMetastore,
            dialect,
            uri,
            port,
            username,
            password,
            database,
        };

        compiled_config
            .register_connection()
            .expect("Failed to register warehouse configuration");

        println!(
            "Added warehouse: {:?} to ~/.whale/config/connections.yaml.",
            compiled_config.name,
        );
    }
}
