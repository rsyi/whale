use super::{metadatasource::MetadataSource, whutils::get_name};
use crate::serialization::{Deserialize, Serialize, YamlWriter};
use crate::utils;
use colored::*;

#[derive(Serialize, Deserialize)]
pub struct GenericWarehouse {
    name: String,
    metadata_source: MetadataSource,
    uri: String,
    port: i32,
    username: Option<String>,
    password: Option<String>,
    database: Option<String>,
}

impl GenericWarehouse {
    pub fn prompt_add_details(metadata_source: MetadataSource) {
        let metadata_source_str = metadata_source.to_string();
        println!(
            "Starting warehouse detail onboarding sequence for {}.",
            metadata_source_str.yellow()
        );

        // name
        let name: String = get_name();

        // uri
        let uri_msg = "What is the URI for this warehouse?";
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

        let database: Option<String>;
        let database_msg = {
            "[Optional] Enter a catalog/database within this connection if you'd like to restrict your metadata scrapes to only this catalog/database. By default, whale will otherwise scrape all metadata from all available catalogs/databases."
        };
        let database_msg = format!("{} {}", database_msg, "[default: None]");
        let database_tmp = utils::get_input_with_message(&database_msg);
        if database_tmp == "" {
            database = None;
        } else {
            database = Some(database_tmp);
        }

        let compiled_config = GenericWarehouse {
            name,
            metadata_source,
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
            compiled_config.name
        );
    }
}
