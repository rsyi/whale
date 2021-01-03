use super::{metadatasource::MetadataSource, whutils::get_name};
use crate::serialization::{Deserialize, Serialize, YamlWriter};
use crate::utils;

#[derive(Serialize, Deserialize)]
pub struct Snowflake {
    name: String,
    metadata_source: MetadataSource,
    account: String,
    username: String,
    password: String,
    database: String,
}

impl Snowflake {
    pub fn prompt_add_details() {

        // name
        let name: String = get_name();

        // account
        let account_msg = "What is the account for this warehouse (if your snowflake URL is robert123.snowflakecomputing.com, robert123 is your account)?";
        let account: String = utils::get_input_with_message(account_msg);

        // username
        let username_msg = "Username?";
        let username: String = utils::get_input_with_message(username_msg);

        // password
        let password_msg = "Password?";
        let password: String = utils::get_input_with_message(password_msg);

        // database
        let database_msg = {
            "Database? (In ANSI standard, this is the 'catalog', but Snowflake calls it a 'database'.)"
        };
        let database: String = utils::get_input_with_message(database_msg);

        let compiled_config = Snowflake {
            name,
            metadata_source: MetadataSource::Snowflake,
            account,
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
