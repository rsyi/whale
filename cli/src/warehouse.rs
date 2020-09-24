extern crate colored;
extern crate names;
extern crate serde;
extern crate serde_yaml;
use colored::*;
use names::{Generator, Name};
use serde::{Serialize, Deserialize};
use std::{env, fmt, io};
use std::io::Write;
use std::fs::OpenOptions;
use std::path::Path;
use std::process;
use std::str::FromStr;

use crate::utils;

const GOOGLE_ENV_VAR: &str = "GOOGLE_APPLICATION_CREDENTIALS";

#[derive(Debug, Clone)]
pub struct ParseMetadataSourceError {}

impl fmt::Display for ParseMetadataSourceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid metadata source.")
    }
}


#[derive(Serialize, Deserialize)]
pub enum MetadataSource {
    Bigquery,
    Hive,
    HiveMetastore,
    Presto,
    Snowflake
}

impl FromStr for MetadataSource {
    type Err = ParseMetadataSourceError;
    fn from_str(warehouse: &str) -> Result<Self, Self::Err> {
        match warehouse {
            "bigquery" | "bq" => Ok(MetadataSource::Bigquery),
            "hive" | "h" => Ok(MetadataSource::Hive),
            "hive_metastore" | "hive-metastore" | "hive metastore"  | "hm" => Ok(MetadataSource::HiveMetastore),
            "presto" | "p" => Ok(MetadataSource::Presto),
            "snowflake" | "s" => Ok(MetadataSource::Snowflake),
            _ => Err(ParseMetadataSourceError {}),
        }
    }
}

impl fmt::Display for MetadataSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
       match *self {
           MetadataSource::Bigquery => write!(f, "Bigquery"),
           MetadataSource::Hive => write!(f, "Hive"),
           MetadataSource::HiveMetastore => write!(f, "Hive Metastore"),
           MetadataSource::Presto => write!(f, "Presto"),
           MetadataSource::Snowflake => write!(f, "Snowflake"),
       }
    }
}

pub fn prompt_add_warehouse(is_first_time: bool) {

    // Text

    if !is_first_time {
        println!("{} [{}/{}]",
                 "\nAdd another warehouse?".purple(),
                 "Y".green(),
                 "n".red());
        let has_warehouse_to_add: bool = utils::get_input_as_bool();
        if !has_warehouse_to_add {
            return ()
        }
    }

    println!("\n{}", "What type of warehouse would you like to add?".purple());
    println!(" {}:", "Options".bold());

    let supported_warehouse_types = [
        "bigquery",
        "hive",
        "hive_metastore",
        "presto",
        "snowflake"
    ];
    for supported_warehouse_type in supported_warehouse_types.iter() {
        println!(" * {}", supported_warehouse_type)
    }

    // Get input

    let raw_warehouse_type_input: String = utils::get_input();
    let warehouse_type = raw_warehouse_type_input.trim();
    let warehouse_enum = MetadataSource::from_str(warehouse_type);
    println!("You entered: {}", warehouse_type.yellow());

    match warehouse_enum {
        Ok(MetadataSource::Bigquery) => Bigquery::prompt_add_details(),
        Ok(MetadataSource::Hive)
            | Ok(MetadataSource::HiveMetastore)
            | Ok(MetadataSource::Presto)
            | Ok(MetadataSource::Snowflake)
            => GenericWarehouse::prompt_add_details(warehouse_enum.unwrap()),
        Err(e) => handle_error(e),
    };

    fn handle_error(e: ParseMetadataSourceError) {
        eprintln!("{} {}", "WARNING:".red(), e);
        println!("Try again.");
        prompt_add_warehouse(true);
        process::exit(1)
    }
    prompt_add_warehouse(false);

}


fn get_name() -> String {
    println!("\n{}", "Input a name for your warehouse (e.g. bq-1)".purple());
    let mut name = utils::get_input();
    if name == "" {
        let mut generator = Generator::with_naming(Name::Plain);
        name = generator.next().unwrap();
        println!("Using randomly generated name: {}", name.green());
    }
    name.trim().to_string()
}

pub trait YamlWriter {
    fn register_config(&self) -> Result<(), io::Error>{

        // TODO: refer to filesystem.rs instead of hard-coding
        let base_path = shellexpand::tilde("~");
        let base_path = Path::new(&*base_path);
        let connections_path = base_path
            .join(".whale")
            .join("config")
            .join("connections.yaml");


        // Format yaml docs
        let mut new_docs = self.generate_yaml();

        // Open yaml file
        let mut file = OpenOptions::new().write(true)
                                 .create(true)
                                 .append(true)
                                 .open(connections_path)?;

        new_docs.push_str("\n");
        file.write_all(new_docs.as_bytes())?;
        Ok(())

    }

    fn generate_yaml(&self) -> String;

}

impl<T> YamlWriter for T
    where T: Serialize {
    fn generate_yaml(&self) -> String {
        serde_yaml::to_string(&self)
            .unwrap()
    }
}

#[derive(Serialize, Deserialize)]
pub struct GenericWarehouse {
    name: String,
    metadata_source: MetadataSource,
    uri: String,
    port: i32,
    username: Option<String>,
    password: Option<String>
}

impl GenericWarehouse {
    pub fn prompt_add_details(metadata_source: MetadataSource) {
        let metadata_source_str = metadata_source.to_string();
        println!("Starting warehouse detail onboarding sequence for {}.",
                 metadata_source_str.yellow());

        // name
        let name: String = get_name();

        // uri
        let uri_msg = "What is the URI for this warehouse?";
        let uri: String = utils::get_input_with_message(uri_msg);

        // port
        let port_msg = "Port?";
        let port_str: String = utils::get_input_with_message(port_msg)
            .to_string()
            .trim()
            .to_string();
        let port: i32 = port_str.parse::<i32>().unwrap();

        // username
        let username: Option<String>;
        let username_msg = "Username? [default: None]";
        let username_tmp = utils::get_input_with_message(username_msg);
        if username_tmp == "" {
            username = None;
        }
        else {
            username = Some(username_tmp);
        }

        let password: Option<String>;
        let password_msg = "Password? [default: None]";
        let password_tmp = utils::get_input_with_message(password_msg);
        if password_tmp == "" {
            password = None;
        }
        else {
            password = Some(password_tmp);
        }

        let compiled_config = GenericWarehouse {
            name: name,
            metadata_source: metadata_source,
            uri: uri,
            port: port,
            username: username,
            password: password
        };

        compiled_config.register_config();

        println!("{} {:?} {}",
                 "Added warehouse:",
                 compiled_config.name,
                 "to ~/.whale/config/connections.yaml.",
                 );
    }

}


#[derive(Serialize, Deserialize)]
pub struct Bigquery {
    name: String,
    metadata_source: MetadataSource,
    key_path: Option<String>,
    project_credentials: Option<String>,
    project_id: Option<String>,
}

impl Bigquery {
    fn check_for_env_var() -> Option<String> {
        match env::var(GOOGLE_ENV_VAR) {
            Ok(env_var) => Some(env_var),
            Err(_) => None
        }
    }

    pub fn prompt_add_details() {
        println!("Starting warehouse detail onboarding sequence for {}.", "Google BigQuery".yellow());

        let mut key_path: Option<String>;
        let mut project_credentials: Option<String>;
        let mut project_id: Option<String>;


        // name
        let name: String = get_name();

        // Credentials

        let mut can_use_bigquery_env_var = false;
        let env_var: Option<String> = Bigquery::check_for_env_var();
        if let Some(_) = env_var {
            println!("Environment variable {} found.",
                     "GOOGLE_APPLICATION_CREDENTIALS".yellow());

            println!("\n{} [{}/{}]",
                     "Use GOOGLE_APPLICATION_CREDENTIALS for authentication?".purple(),
                     "Y".green(),
                     "n".red());
            can_use_bigquery_env_var = utils::get_input_as_bool();
        }

        if can_use_bigquery_env_var {
            let unwrapped_env_var = env_var.unwrap();
            println!("Using credentials file located at:\n{}", unwrapped_env_var.yellow());
            key_path = Some(unwrapped_env_var);
            project_credentials = None;
        }
        else {
            let method = Bigquery::prompt_choose_credential_method();
            let credentials = Bigquery::get_credentials(method);
            if method == 1 {
                key_path = Some(credentials);
                project_credentials = None;
            }
            else if method == 2 {
                key_path = None;
                project_credentials = Some(credentials);
            }
            else {  // shouldn't happen, but covering all cases.
                key_path = None;
                project_credentials = None;
            }
        }

        // Project_id

        project_id = Some(Bigquery::prompt_project_id());

        let compiled_config = Bigquery {
            name: name,
            metadata_source: MetadataSource::Bigquery,
            key_path: key_path,
            project_credentials: project_credentials,
            project_id: project_id
        };

        compiled_config.register_config();

        println!("{} {:?} {}",
                 "Added warehouse:",
                 &compiled_config.project_id.unwrap(),
                 "to ~/.whale/config/connections.yaml.",
                 );

    }

    fn prompt_choose_credential_method() -> i32 {
        println!("{}", "Which credential method?".purple());
        println!(" {}", "Options:".bold());
        println!(" [1] Path (specify the path where your credentials json file is located)");
        println!(" [2] Key (directly pass the credential key)");
        println!("Enter {} or {}.", "1".green(), "2".green());

        let mut input = 0;
        let acceptable_values = vec![1, 2];
        while !acceptable_values.contains(&input) {
            println!("Enter {} or {}.", "1".green(), "2".red());
            input = utils::get_integer_input();
        }
        input
    }

    fn get_credentials(method: i32) -> String {
        let mut credentials: String;
        if method == 2 {
            println!("\n{}",
                     "Enter your credentials key.".purple());
            credentials = utils::get_input();
        }
        else {
            println!("\n{}",
                     "Enter the path where your credentials json is located.".purple());
            credentials = utils::get_input();
        }
        credentials
    }

    fn prompt_project_id() -> String {
        println!("\n{}",
                 "Enter the project_id you want to pull metadata from.".purple());
        let project_id = utils::get_input();
        let mut trimmed_project_id: String;
        if project_id == "" {
            println!("You must specify a project_id.");
            return Bigquery::prompt_project_id();
        }
        else {
            trimmed_project_id = project_id
                .to_string()
                .trim()
                .to_string();
            trimmed_project_id
        }
    }

}