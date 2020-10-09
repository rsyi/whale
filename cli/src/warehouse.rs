use colored::*;
use names::{Generator, Name};
use std::{
    collections::HashMap,
    env,
    fmt};
use std::process;
use std::str::FromStr;

use crate::serialization::{
    Serialize, Deserialize, YamlWriter, self
};
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
    GitServer,
    Hive,
    HiveMetastore,
    Postgres,
    Presto,
    Redshift,
    Snowflake,
    AmundsenNeo4j,
    BuildScript
}

impl FromStr for MetadataSource {
    type Err = ParseMetadataSourceError;
    fn from_str(warehouse: &str) -> Result<Self, Self::Err> {
        match warehouse {
            "bigquery" | "bq" => Ok(MetadataSource::Bigquery),
            "hive" | "h" => Ok(MetadataSource::Hive),
            "hive_metastore" | "hive-metastore" | "hive metastore"  | "hm" => Ok(MetadataSource::HiveMetastore),
            "postgres" | "po" => Ok(MetadataSource::Postgres),
            "presto" | "pr" => Ok(MetadataSource::Presto),
            "redshift" | "r" => Ok(MetadataSource::Redshift),
            "snowflake" | "s" => Ok(MetadataSource::Snowflake),
            "amundsen-neo4j" | "a" => Ok(MetadataSource::AmundsenNeo4j),
            "git" | "g" => Ok(MetadataSource::GitServer),
            "build-script" | "b" => Ok(MetadataSource::BuildScript),
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

pub fn prompt_add_warehouse(is_first_time: bool) {

    // Text

    if !is_first_time {
        println!("{} [{}/{}]",
                 "\nAdd another warehouse/source?".purple(),
                 "Y".green(),
                 "n".red());
        let has_warehouse_to_add: bool = utils::get_input_as_bool();
        if !has_warehouse_to_add {
            return ()
        }
    }

    println!("\n{}", "What type of connection would you like to add?".purple());
    println!(" {}:", "Options".bold());

    let supported_warehouse_types = [
        "bigquery",
        "postgres",
        "presto",
        "redshift",
        "snowflake",
        "git",
        "amundsen-neo4j",
        "build-script"
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
            | Ok(MetadataSource::Postgres)
            | Ok(MetadataSource::Presto)
            | Ok(MetadataSource::Redshift)
            | Ok(MetadataSource::Snowflake)
            | Ok(MetadataSource::AmundsenNeo4j)
            => GenericWarehouse::prompt_add_details(warehouse_enum.unwrap()),
        Ok(MetadataSource::GitServer) => GitServer::prompt_add_details(),
        Ok(MetadataSource::BuildScript) => BuildScript::prompt_add_details(),
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


#[derive(Serialize, Deserialize)]
pub struct BuildScript {
    pub build_script_path: String,
    pub venv_path: String,
    pub python_binary_path: Option<String>,
}

impl BuildScript {
    pub fn prompt_add_details() {
        let header = format!("
If you have a python script that generates (a) a manifest and (b) files in the ./metadata directory, whale allows you to fully integrate this script through association with `wh pull`. You'll need to simply specify a python3 virtual environment, the path the script, and [optionally] a python3 binary (by default, whale will use whatever binary is aliased to `python3`. For a sample build script, see the documentation.");
        println!("{}", header);


        let build_script_message = "Enter the path to your build script.";
        let build_script_path = utils::get_input_with_message(build_script_message);

        let venv_message = "Enter the path to the virtual environment you want to use to run this script.";
        let venv_path = utils::get_input_with_message(venv_message);

        let python_binary_path: Option<String>;
        let binary_message = format!("Specify a python3 binary path [Default: {}]", "python3".green());
        let binary_path_tmp = utils::get_input_with_message(&binary_message);
        if binary_path_tmp == "" { python_binary_path = None; }
        else { python_binary_path = Some(binary_path_tmp); }

        let compiled_config = BuildScript {
            build_script_path: build_script_path,
            venv_path: venv_path,
            python_binary_path: python_binary_path,
        };

        compiled_config.register_connection().expect("Failed to register warehouse configuration");
    }
}


#[derive(Serialize, Deserialize)]
pub struct GitServer {
    pub metadata_source: MetadataSource,  // Unused. We reference the `.git` dir instead.
    pub uri: String,
}

impl GitServer {
    pub fn prompt_add_details() {
        let git_header = format!("
{} supports git-versioning to enable teams to collaborate of a single whale repository on a hosted git platform (e.g. github).

For more information, see https://docs.whale.cx/getting-started-for-teams.

This command will set a configuration flag in config/app.yaml that causes `wh pull` and any cron jobs scheduled through the platform to reference the git remote referenced in the `~/.whale/.git` directory instead.

{} Do not do this unless you've set up a git remote server, following the documentation above. This will halt all non-git scraping.

{}",
            "Whale".cyan(),
            "WARNING:".red(),
            "Enable git as the primary metadata source?".purple()
            );
        println!("{}", git_header);
        utils::pause();

        let mut config_kv_to_update = HashMap::new();
        config_kv_to_update.insert("is_git_etl_enabled", "true");
        serialization::update_config(config_kv_to_update).expect("Failed to update config file.");
    }
}

impl Default for GitServer {
    fn default() -> Self {
        GitServer {
            metadata_source: MetadataSource::GitServer,
            uri: String::from("None")
        }
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

        compiled_config.register_connection().expect("Failed to register warehouse configuration");

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

        let key_path: Option<String>;
        let project_credentials: Option<String>;
        let project_id: Option<String>;


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

        compiled_config.register_connection().expect("Failed to register warehouse configuration");

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

        let mut input = 0;
        let acceptable_values = vec![1, 2];
        while !acceptable_values.contains(&input) {
            println!("Enter 1 or 2");
            input = utils::get_integer_input();
        }
        input
    }

    fn get_credentials(method: i32) -> String {
        let credentials: String;
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
        let trimmed_project_id: String;
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
