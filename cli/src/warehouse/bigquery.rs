use super::{constants::GOOGLE_ENV_VAR, metadatasource::MetadataSource, whutils::get_name};

use crate::serialization::{Deserialize, Serialize, YamlWriter};
use crate::utils;
use colored::*;
use std::env;

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
            Err(_) => None,
        }
    }

    pub fn prompt_add_details() {
        println!(
            "Starting warehouse detail onboarding sequence for {}.",
            "Google BigQuery".yellow()
        );

        let key_path: Option<String>;
        let project_credentials: Option<String>;
        let project_id: Option<String>;

        // name
        let name: String = get_name();

        // Credentials

        let mut can_use_bigquery_env_var = false;
        let env_var: Option<String> = Bigquery::check_for_env_var();

        if env_var.is_some() {
            println!(
                "Environment variable {} found.",
                "GOOGLE_APPLICATION_CREDENTIALS".yellow()
            );

            println!(
                "\n{} [{}/{}]",
                "Use GOOGLE_APPLICATION_CREDENTIALS for authentication?".purple(),
                "Y".green(),
                "n".red()
            );
            can_use_bigquery_env_var = utils::get_input_as_bool();
        }

        if can_use_bigquery_env_var {
            let unwrapped_env_var = env_var.unwrap();
            println!(
                "Using credentials file located at:\n{}",
                unwrapped_env_var.yellow()
            );
            key_path = Some(unwrapped_env_var);
            project_credentials = None;
        } else {
            let method = Bigquery::prompt_choose_credential_method();
            let credentials = Bigquery::get_credentials(method);
            if method == 1 {
                key_path = Some(credentials);
                project_credentials = None;
            } else if method == 2 {
                key_path = None;
                project_credentials = Some(credentials);
            } else {
                // shouldn't happen, but covering all cases.
                key_path = None;
                project_credentials = None;
            }
        }

        // Project_id

        project_id = Some(Bigquery::prompt_project_id());

        let compiled_config = Bigquery {
            name,
            metadata_source: MetadataSource::Bigquery,
            key_path,
            project_credentials,
            project_id,
        };

        compiled_config
            .register_connection()
            .expect("Failed to register warehouse configuration");

        println!(
            "Added warehouse: {:?} to ~/.whale/config/connections.yaml.",
            &compiled_config.project_id.unwrap(),
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
            println!("\n{}", "Enter your credentials key.".purple());
            credentials = utils::get_input();
        } else {
            println!(
                "\n{}",
                "Enter the path where your credentials json is located.".purple()
            );
            credentials = utils::get_input();
        }
        credentials
    }

    fn prompt_project_id() -> String {
        println!(
            "\n{}",
            "Enter the project_id you want to pull metadata from.".purple()
        );
        let project_id = utils::get_input();

        if project_id == "" {
            println!("You must specify a project_id.");
            Bigquery::prompt_project_id()
        } else {
            project_id.trim().to_string()
        }
    }
}
