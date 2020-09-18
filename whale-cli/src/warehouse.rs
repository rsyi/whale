extern crate colored;
use colored::*;
use std::env;
use std::str::FromStr;
use std::string::ParseError;

use crate::utils;

const GOOGLE_ENV_VAR: &str = "GOOGLE_APPLICATION_CREDENTIALS";


pub enum Warehouse {
    Bigquery,
    Hive,
    HiveMetastore,
    Presto,
    Snowflake
}

// impl FromStr for Warehouse {
//     type Err = ParseError;
//     fn from_str(warehouse: &str) -> Result<Self, ParseError> {
//         match warehouse {
//             "bigquery" => Ok(Warehouse::Bigquery),
//             "hive" => Ok(Warehouse::Hive),
//             "hive_metastore" => Ok(Warehouse::HiveMetastore),
//             "presto" => Ok(Warehouse::Presto),
//             "snowflake" => Ok(Warehouse::Snowflake),
//             _ => Err(()),
//         }
//     }
// }


pub struct Bigquery {}

impl Bigquery {
    fn check_for_env_var() -> bool {
        match env::var(GOOGLE_ENV_VAR) {
            Ok(_) => true,
            Err(_) => false
        }
    }

    pub fn prompt() -> bool {
        if Bigquery::check_for_env_var() {
            println!("GOOGLE_APPLICATION_CREDENTIALS found. Use this? [{}/{}]",
                     "Y".green(),
                     "n".red());
            let can_use_bigquery_env_var = utils::get_input_as_bool();
            if !can_use_bigquery_env_var {
                Bigquery::prompt_choose_credential_method();
            }
        }
        true
    }

    fn prompt_choose_credential_method() -> &'static str {
        println!("Choose credential method.");
        "string"
    }

}

