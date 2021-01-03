use colored::*;
use names::{Generator, Name};
use std::process;
use std::str::FromStr;

use super::{
    bigquery::Bigquery, buildscript::BuildScript, errors::ParseMetadataSourceError,
    generic::GenericWarehouse, gitserver::GitServer, glue::Glue, hive::HiveMetastore,
    metadatasource::MetadataSource, snowflake::Snowflake,
};
use crate::utils;

pub fn prompt_add_warehouse(is_first_time: bool) {
    if !is_first_time {
        println!(
            "{} [{}/{}]",
            "\nAdd another warehouse/source?".purple(),
            "Y".green(),
            "n".red()
        );
        let has_warehouse_to_add: bool = utils::get_input_as_bool();
        if !has_warehouse_to_add {
            return;
        }
    }

    println!(
        "\n{}",
        "What type of connection would you like to add?".purple()
    );
    println!(" {}:", "Options".bold());

    let supported_warehouse_types = [
        "bigquery",
        "glue",
        "hive-metastore",
        "postgres",
        "presto",
        "redshift",
        "snowflake",
        "git",
        "amundsen-neo4j",
        "build-script",
    ];
    for supported_warehouse_type in supported_warehouse_types.iter() {
        println!(" * {}", supported_warehouse_type)
    }

    let raw_warehouse_type_input: String = utils::get_input();
    let warehouse_type = raw_warehouse_type_input.trim();
    let warehouse_enum = MetadataSource::from_str(warehouse_type);

    println!("You entered: {}", warehouse_type.yellow());

    match warehouse_enum {
        Ok(MetadataSource::Bigquery) => Bigquery::prompt_add_details(),
        Ok(MetadataSource::Hive)
        | Ok(MetadataSource::Postgres)
        | Ok(MetadataSource::Presto)
        | Ok(MetadataSource::Redshift)
        | Ok(MetadataSource::AmundsenNeo4j) => {
            GenericWarehouse::prompt_add_details(warehouse_enum.unwrap())
        }
        Ok(MetadataSource::Glue) => Glue::prompt_add_details(),
        Ok(MetadataSource::HiveMetastore) => HiveMetastore::prompt_add_details(),
        Ok(MetadataSource::Snowflake) => Snowflake::prompt_add_details(),
        Ok(MetadataSource::GitServer) => GitServer::prompt_add_details(),
        Ok(MetadataSource::BuildScript) => BuildScript::prompt_add_details(),
        Err(e) => handle_error(e),
    };

    prompt_add_warehouse(false);
}

fn handle_error(e: ParseMetadataSourceError) {
    eprintln!("{} {}", "WARNING:".red(), e);
    println!("Try again.");
    prompt_add_warehouse(true);
    process::exit(1)
}

pub fn get_name() -> String {
    println!(
        "\n{}",
        "Input a name for your warehouse (e.g. bq-1)".purple()
    );
    let mut name = utils::get_input();
    if name.is_empty() {
        let mut generator = Generator::with_naming(Name::Plain);
        name = generator.next().unwrap();
        println!("Using randomly generated name: {}", name.green());
    }
    name.trim().to_string()
}
