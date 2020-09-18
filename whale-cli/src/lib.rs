extern crate clap;
extern crate colored;
extern crate shellexpand;
use clap::{ArgMatches};
use colored::*;
use std::fs;
use std::path::Path;

pub mod warehouse;
pub mod skimmer;
pub mod utils;


fn print_initialization_header() {
    let whale_header = "
                 oooo
                 `888              ~:~
oooo oooo    ooo  888 .oo.          :
 `88. `88.  .8'   888P\"Y88b      ___:____     |\"\\/\"|
  `88..]88..8'    888   888    ,'        `.    \\  /
   `888'`888'     888   888    |  o        \\___/  |
    `8'  `8'     o888o o888o  ~^~^~^~^~^~^~^~^~^~^~^~

    Data Warehouse Command-Line Explorer".blue().bold();

    println!("{}", whale_header);
    println!("
Whale needs to initialize the following file structure:

 ~/.whale
 ├── config
 │   └── connections.yaml
 ├── logs
 └── metadata

We'll check if this exists and make it if it doesn't.")
}

fn create_file_structure() {
    let base_path = shellexpand::tilde("~");
    let base_path = Path::new(&*base_path);
    let whale_base_path = base_path
        .join(".whale");
    let subpaths = ["config", "metadata", "logs"];

    for subpath in subpaths.iter() {
        let path_to_create = whale_base_path.join(subpath);
        fs::create_dir_all(path_to_create)
            .expect("Directory was not created succesfully.");
    }
}

fn prompt_add_warehouse(is_first_time: bool) -> bool {
    let header_statement = match is_first_time {
        true => "Add new warehouse?",
        false => "Add another warehouse?"
    };

    println!("{} [{}/{}]",
             header_statement.blue(),
             "Y".green(),
             "n".red());
    let input: String = utils::get_input();
    if input == "n\n" {
        return false
    }

    let mut has_valid_warehouse_type = false;

    while !has_valid_warehouse_type {
        has_valid_warehouse_type = prompt_warehouse_details();
    }

    true

}



fn prompt_warehouse_details() -> bool {
    println!("{}", "What type of warehouse would you like to create?".blue());

    let supported_warehouse_types = [
        "bigquery",
        "hive",
        "hive_metastore",
        "presto",
        "snowflake"
    ];

    println!("{}:", " Options".bold());

    for supported_warehouse_type in supported_warehouse_types.iter() {
        println!(" * {}", supported_warehouse_type)
    }

    let raw_warehouse_type_input: String = utils::get_input();
    let warehouse_type = raw_warehouse_type_input.trim();
    println!("You entered: {}", warehouse_type);

    let has_valid_warehouse_type = match warehouse_type {
        "bigquery" => warehouse::Bigquery::prompt(),
        "hive" => prompt_hive(),
        "hive_metastore" => prompt_hive_metastore(),
        "presto" => prompt_presto(),
        "snowflake" => prompt_snowflake(),
        _ => prompt_invalid_warehouse()};

    has_valid_warehouse_type
}


fn prompt_invalid_warehouse() -> bool {
    println!("Invalid entry. Try again.");
    false
}


fn prompt_hive() -> bool {
    true
}


fn prompt_hive_metastore() -> bool {
    true
}


fn prompt_presto() -> bool {
    true
}


fn prompt_snowflake() -> bool{
    true
}





pub struct Whale {}

impl Whale {
    pub fn run_with(matches: ArgMatches) {
        skimmer::table_skim();
    }

    pub fn init() {
        print_initialization_header();
        utils::pause();
        create_file_structure();

        let mut is_in_warehouse_loop = true;
        let mut is_first_time = true;

        while is_in_warehouse_loop {
            is_in_warehouse_loop = prompt_add_warehouse(is_first_time);
            is_first_time = false;
        }
    }

    pub fn etl() {
        println!("Running ETL job manually...")
    }
}
