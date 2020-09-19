extern crate clap;
extern crate colored;
extern crate shellexpand;
use clap::{ArgMatches};
use colored::*;

pub mod warehouse;
pub mod skimmer;
pub mod utils;
pub mod filesystem;


fn print_initialization_header() {
    let whale_header = "
                 oooo
                 `888              ~:~
oooo oooo    ooo  888 .oo.          :
 `88. `88.  .8'   888P\"Y88b      ___:____     |\"\\/\"|
  `88..]88..8'    888   888    ,'        `.    \\  /
   `888'`888'     888   888    |  o        \\___/  |
    `8'  `8'     o888o o888o  ~^~^~^~^~^~^~^~^~^~^~^~

    Data Warehouse Command-Line Explorer".purple().bold();

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



pub struct Whale {}

impl Whale {
    pub fn run_with(matches: ArgMatches) {
        skimmer::table_skim();
    }

    pub fn init() {
        print_initialization_header();
        utils::pause();
        filesystem::create_file_structure();

        let mut has_more_warehouses = true;
        let mut is_first_warehouse = true;

        warehouse::prompt_add_warehouse(is_first_warehouse);
        is_first_warehouse = false;
    }

    pub fn etl() {
        println!("Running ETL job manually...")
    }
}
