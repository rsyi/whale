extern crate clap;
extern crate colored;
extern crate shellexpand;
use clap::{ArgMatches};
use colored::*;
use std::path::Path;
use std::process::Command;

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

        let is_first_warehouse = true;
        warehouse::prompt_add_warehouse(is_first_warehouse);
    }

    pub fn etl() {
        let base_path = shellexpand::tilde("~");
        let build_script_path = Path::new(&*base_path)
            .join(".whale")
            .join("libexec")
            .join("dist")
            .join("build_script")
            .join("build_script");
        println!("Running ETL job manually.");
        Command::new(build_script_path)
            .output()
            .expect("ETL failed.");
    }
}
