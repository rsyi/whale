extern crate clap;
extern crate colored;
extern crate shellexpand;
use clap::{ArgMatches};
use colored::*;
use std::{fs, io};
use std::path::Path;

pub mod skimmer;

const WHALE_BASE_PATH: &str = "~/.whale";

fn pause() {
    println!("{} [Press {} to continue, {} to exit]", "Continue?".blue().bold(), "enter".green(), "CTRL+C".red());
    io::stdin().read_line(&mut String::new()).unwrap();
}

fn print_initialization_header_and_pause() {
    let whale_header = "
                 oooo
                 `888              ~:~
oooo oooo    ooo  888 .oo.          :
 `88. `88.  .8'   888P\"Y88b      ___:____     |\"\\/\"|
  `88..]88..8'    888   888    ,'        `.    \\  /
   `888'`888'     888   888    |  o        \\___/  |
    `8'  `8'     o888o o888o  ~^~^~^~^~^~^~^~^~^~^~^~

    Command-line Data Warehouse and Lake Explorer".blue().bold();

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


pub struct Whale {}

impl Whale {
    pub fn run_with(matches: ArgMatches) {
        skimmer::table_skim();
    }

    pub fn init() {
        print_initialization_header_and_pause();
        pause();
        create_file_structure();
    }

    pub fn etl() {
        println!("Running ETL job manually...")
    }
}
