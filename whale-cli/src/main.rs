extern crate clap;

use whale;
use clap::{App, SubCommand};

fn main() {
    let app = App::new("Whale CLI")
        .about("Data WareHouse And Lake Explorer")
        .author("Robert Yi (@rsyi on github)")
        .subcommand(SubCommand::with_name("init")
            .about("Initializes the scheduler and extraction pipeline"))
        .subcommand(SubCommand::with_name("etl")
            .about("Manually runs the metadata extraction job"));

    let matches = app.get_matches();

    if let Some(init_matches) = matches.subcommand_matches("init") {
        let output = whale::Whale::init();
    }

    else if let Some(etl_matches) = matches.subcommand_matches("etl") {
        let output = whale::Whale::etl();
    }

    else {
        let output = whale::Whale::run_with(matches);
    }
}
