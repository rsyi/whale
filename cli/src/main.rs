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
            .about("Manually runs the metadata extraction job"))
        .subcommand(SubCommand::with_name("schedule")
            .about("Register metadata scraping job with crontab"));

    let matches = app.get_matches();

    if let Some(_init_matches) = matches.subcommand_matches("init") {
        let _output = whale::Whale::init();
    }

    else if let Some(_etl_matches) = matches.subcommand_matches("etl") {
        let _output = whale::Whale::etl();
    }

    else if let Some(_schedule_matches) = matches.subcommand_matches("schedule") {
        let _output = whale::Whale::schedule(true);
    }

    else {
        let _output = whale::Whale::run_with(matches);
    }
}
