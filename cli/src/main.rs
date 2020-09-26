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

    match matches.subcommand_name() {
        Some("init") => whale::Whale::init(),
        Some("etl") => whale::Whale::etl(),
        Some("schedule") => whale::Whale::schedule(true),
        _ => whale::Whale::run_with(matches)
    }

}
