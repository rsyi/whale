#[macro_use] extern crate lazy_static;
extern crate clap;
extern crate colored;
extern crate shellexpand;
use clap::{ArgMatches};
use colored::*;
use std::fmt;
use std::path::Path;
use std::process::Command;

pub mod warehouse;
pub mod skimmer;
pub mod utils;
pub mod filesystem;

const DEFAULT_CRON_STRING: &str = "0 */6 * * *";


fn print_initialization_header() {
    let whale_header = "
                 oooo
                 `888              ~:~
oooo oooo    ooo  888 .oo.          :
 `88. `88.  .8'   888P\"Y88b      ___:____     |\"\\/\"|
  `88..]88..8'    888   888    ,'        `.    \\  /
   `888'`888'     888   888    |  o        \\___/  |
    `8'  `8'     o888o o888o  ~^~^~^~^~^~^~^~^~^~^~^~

    Data Warehouse Command-Line Explorer".cyan().bold();

    println!("{}", whale_header);
    // println!("
// Whale needs to initialize the following file structure:
//
//  ~/.whale
//  ├── config
//  │   └── connections.yaml
//  ├── logs
//  └── metadata
//
// We'll check if this exists and make it if it doesn't.")
}


fn print_etl_header() {
    println!("Running ETL job manually.");
}


fn print_scheduler_header() {
    println!("\n{} [Default: {} (on the hour, every 6 hours)]", "Enter cron string.".purple(), DEFAULT_CRON_STRING.green());
}


pub struct Whale {}

impl Whale {
    pub fn run_with(matches: ArgMatches) {
        skimmer::table_skim();
    }

    pub fn init() {
        print_initialization_header();
        // utils::pause();
        // filesystem::create_file_structure();

        if !utils::is_cron_expression_registered() {
            println!("\n{} [{}/{}]",
                     "Would you like to register a cron job to periodically scrape metadata?".purple(),
                     "Y".green(),
                     "n".red()
                     );
            println!("This is very, very resource light, and can always be removed by editing the file at `crontab -e`.");
            let is_scheduling_requested: bool = utils::get_input_as_bool();
            if is_scheduling_requested {
                Whale::schedule();
            }
        }

        let is_first_warehouse = true;
        warehouse::prompt_add_warehouse(is_first_warehouse);
    }

    pub fn etl() {
        print_etl_header();
        let base_path = shellexpand::tilde("~");
        let build_script_path = Path::new(&*base_path)
            .join(".whale")
            .join("libexec")
            .join("dist")
            .join("build_script")
            .join("build_script");
        Command::new(build_script_path)
            .output()
            .expect("ETL failed.");
    }

    pub fn schedule() {
        print_scheduler_header();
        let mut cron_string = utils::get_input();
        if cron_string.is_empty() {
            cron_string = DEFAULT_CRON_STRING.to_string();
        }
        let is_valid_cron: bool = utils::is_valid_cron_expression(&cron_string);
        if is_valid_cron {
            println!(
                "\n{} {}.\n",
                "Valid cron string detected:",
                cron_string.yellow());
        }
        else {
            println!(
                "\n{} {} {} {}\n",
                "WARNING:".bold().red(),
                "Cron expression",
                cron_string.yellow(),
                "appears invalid. Proceed with caution.",
                );
        }

        println!("{}", "Register metadata scraping job at this schedule in crontab?".purple());
        println!("This will excise and replace any whale-based cron job you've already registered through this interface.");
        println!("You can manually delete this later by editing the file accessed by `crontab -e`.");

        utils::pause();

        let whale_etl_command = format!("{} {}", shellexpand::tilde("~/.whale/libexec/whale"), "etl");
        let whale_logs_path = shellexpand::tilde("~/.whale/logs/cron.log");

        let whale_cron_expression = format!(
            "{} {} >> {}",
            cron_string,
            whale_etl_command,
            whale_logs_path);
        let scheduler_command = format!("(crontab -l | fgrep -v \"{}\"; echo \"{}\") | crontab -", whale_etl_command, whale_cron_expression);

        Command::new("sh")
            .args(&["-c", &scheduler_command])
            .spawn()
            .expect("Crontab registration failed.");

        if utils::is_cron_expression_registered() {
            println!("If you are prompted for permissions, allow and continue. [Press {} to continue]", "enter".green());
            utils::get_input();
            println!("Cron expression successfully registered.");
        }
    }
}
