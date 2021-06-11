#[macro_use]
extern crate lazy_static;
use clap::ArgMatches;
use colored::*;
use std::io;
use std::process::Command;

pub mod filesystem;
pub mod serialization;
pub mod skimmer;
pub mod utils;
pub mod warehouse;

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

    Data Warehouse Command-Line Explorer"
        .cyan()
        .bold();

    println!("{}", whale_header);
}

fn print_scheduler_header() {
    println!(
        "\n{} [Default: {} (on the hour, every 6 hours)]",
        "Enter cron string.".purple(),
        DEFAULT_CRON_STRING.green()
    );
}

/// Holds all command-line tasks.
///
/// The Whale struct acts as an interface containing all methods referenced and executed by the
/// CLI. These are referenced in main.rs with a match statement on clap matches.
pub struct Whale {}

impl Whale {
    pub fn run_with(_matches: ArgMatches) -> Result<(), io::Error> {
        skimmer::table_skim();
        let is_git_etl_enabled = serialization::read_config("is_git_etl_enabled", "false")
            .parse::<bool>()
            .unwrap();
        if is_git_etl_enabled {
            let whale_dirname = filesystem::get_base_dirname();
            let command = format!(
                "cd {} && (git status | grep Unmerged > /dev/null 2>&1 && echo true || echo false)",
                whale_dirname
            );

            let output = Command::new("sh").args(&["-c", &command]).output()?;
            let is_unmerged_files_found = String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse()
                .unwrap();
            if is_unmerged_files_found {
                println!("{} You have unmerged files that conflict with your upstream remote. Navigate to ~/.whale to fix this. Your metadata will be out of date until you do. Run {} after fixing this to pull the freshest metadata immediately.", "WARNING:".red(), "wh pull".cyan())
            }
        }
        Ok(())
    }

    pub fn init() -> Result<(), io::Error> {
        print_initialization_header();
        filesystem::create_file_structure();

        if !utils::is_cron_expression_registered() {
            println!(
                "\n{} [{}/{}]",
                "Would you like to register a cron job to periodically scrape metadata?".purple(),
                "Y".green(),
                "n".red()
            );
            println!(
                "This is resource-light and can be manually deleted by editing the file at {}.",
                "crontab -e".magenta()
            );
            let is_scheduling_requested: bool = utils::get_input_as_bool();
            if is_scheduling_requested {
                Whale::schedule(false)?;
            }
        }

        let is_first_warehouse = true;
        warehouse::whutils::prompt_add_warehouse(is_first_warehouse);
        Ok(())
    }

    pub fn git_enable() -> Result<(), io::Error> {
        warehouse::gitserver::GitServer::prompt_add_details();
        Ok(())
    }

    pub fn git_setup(git_address: &str) -> Result<(), io::Error> {
        if git_address == "None" {
            println!("You must supply a git remote.");
            Ok(())
        } else {
            let init_command = format!(
                "cd ~/.whale && git init && git remote add origin {}",
                git_address
            );
            let gitignore_command = "echo 'bin/\nlogs/\nconfig/config.yaml\nlibexec/' > .gitignore";
            let git_push_command =
                "git add . && git commit -m 'Whale on our way' && git push -u origin master";
            let full_command = format!(
                "{} && {} && {}",
                init_command, gitignore_command, git_push_command
            );
            let mut child = Command::new("sh").args(&["-c", &full_command]).spawn()?;
            child.wait()?;
            Ok(())
        }
    }

    pub fn config() -> Result<(), io::Error> {
        let config_file = filesystem::get_config_filename();
        let editor = filesystem::get_open_command();
        Command::new(editor).arg(config_file).status()?;
        Ok(())
    }

    pub fn connections() -> Result<(), io::Error> {
        let connections_config_file = filesystem::get_connections_filename();
        let editor = filesystem::get_open_command();
        Command::new(editor).arg(connections_config_file).status()?;
        Ok(())
    }

    pub fn etl() -> Result<(), io::Error> {
        println!("Running ETL job manually.");
        let is_git_etl_enabled = serialization::read_config("is_git_etl_enabled", "false")
            .parse::<bool>()
            .unwrap();

        if is_git_etl_enabled {
            println!("Git-syncing is enabled (see ~/.whale/config/config.yaml or https://docs.whale.cx/getting-started-for-teams for more details).
Fetching and rebasing local changes from github.");
            let whale_dirname = filesystem::get_base_dirname();

            let command = format!("cd {} && git pull --rebase --autostash", whale_dirname);
            let mut child = Command::new("sh").args(&["-c", &command]).spawn()?;
            child.wait()?;
        } else {
            let python_alias = serialization::read_config("python3_alias", "python3");
            let activate_path = filesystem::get_activate_filename();
            let build_script_path = filesystem::get_build_script_filename();
            let full_command = format!(
                ". {} && {} {}",
                activate_path, python_alias, build_script_path
            );
            let mut child = Command::new("sh").args(&["-c", &full_command]).spawn()?;
            child.wait()?;

            let manifest_path = filesystem::get_manifest_filename();
            filesystem::deduplicate_file(&manifest_path);
        }
        Ok(())
    }

    pub fn run(sql_file: &str, warehouse_name: &str) -> Result<(), io::Error> {
        let python_alias = serialization::read_config("python3_alias", "python3");
        let activate_path = filesystem::get_activate_filename();
        let run_script_path = filesystem::get_run_script_filename();
        let arguments = format!("{} {}", sql_file, warehouse_name);
        let full_command = format!(
            ". {} && {} {} {}",
            activate_path, python_alias, run_script_path, arguments
        );
        let mut child = Command::new("sh").args(&["-c", &full_command]).spawn()?;
        child.wait()?;
        Ok(())
    }

    pub fn schedule(ask_for_permission: bool) -> Result<(), io::Error> {
        print_scheduler_header();
        let mut cron_string = utils::get_input();
        if cron_string.is_empty() {
            cron_string = DEFAULT_CRON_STRING.to_string();
        }
        let is_valid_cron: bool = utils::is_valid_cron_expression(&cron_string);
        if is_valid_cron {
            println!("\nValid cron string detected: {}.\n", cron_string.yellow());
        } else {
            println!(
                "\n{} Cron expression {} appears invalid. Proceed with caution\n",
                "WARNING:".bold().red(),
                cron_string.yellow(),
            );
        }

        let mut can_add_crontab = true;
        if ask_for_permission {
            println!(
                "{} [{}, {}]",
                "Register metadata scraping job at this schedule in crontab?".purple(),
                "Y".green(),
                "n".red()
            );
            println!("This will excise and replace any whale-based cron job you've already registered through this interface.");
            println!(
                "You can manually delete this later by editing the file accessed by `crontab -e`."
            );

            can_add_crontab = utils::get_input_as_bool();
        }

        if can_add_crontab {
            let whale_etl_command = filesystem::get_etl_command();

            let whale_cron_expression = format!("{} {}", cron_string, whale_etl_command);
            let scheduler_command = format!(
                "(crontab -l | grep -v \"{}\"; echo \"{}\") | crontab -",
                whale_etl_command, whale_cron_expression
            );

            Command::new("sh")
                .args(&["-c", &scheduler_command])
                .spawn()?;

            println!(
                "If you are prompted for permissions, allow and continue. [Press {} to continue]",
                "enter".green()
            );
            utils::get_input();

            if utils::is_cron_expression_registered() {
                println!("{}", "Cron expression successfully registered.".cyan());
            } else {
                println!("{} Cron tab failed to register.", "WARNING:".red());
                println!("If failures persist, manually add the following line to your crontab (accessed through {}).", "crontab -e".magenta());
                println!("{}", whale_cron_expression.magenta());
            }
        }
        Ok(())
    }
}
