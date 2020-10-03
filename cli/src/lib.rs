mod tuievent;

#[macro_use] extern crate lazy_static;
use clap::{ArgMatches};
use colored::*;
use std::{
    io::{self, Write},
    path::Path,
    process::Command,
};
use tui::{
    backend::TermionBackend,
    layout::{Layout, Constraint, Direction},
    style::{Color, Modifier, Style},
    symbols,
    text::Span,
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType},
    Terminal,
};
use termion::{
    clear,
    event::Key,
    raw::IntoRawMode,
};
use crate::tuievent::{
    {Event, Events},
};

pub mod warehouse;
pub mod skimmer;
pub mod utils;
pub mod filesystem;
pub mod serialization;


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


fn print_scheduler_header() {
    println!("\n{} [Default: {} (on the hour, every 6 hours)]", "Enter cron string.".purple(), DEFAULT_CRON_STRING.green());
}


/// Holds all command-line tasks.
///
/// The Whale struct acts as an interface containing all methods referenced and executed by the
/// CLI. These are referenced in main.rs with a match statement on clap matches.
pub struct Whale {}

impl Whale {
    pub fn run_with(_matches: ArgMatches) -> Result<(), io::Error> {
        skimmer::table_skim();
        Ok(())
    }

    pub fn init() -> Result<(), io::Error> {
        print_initialization_header();
        filesystem::create_file_structure();

        if !utils::is_cron_expression_registered() {
            println!("\n{} [{}/{}]",
                     "Would you like to register a cron job to periodically scrape metadata?".purple(),
                     "Y".green(),
                     "n".red()
                     );
            println!("This is resource-light and can be manually deleted by editing the file at {}.", "crontab -e".magenta());
            let is_scheduling_requested: bool = utils::get_input_as_bool();
            if is_scheduling_requested {
                Whale::schedule(false)?;
            }
        }

        let is_first_warehouse = true;
        warehouse::prompt_add_warehouse(is_first_warehouse);

        Ok(())
    }

    pub fn git_enable() -> Result<(), io::Error> {
        warehouse::GitServer::prompt_add_details();

        Ok(())
    }

    pub fn config() -> Result<(), io::Error>{
        let config_file = filesystem::get_config_filename();
        let editor = filesystem::get_open_command();

        Command::new(editor)
            .arg(config_file)
            .status()?;

        Ok(())
    }

    pub fn connections() -> Result<(), io::Error> {
        let connections_config_file = filesystem::get_connections_filename();
        let editor = filesystem::get_open_command();

        Command::new(editor)
            .arg(connections_config_file)
            .status()?;

        Ok(())
    }

    pub fn etl() -> Result<(), io::Error> {
        println!("Running ETL job manually.");

        if serialization::read_config("is_git_etl_enabled").unwrap() == "true".to_string() {
            println!("TRUE!");
        }

        let build_script_path = filesystem::get_build_script_filename();
        let build_script_path = Path::new(&*build_script_path);
        let mut child = Command::new(build_script_path)
            .spawn()?;
        child.wait()?;

        let manifest_path = filesystem::get_manifest_filename();
        filesystem::deduplicate_file(&manifest_path);

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

        let mut can_add_crontab = true;
        if ask_for_permission {
            println!("{} [{}, {}]", "Register metadata scraping job at this schedule in crontab?".purple(), "Y".green(), "n".red());
            println!("This will excise and replace any whale-based cron job you've already registered through this interface.");
            println!("You can manually delete this later by editing the file accessed by `crontab -e`.");

            can_add_crontab = utils::get_input_as_bool();
        }

        if can_add_crontab {
            let whale_etl_command = filesystem::get_etl_command();

            let whale_cron_expression = format!(
                "{} {}",
                cron_string,
                whale_etl_command);
            let scheduler_command = format!("(crontab -l | fgrep -v \"{}\"; echo \"{}\") | crontab -", whale_etl_command, whale_cron_expression);

            Command::new("sh")
                .args(&["-c", &scheduler_command])
                .spawn()?;

            println!("If you are prompted for permissions, allow and continue. [Press {} to continue]", "enter".green());
            utils::get_input();

            if utils::is_cron_expression_registered() {
                println!("{}", "Cron expression successfully registered.".cyan());
            }
            else {
                println!("{} {}", "WARNING:".red(), "Cron tab failed to register.");
                println!("If failures persist, manually add the following line to your crontab (accessed through {}).", "crontab -e".magenta());
                println!("{}", whale_cron_expression.magenta());
            }

        }

        Ok(())
    }


    pub fn dash() -> Result<(), io::Error> {

        const DATA2: [(f64, f64); 7] = [
            (0.0, 0.0),
            (10.0, 1.0),
            (20.0, 0.5),
            (30.0, 1.5),
            (40.0, 1.0),
            (50.0, 2.5),
            (60.0, 3.0),
        ];

        let events = Events::new();
        let mut stdout = io::stdout().into_raw_mode()?;
        write!(stdout, "{}", clear::All)?;
        let backend = TermionBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        loop {
            terminal.draw(|f| {
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .margin(1)
                    .constraints(
                        [
                            Constraint::Percentage(50),
                            Constraint::Percentage(50),
                        ].as_ref()
                    )
                    .split(f.size());

                let block = Block::default()
                     .title("Block")
                     .borders(Borders::ALL);

                let datasets = vec![Dataset::default()
                    .name("count_distinct(nulls)")
                    .marker(symbols::Marker::Braille)
                    .style(Style::default().fg(Color::Yellow))
                    .graph_type(GraphType::Line)
                    .data(&DATA2)];
                let chart = Chart::new(datasets)
                    .block(block)
                    .x_axis(
                        Axis::default()
                            .title("X Axis")
                            .style(Style::default().fg(Color::Gray))
                            .bounds([0.0, 60.0])
                    )
                    .y_axis(
                        Axis::default()
                            .title("Y Axis")
                            .style(Style::default().fg(Color::Gray))
                            .labels(vec![
                                Span::styled("3", Style::default().add_modifier(Modifier::BOLD)),
                                Span::raw("0"),
                            ])
                            .bounds([0.0, 3.0])
                    );

                f.render_widget(chart, chunks[0]);

                let block = Block::default()
                     .title("Block 2")
                     .borders(Borders::ALL);
                f.render_widget(block, chunks[1]);
            })?;

            let input = events.next().unwrap();
            match input {
                Event::Input(input) => {
                    if input == Key::Char('q') {
                        break;
                    }
                },
                Event::Tick => ()
            };
        };

        Ok(())
    }
}
