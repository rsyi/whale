use whale;
use clap::{App, Arg, SubCommand};

fn main() {
    let app = App::new("Whale CLI")
        .about("Data WareHouse And Lake Explorer")
        .author("Robert Yi (@rsyi on github)")
        .subcommand(SubCommand::with_name("init")
            .about("Initializes the scheduler and extraction pipeline"))
        //.subcommand(SubCommand::with_name("dash")
        //    .about("Show macro-level dashboard of metadata metadata"))
        .subcommand(SubCommand::with_name("config")
            .about("Open file containing whale config information"))
        .subcommand(SubCommand::with_name("connections")
            .about("Open file containing warehouse connection information"))
        .subcommand(SubCommand::with_name("git-enable")
            .about("Enable git"))
        .subcommand(SubCommand::with_name("git-setup")
            .about("Set up ~/.whale directory for git and run initial push")
            .arg(Arg::with_name("remote")
                 .help("Remote git address where whale will be backed up"))
            )
        .subcommand(SubCommand::with_name("etl")
            .about("Manually runs the metadata extraction job (deprecated: use `wh pull` instead)"))
        .subcommand(SubCommand::with_name("pull")
            .about("Manually runs the metadata extraction job"))
        .subcommand(SubCommand::with_name("run")
            .about("Execute a query file")
            .arg(Arg::with_name("sql_file")
                 .help("File path that contains the sql file to run")
                 .required(true))
            .arg(Arg::with_name("warehouse_name")
                 .short("w")
                 .long("warehouse")
                 .help("Warehouse name to run the query against")
                 .required(false))
            )
        .subcommand(SubCommand::with_name("schedule")
            .about("Register metadata scraping job with crontab"));

    let matches = app.get_matches();

    let mut git_address = "None";
    if let Some(git_setup_matches) = matches.subcommand_matches("git-setup") {
        if git_setup_matches.is_present("remote") {
            git_address = git_setup_matches.value_of("remote").unwrap()
        }
    }

    let mut sql_file = "";
    let mut warehouse_name = "";
    if let Some(run_matches) = matches.subcommand_matches("run") {
        if run_matches.is_present("sql_file") {
            sql_file = run_matches.value_of("sql_file").unwrap()
        }
        if run_matches.is_present("warehouse_name") {
            warehouse_name = run_matches.value_of("warehouse_name").unwrap()
        }
    }

    match matches.subcommand_name() {
        Some("init") => whale::Whale::init(),
        // Some("dash") => whale::Whale::dash(),
        Some("etl") => whale::Whale::etl(),
        Some("pull") => whale::Whale::etl(),
        Some("schedule") => whale::Whale::schedule(true),
        Some("config") => whale::Whale::config(),
        Some("connections") => whale::Whale::connections(),
        Some("git-enable") => whale::Whale::git_enable(),
        Some("git-setup") => whale::Whale::git_setup(git_address),
        Some("run") => whale::Whale::run(sql_file, warehouse_name),
        _ => whale::Whale::run_with(matches)
    }.expect("Failed to run command.");

}
