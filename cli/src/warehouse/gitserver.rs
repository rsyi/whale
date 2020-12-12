use super::metadatasource::MetadataSource;
use crate::serialization::{self, Deserialize, Serialize};
use crate::utils::get_input;
use colored::*;
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
pub struct GitServer {
    pub metadata_source: MetadataSource, // Unused. We reference the `.git` dir instead.
    pub uri: String,
}

pub fn pause() {
    println!(
        "[Press {} to continue, {} to exit]",
        "enter".green(),
        "CTRL+C".red()
    );
    let _ = get_input();
}

impl GitServer {
    pub fn prompt_add_details() {
        let git_header = format!("
{} supports git-versioning to enable teams to collaborate of a single whale repository on a hosted git platform (e.g. github).

For more information, see https://docs.whale.cx/getting-started-for-teams.

This command will set a configuration flag in config/app.yaml that causes `wh pull` and any cron jobs scheduled through the platform to reference the git remote referenced in the `~/.whale/.git` directory instead.

{} Do not do this unless you've set up a git remote server, following the documentation above. This will halt all non-git scraping.

{}",
            "Whale".cyan(),
            "WARNING:".red(),
            "Enable git as the primary metadata source?".purple()
            );
        println!("{}", git_header);
        pause();

        let mut config_kv_to_update = HashMap::new();
        config_kv_to_update.insert("is_git_etl_enabled", "true");
        serialization::update_config(config_kv_to_update).expect("Failed to update config file.");
    }
}

impl Default for GitServer {
    fn default() -> Self {
        GitServer {
            metadata_source: MetadataSource::GitServer,
            uri: String::from("None"),
        }
    }
}
