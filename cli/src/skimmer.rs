use crate::filesystem;
use crate::serialization;
use crate::utils;
use skim::prelude::{Skim, SkimItemReader, SkimOptionsBuilder};
use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::process::Command;

const ASCII_ART: &str = "<< EOF
___=____
,^        ^.      _
|  x        |____| |
~^~^~^~^~^~^~^~^~^~^~^~

“It is not down on any map; true places never are.”

No table manifest found.

This probably means either:

(a) You have not initialized whale yet.
 Run 'wh init'.

(b) You have not run your first scraping job.
 Run 'wh pull' or wait for your cron job to run.

Good luck, and happy whaling!";

fn bat_is_installed() -> bool {
    command_is_installed("bat")
}

fn command_is_installed(command: &str) -> bool {
    let bat_testing_command = format!(
        "command -v {:} > /dev/null && echo true || echo false",
        command
    );

    let output = Command::new("sh")
        .args(&["-c", &bat_testing_command])
        .output()
        .unwrap();

    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .unwrap()
}

pub fn table_skim() {
    let manifest_file_path = filesystem::get_manifest_filename();
    let tmp_manifest_file_path = filesystem::get_tmp_manifest_filename();

    // If manifest does not exist, use tmp manifest
    // If tmp manifest doesn't exist, use an empty string
    let table_manifest;

    let metadata_path = filesystem::get_metadata_dirname();

    let preview_arg = if Path::new(&manifest_file_path).exists() {
        table_manifest = fs::read_to_string(manifest_file_path).expect("Failed to read manifest.");
        format!("{}/{}.md", metadata_path, "{}")
    } else if Path::new(&tmp_manifest_file_path).exists() {
        table_manifest = fs::read_to_string(tmp_manifest_file_path)
            .expect("Failed to read manifest or tmp manifest.");
        format!("{}/{}.md", metadata_path, "{}")
    } else {
        table_manifest = "No tables found.".to_string();
        ASCII_ART.to_string()
    };

    let custom_preview_command = serialization::read_config("preview_command", "");

    let reader = if custom_preview_command.is_empty() {
        if bat_is_installed() {
            "bat --color=always --style='changes'"
        } else {
            "cat"
        }
    } else {
        &custom_preview_command
    };

    let preview_command = format!("{} {}", reader, preview_arg);

    let options = SkimOptionsBuilder::default()
        .preview(Some(&preview_command))
        .color(Some("fg:241,prompt:5,border:5"))
        .build()
        .unwrap();

    let item_reader = SkimItemReader::default();
    let items = item_reader.of_bufread(Cursor::new(table_manifest));

    let selected_items = Skim::run_with(&options, Some(items))
        .map(|out| out.selected_items)
        .unwrap_or_else(Vec::new);

    for item in selected_items.iter() {
        Command::new(filesystem::get_open_command())
            .arg(utils::convert_table_name_to_file_name(
                &item.text().into_owned(),
            ))
            .status()
            .expect("Failed to open file.");
    }
}

#[cfg(test)]
mod tests {
    use super::command_is_installed;

    #[test]
    fn test_command_installed() {
        assert_eq!(command_is_installed("cat"), true)
    }

    #[test]
    fn test_command_is_not_installed() {
        assert_eq!(command_is_installed("nosuchcommand"), false)
    }
}
