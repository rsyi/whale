use crate::filesystem;
use crate::serialization;
use crate::utils;
use skim::prelude::{Skim, SkimItemReader, SkimOptionsBuilder};
use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::process::Command;

pub fn table_skim() {
    let manifest_file_path = filesystem::get_manifest_filename();
    let tmp_manifest_file_path = filesystem::get_tmp_manifest_filename();

    // If manifest does not exist, use tmp manifest
    // If tmp manifest doesn't exist, use an empty string
    let table_manifest;
    let is_manifest_found: bool;
    if Path::new(&manifest_file_path).exists() {
        table_manifest = fs::read_to_string(manifest_file_path).expect("Failed to read manifest.");
        is_manifest_found = true;
    } else if Path::new(&tmp_manifest_file_path).exists() {
        table_manifest = fs::read_to_string(tmp_manifest_file_path)
            .expect("Failed to read manifest or tmp manifest.");
        is_manifest_found = true;
    } else {
        table_manifest = "No tables found.".to_string();
        is_manifest_found = false;
    }

    let preview_arg: String;
    let metadata_path = filesystem::get_metadata_dirname();
    if is_manifest_found {
        preview_arg = format!("{}/{}.md", metadata_path, "{}");
    } else {
        preview_arg = "<< EOF
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

 Good luck, and happy whaling!

        "
        .to_string();
    }

    let reader;

    let custom_preview_command = serialization::read_config("preview_command", "");

    if custom_preview_command.is_empty() {
        let bat_testing_command =
            format!("command -v bat > /dev/null 2>&1 && echo true || echo false");
        let output = Command::new("sh")
            .args(&["-c", &bat_testing_command])
            .output()
            .unwrap();
        let is_bat_installed = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse()
            .unwrap();

        if is_bat_installed {
            reader = "bat --color=always --style='changes'";
        } else {
            reader = "cat";
        }
    } else {
        reader = &custom_preview_command;
    }

    let preview_command = format!("{} {}", reader, preview_arg);

    let options = SkimOptionsBuilder::default()
        .preview(Some(&preview_command))
        .color(Some("fg:241,prompt:5,border:5"))
        .build()
        .unwrap();

    let item_reader = SkimItemReader::default();
    let items = item_reader.of_bufread(Cursor::new(table_manifest));

    // `run_with` would read and show items from the stream
    let selected_items = Skim::run_with(&options, Some(items))
        .map(|out| out.selected_items)
        .unwrap_or_else(|| Vec::new());

    // let recently_used_filename = filesystem::get_recently_used_filename();
    for item in selected_items.iter() {
        let table_name = item.text().into_owned();
        let filename = utils::convert_table_name_to_file_name(&table_name);

        let editor = filesystem::get_open_command();
        Command::new(editor)
            .arg(filename)
            .status()
            .expect("Failed to open file.");

        // let contents = format!("{}", table_name);
        // filesystem::append_to_file(contents, &recently_used_filename);
    }

    // filesystem::deduplicate_file(&recently_used_filename);
}
