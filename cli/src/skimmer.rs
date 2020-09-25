extern crate skim;

use skim::prelude::{Skim, SkimItemReader, SkimOptionsBuilder};
use std::fs;
use std::path::Path;
use std::process::Command;
use std::io::Cursor;
use crate::utils;

pub fn table_skim() {

    let base_path = shellexpand::tilde("~");
    let base_path = Path::new(&*base_path);
    let whale_base_path = base_path.join(".whale");
    let manifest_file_path = whale_base_path
        .join("manifests")
        .join("manifest.txt");
    let tmp_manifest_file_path = whale_base_path
        .join("manifests")
        .join("tmp_manifest.txt");

    // If manifest does not exist, use tmp manifest
    // If tmp manifest doesn't exist, use an empty string
    let table_manifest;
    if Path::new(&manifest_file_path).exists() {
        table_manifest = fs::read_to_string(manifest_file_path)
            .expect("Failed to read manifest.");
    }
    else if Path::new(&tmp_manifest_file_path).exists() {
        table_manifest = fs::read_to_string(tmp_manifest_file_path)
            .expect("Failed to read manifest or tmp manifest.");
    }
    else {
        table_manifest = "No tables yet! Run `wh etl` if you're feeling impatient.".to_string();
    }

    let metadata_path = shellexpand::tilde("~/.whale/metadata/");
    let preview_command = format!("cat {}{}.md", metadata_path, "{}");
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

    for item in selected_items.iter() {
        let table_name = item.text().into_owned();
        let filename = utils::convert_table_name_to_file_name(&table_name);

        let editor = match std::env::var("EDITOR") {
                Ok(val) => val,
                Err(_e) => "open".to_string(),
        };

        Command::new(editor)
            .arg(filename)
            .status()
            .expect("Failed to open file.");
    }
}
