extern crate skim;

use skim::prelude::{Skim, SkimItemReader, SkimOptionsBuilder};
use std::fs;
use std::io::Cursor;

const MANIFEST_FILE_PATH: &str = "./short_table_manifest.txt";

pub fn table_skim() {

    let table_manifest = fs::read_to_string(MANIFEST_FILE_PATH)
        .expect("Failed to read file.");

    println!("Manifest: {}", table_manifest);

    let options = SkimOptionsBuilder::default()
        .preview(Some("head -10 {}"))
        .build()
        .unwrap();

    let item_reader = SkimItemReader::default();
    let items = item_reader.of_bufread(Cursor::new(table_manifest));

    // `run_with` would read and show items from the stream
    let selected_items = Skim::run_with(&options, Some(items))
        .map(|out| out.selected_items)
        .unwrap_or_else(|| Vec::new());

}
