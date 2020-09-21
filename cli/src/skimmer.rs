extern crate skim;

use skim::prelude::{Skim, SkimItemReader, SkimOptionsBuilder};
use std::fs;
use std::path::Path;
use std::io::Cursor;

pub fn table_skim() {

    let base_path = shellexpand::tilde("~");
    let base_path = Path::new(&*base_path);
    let whale_base_path = base_path.join(".whale");
    let manifest_file_path = whale_base_path.join("short_table_manifest.txt");

    let table_manifest = fs::read_to_string(manifest_file_path)
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
