use std::fs;
use std::path::{Path};

pub fn create_file_structure() {
    let base_path = shellexpand::tilde("~");
    let base_path = Path::new(&*base_path);
    let whale_base_path = base_path.join(".whale");
    let config_path = base_path.join("config");
    let metadata_path = base_path.join("metadata");
    let logs_path = base_path.join("logs");

    let subpaths = [
        config_path,
        metadata_path,
        logs_path,
    ];

    for subpath in subpaths.iter() {
        fs::create_dir_all(subpath)
            .expect("Directory was not created succesfully.");
    }
}
