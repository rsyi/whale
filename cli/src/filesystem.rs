use std::path::{Path};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::Write,
};


pub fn create_file_structure() {
    let base_path = shellexpand::tilde("~");
    let base_path = Path::new(&*base_path);
    let whale_base_path = base_path.join(".whale");
    let bin_path = whale_base_path.join("bin");
    let config_path = whale_base_path.join("config");
    let libexec_path = whale_base_path.join("libexec");
    let logs_path = whale_base_path.join("logs");
    let metadata_path = whale_base_path.join("metadata");

    let subpaths = [
        bin_path,
        config_path,
        metadata_path,
        libexec_path,
        logs_path,
    ];

    for subpath in subpaths.iter() {
        fs::create_dir_all(subpath)
            .expect("Directory was not created succesfully.");
    }
}


pub fn deduplicate_file(file_path_string: &str) {
    let file_path = Path::new(file_path_string);
    let contents = fs::read_to_string(file_path).expect("Can't read file.");
    let lines: BTreeSet<_> = contents.lines().collect();
    let mut file = File::open(file_path).expect("Can't open file.");
    for line in lines {
        writeln!(file, "{}", line);
    }
}
