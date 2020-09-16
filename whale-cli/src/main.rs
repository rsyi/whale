extern crate clap;

mod skimmer;

use clap::{App, Arg};

fn main() {
    let matches = App::new("wh")
        .arg(Arg::with_name("init"))
        .get_matches();

    skimmer::table_skim();
}
