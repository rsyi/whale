extern crate colored;
use colored::*;
use std::io;


pub fn get_input() -> String {
    let mut buffer: String = String::new();
    io::stdin().read_line(&mut buffer).unwrap();
    buffer
}


pub fn get_input_as_bool() -> bool {
    let raw_input = get_input();
    let buffer: &str = raw_input.as_str();
    let negative_inputs = ["n", "N"];

    if negative_inputs.contains(&buffer) {
        false
    }
    else {
        println!("You entered: {}", buffer);
        true
    }
}


pub fn pause() {
    println!("{} [Press {} to continue, {} to exit]", "Continue?".blue().bold(), "enter".green(), "CTRL+C".red());
    let _ = get_input();
}

