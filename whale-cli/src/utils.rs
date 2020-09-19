extern crate colored;
use colored::*;
use std::io::{self, Write};


pub fn get_input() -> String {
    let _ = io::stdout().flush();
    print!("{}", ":".cyan());
    let _ = io::stdout().flush();
    let mut buffer: String = String::new();
    io::stdin().read_line(&mut buffer).unwrap();
    buffer
}


pub fn get_integer_input() -> i32 {
    let buffer = get_input();
    let buffer_cast_as_int = buffer.parse::<i32>();
    buffer_cast_as_int.unwrap()
}


pub fn get_input_as_bool() -> bool {
    let raw_input = get_input();
    let buffer: &str = raw_input.as_str().trim();
    let negative_inputs = ["n", "N"];

    if negative_inputs.contains(&buffer) {
        false
    }
    else {
        true
    }
}


pub fn get_input_with_message(message: &str) -> String {
    println!("\n{}", message.purple());
    let buffer = get_input();
    buffer
}


pub fn pause() {
    println!("{} [Press {} to continue, {} to exit]", "Continue?".purple().bold(), "enter".green(), "CTRL+C".red());
    let _ = get_input();
}

