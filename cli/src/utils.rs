use colored::*;
use regex::Regex;
use std::{
    fs::{OpenOptions},
    io::{self, Write},
    path::Path,
    process::Command,
};
use crate::filesystem;

pub fn get_input() -> String {
    let _ = io::stdout().flush();
    print!("{}", ":".cyan());
    let _ = io::stdout().flush();
    let mut buffer: String = String::new();
    io::stdin().read_line(&mut buffer).unwrap();
    buffer.trim().to_string()
}


pub fn get_integer_input() -> i32 {
    let buffer = get_input();
    let buffer_cast_as_int = buffer.parse::<i32>();
    buffer_cast_as_int.unwrap()
}


pub fn get_input_as_bool() -> bool {
    let raw_input = get_input();
    let buffer: &str = raw_input.as_str().trim();
    let negative_inputs = ["n", "N", "f", "false", "False"];

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
    println!("[Press {} to continue, {} to exit]", "enter".green(), "CTRL+C".red());
    let _ = get_input();
}


pub fn is_valid_cron_expression(expression: &str) -> bool {
    lazy_static! {
        static ref RE: Regex = Regex::new(r#"(@(annually|yearly|monthly|weekly|daily|hourly|reboot))|(@every (\d+(ns|us|µs|ms|s|m|h))+)|(((([\d]+,)+\d+|(\d+(/|-)\d+)|\d+|\*|(\d|/|\*)) ?){5,7})"#).unwrap();
    }
    RE.is_match(expression)
}

pub fn is_cron_expression_registered() -> bool {
    let result = Command::new("sh")
        .args(&["-c", "crontab -l | fgrep \"whale etl\""])
        .output().expect("Fgrep failed.");
    let stdout = String::from_utf8(result.stdout)
        .unwrap();
    if !stdout.trim().is_empty() {
        true
    }
    else {
        false
    }
}


pub fn convert_table_name_to_file_name(table_name: &str) -> String {
    let metadata_path = filesystem::get_metadata_dirname();
    format!("{}/{}.md", metadata_path, table_name)
}


pub fn touch(path: &Path) -> io::Result<()> {
    match OpenOptions::new().create(true).write(true).open(path) {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_file_name_conversion() {
        let table_name = "hello";
        let actual_file_name = shellexpand::tilde("~/.whale/metadata/hello.md");
        let file_name = convert_table_name_to_file_name(table_name);
        assert_eq!(file_name, actual_file_name);
    }
}
