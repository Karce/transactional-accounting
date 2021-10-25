use std::env;
use serde::Deserialize;
use anyhow::{Context, Result};
use csv::{ReaderBuilder, Trim;

#[derive(Deserialize, Debug)]
struct Transaction {
    r#type: String,
    client: u16,
    tx: u32,
    amount: f32,
}

fn main() -> Result<()> {
    //println!("Hello, world!");
    let args: Vec<String> = env::args().collect();
    println!("{:?}", args);
    if args.len() < 2 {
        eprintln!("Please provide a CSV file to process transactions.");
    }

    let mut rdr = ReaderBuilder::new()
        .trim(Trim::All)
        .from_path(&args[1]).with_context(|| format!("Failed to read provided file {}", args[1]))?;

    for result in rdr.deserialize() {
        println!("{:?}", result);
        let transaction: Transaction = result?;
        println!("{:?}", transaction);
    }
    //_csv()?;
    Ok(())
}

mod tests {
    use rand::prelude::*;

    fn create_test_input() -> String {
        return csv_path;
    }

    #[test]
    fn test_parsing_input() {

    }
}
