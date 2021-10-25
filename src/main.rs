use anyhow::{Context, Result};
use csv::{ReaderBuilder, Trim};
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Serialize, Deserialize, Debug)]
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

    process_input(&args[1])?;

    Ok(())
}

fn process_input(csv_path: &str) -> Result<()> {
    let mut rdr = ReaderBuilder::new()
        .trim(Trim::All)
        .from_path(csv_path)
        .with_context(|| format!("Failed to read provided file {}", csv_path))?;

    for result in rdr.deserialize() {
        //println!("{:?}", result);
        // Handle errors here related to parsing this record.
        let transaction: Transaction = result?;
        println!("{:?}", transaction);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Transaction, process_input};
    use rand::distributions::Alphanumeric;
    use rand::prelude::*;

    fn create_test_input() -> String {
        let mut csv_path: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        csv_path.insert_str(0, "test-");
        csv_path += ".csv";

        let num_lines: u8 = rand::random();
        let mut wtr = csv::Writer::from_path(&csv_path).unwrap();
        let t_types: Vec<&str> = vec!["deposit", "withdrawal", "dispute", "resolve", "chargeback"];
        for _i in 0..num_lines {
            for t_type in t_types.iter() {
                let client = random::<u16>();
                let tx = random::<u32>();
                let amount = random::<f32>();
                wtr.serialize(Transaction {
                    r#type: t_type.to_string(),
                    client,
                    tx,
                    amount,
                })
                .unwrap();
            }
        }

        return csv_path;
    }

    #[test]
    fn test_process_input() {
        let test_file: String = create_test_input();
        println!("{}", &test_file);
        process_input(&test_file).unwrap();
        assert!(false);
    }
}
