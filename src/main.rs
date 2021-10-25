use anyhow::{Context, Result};
use csv::{ReaderBuilder, Trim};
use serde::{Serialize, Deserialize};
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

    let mut rdr = ReaderBuilder::new()
        .trim(Trim::All)
        .from_path(&args[1])
        .with_context(|| format!("Failed to read provided file {}", args[1]))?;

    for result in rdr.deserialize() {
        println!("{:?}", result);
        let transaction: Transaction = result?;
        println!("{:?}", transaction);
    }
    //_csv()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use rand::distributions::Alphanumeric;
    use rand::prelude::*;
    use super::Transaction;

    fn create_test_input() -> String {
        let mut csv_path: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        csv_path += ".csv";

        let num_lines: u16 = rand::random();
        let mut wtr = csv::Writer::from_path(&csv_path).unwrap();
        let t_types: Vec<&str> = vec!("deposit", "withdrawal", "dispute", "resolve", "chargeback");
        for _i in 0..num_lines {
            for t_type in t_types.iter() {
                let client = random::<u16>();
                let tx = random::<u32>();
                let amount = random::<f32>();
                wtr.serialize(Transaction {

                    r#type: t_type.to_string(),
                    client,
                    tx,
                    amount
                })
                /*
                wtr.write_record(&[
                    t_type,
                    &client,
                    &tx,
                    &amount,
                ])
                */
                .unwrap();
            }
        }

        return csv_path;
    }

    #[test]
    fn test_process_input() {
        println!("{}", create_test_input());
        assert!(false);
    }
}
