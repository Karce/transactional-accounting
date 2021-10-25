use anyhow::{Context, Result};
use csv::{ReaderBuilder, Trim};
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use std::env;
use std::io;

#[derive(Serialize, Deserialize, Debug)]
struct Transaction {
    r#type: String,
    client: u16,
    tx: u32,
    amount: f32,
    disputed: Option<bool>,
}

#[derive(Serialize)]
struct Account {
    client: u16,
    available: f32,
    held: f32,
    total: f32,
    locked: bool,
}

fn main() -> Result<()> {
    //println!("Hello, world!");
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Please provide a CSV file to process transactions.");
    }

    process_input(&args[1])?;

    Ok(())
}

fn process_input(csv_path: &str) -> Result<HashMap<u16, Account>> {
    let mut rdr = ReaderBuilder::new()
        .trim(Trim::All)
        .from_path(csv_path)
        .with_context(|| format!("Failed to read provided file {}", csv_path))?;

    let mut state: HashMap<u16, Account> = HashMap::new();
    let mut transactions: HashMap<u32, Transaction> = HashMap::new();

    for result in rdr.deserialize() {
        // Handle errors here related to parsing this record.
        let mut transaction: Transaction = result?;
        process_transaction(&mut state, transaction, &mut transactions);
    }

    let mut wtr = csv::Writer::from_writer(io::stdout());

    for (_, account) in &state {
        wtr.serialize(account).unwrap();
    }

    Ok(state)
}

fn process_transaction(
    state: &mut HashMap<u16, Account>,
    mut transaction: Transaction,
    transactions: &mut HashMap<u32, Transaction>,
) {
    // This step is performed so we only use 4 digits of precision after the decimal.
    transaction.amount = (transaction.amount * 10000.0).trunc() / 10000.0;
    match transaction.r#type.as_str() {
        "deposit" => {
            if !state.contains_key(&transaction.client) {
                state.insert(
                    transaction.client,
                    Account {
                        client: transaction.client,
                        available: transaction.amount,
                        held: 0.0,
                        total: transaction.amount,
                        locked: false,
                    },
                );
            } else {
                let existing = state.get(&transaction.client).unwrap();
                state.insert(
                    transaction.client,
                    Account {
                        client: transaction.client,
                        available: existing.available + transaction.amount,
                        held: existing.held,
                        total: existing.total + transaction.amount,
                        locked: existing.locked,
                    },
                );
            }
        }
        "withdraw" => {
            if !state.contains_key(&transaction.client) {
                // No held account. Denied.
            } else {
                let existing = state.get(&transaction.client).unwrap();
                if existing.available < transaction.amount {
                    // Insufficient funds. Denied.
                } else {
                    state.insert(
                        transaction.client,
                        Account {
                            client: transaction.client,
                            available: existing.available - transaction.amount,
                            held: existing.held,
                            total: existing.total - transaction.amount,
                            locked: existing.locked,
                        },
                    );
                }
            }
        }
        "dispute" => {
            if !state.contains_key(&transaction.client)
                || !transactions.contains_key(&transaction.tx)
            {
                // No held account. Denied.
            } else {
                let existing = state.get(&transaction.client).unwrap();
                let old_transaction = transactions.get_mut(&transaction.tx).unwrap();
                old_transaction.disputed = Some(true);
                //transactions.insert(old_transaction.tx, *old_transaction);
                state.insert(
                    transaction.client,
                    Account {
                        client: transaction.client,
                        available: existing.available - old_transaction.amount,
                        held: existing.held + old_transaction.amount,
                        total: existing.total,
                        locked: existing.locked,
                    },
                );
            }
        }
        "resolve" => {
            if !state.contains_key(&transaction.client)
                || !transactions.contains_key(&transaction.tx)
            {
                // No held account. Denied.
            } else {
                let existing = state.get(&transaction.client).unwrap();
                let old_transaction = transactions.get_mut(&transaction.tx).unwrap();
                match old_transaction.disputed {
                    Some(flag) => {
                        if flag {
                            old_transaction.disputed = Some(false);
                            state.insert(
                                transaction.client,
                                Account {
                                    client: transaction.client,
                                    available: existing.available + old_transaction.amount,
                                    held: existing.held - old_transaction.amount,
                                    total: existing.total,
                                    locked: existing.locked,
                                },
                            );
                        }
                    }
                    None => {}
                }
            }
        }
        "chargeback" => {
            if !state.contains_key(&transaction.client)
                || !transactions.contains_key(&transaction.tx)
            {
                // No held account. Denied.
            } else {
                let existing = state.get(&transaction.client).unwrap();
                let old_transaction = transactions.get(&transaction.tx).unwrap();
                match old_transaction.disputed {
                    Some(flag) => {
                        if flag {
                            state.insert(
                                transaction.client,
                                Account {
                                    client: transaction.client,
                                    available: existing.available,
                                    held: existing.held - old_transaction.amount,
                                    total: existing.total - old_transaction.amount,
                                    locked: true,
                                },
                            );
                        }
                    }
                    None => {}
                }
            }
        }
        _ => {}
    }
    transaction.disputed = Some(false);
    transactions.insert(transaction.tx, transaction);
}

#[cfg(test)]
mod tests {
    use super::{process_input, Transaction};
    use rand::distributions::Alphanumeric;
    use rand::prelude::*;

    fn create_test_input() -> String {
        let mut csv_path: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        csv_path.insert_str(0, "test_csvs/test-");
        csv_path += ".csv";

        let num_lines: u8 = rand::random();
        let mut wtr = csv::Writer::from_path(&csv_path).unwrap();
        let t_types: Vec<&str> = vec!["deposit", "withdrawal", "dispute", "resolve", "chargeback"];
        for _i in 0..num_lines {
            for t_type in t_types.iter() {
                let client = thread_rng().gen_range(0..16);
                let tx = random::<u32>();
                let amount = random::<f32>();
                wtr.serialize(Transaction {
                    r#type: t_type.to_string(),
                    client,
                    tx,
                    amount,
                    disputed: None,
                })
                .unwrap();
            }
        }

        return csv_path;
    }

    #[test]
    fn test_deposit_regular_accounts() {
        let state = process_input("test_csvs/deposit1.csv").unwrap();
        assert_eq!(state.get(&1).unwrap().available, 3.0);
        assert_eq!(state.get(&1).unwrap().total, 3.0);

        assert_eq!(state.get(&2).unwrap().available, 2.0);
        assert_eq!(state.get(&2).unwrap().total, 2.0);

        assert_eq!(state.get(&3).unwrap().available, 2.0);
        assert_eq!(state.get(&3).unwrap().total, 2.0);

        assert_eq!(state.get(&999).unwrap().available, 2.0567);
        assert_eq!(state.get(&999).unwrap().total, 2.0567);
    }

    #[test]
    fn test_withdraw_regular_accounts() {
        let state = process_input("test_csvs/withdraw1.csv").unwrap();
        assert_eq!(state.get(&1).unwrap().available, 1.5);
        assert_eq!(state.get(&1).unwrap().total, 1.5);

        assert_eq!(state.get(&2).unwrap().available, 2.0);
        assert_eq!(state.get(&2).unwrap().total, 2.0);
    }
}
