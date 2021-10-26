/*
 * Copyright (C) 2021 Keaton Bruce
 *
 * This file is part of transactional-accounting.
 *
 * transactional-accounting is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * transactional-accounting is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with transactional-accounting. If not, see <http://www.gnu.org/licenses/>.
 *
 */

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
    amount: Option<f32>,
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
        let mut transaction: Transaction = result?;
        // Handle the case of locked accounts after a chargeback.
        if let Some(account) = state.get(&transaction.client) {
            if account.locked {
                continue;
            }
        }
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
    if transaction.amount.is_some() {
        transaction.amount = Some((transaction.amount.unwrap() * 10000.0).trunc() / 10000.0);
    }
    match transaction.r#type.as_str() {
        "deposit" => {
            if !state.contains_key(&transaction.client) {
                state.insert(
                    transaction.client,
                    Account {
                        client: transaction.client,
                        available: transaction.amount.unwrap(),
                        held: 0.0,
                        total: transaction.amount.unwrap(),
                        locked: false,
                    },
                );
            } else {
                let existing = state.get(&transaction.client).unwrap();
                state.insert(
                    transaction.client,
                    Account {
                        client: transaction.client,
                        available: existing.available + transaction.amount.unwrap(),
                        held: existing.held,
                        total: existing.total + transaction.amount.unwrap(),
                        locked: existing.locked,
                    },
                );
            }
            transaction.disputed = Some(false);
            transactions.insert(transaction.tx, transaction);
        }
        "withdrawal" => {
            if !state.contains_key(&transaction.client) {
                // No held account. Denied.
            } else {
                let existing = state.get(&transaction.client).unwrap();
                if existing.available < transaction.amount.unwrap() {
                    // Insufficient funds. Denied.
                } else {
                    state.insert(
                        transaction.client,
                        Account {
                            client: transaction.client,
                            available: existing.available - transaction.amount.unwrap(),
                            held: existing.held,
                            total: existing.total - transaction.amount.unwrap(),
                            locked: existing.locked,
                        },
                    );
                }
            }
            transaction.disputed = Some(false);
            transactions.insert(transaction.tx, transaction);
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
                println!("{:?}", old_transaction);
                let mut available: f32 = existing.available;
                let mut held: f32 = existing.held;
                if old_transaction.r#type == "deposit" {
                    available -= old_transaction.amount.unwrap();
                    held += old_transaction.amount.unwrap();
                } else if old_transaction.r#type == "withdrawal" {
                    // I don't think it make sense to make funds available yet on
                    // a withdrawal until it is finalized.
                    // Do disputes apply to withdrawals?
                    // You don't chargeback a withdrawal.
                    //available += old_transaction.amount.unwrap();
                    //held -= old_transaction.amount.unwrap();
                }
                state.insert(
                    transaction.client,
                    Account {
                        client: transaction.client,
                        available,
                        held,
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
                                    available: existing.available + old_transaction.amount.unwrap(),
                                    held: existing.held - old_transaction.amount.unwrap(),
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
                                    held: existing.held - old_transaction.amount.unwrap(),
                                    total: existing.total - old_transaction.amount.unwrap(),
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
}

#[cfg(test)]
mod tests {
    use super::process_input;

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

        assert!(!state.contains_key(&3));
    }

    #[test]
    fn test_dispute_regular_deposit() {
        let state = process_input("test_csvs/dispute1.csv").unwrap();
        assert_eq!(state.get(&1).unwrap().available, 1.0);
        assert_eq!(state.get(&1).unwrap().held, 2.5);
        assert_eq!(state.get(&1).unwrap().total, 3.5);
        assert_eq!(state.get(&1).unwrap().locked, false);
    }

    #[test]
    fn test_dispute_regular_withdrawal() {
        // I'm defining withdrawals as undisputable since
        // performing chargebacks on a withdrawal doesn't make sense.
        let state = process_input("test_csvs/dispute2.csv").unwrap();
        assert_eq!(state.get(&1).unwrap().available, 2.0);
        assert_eq!(state.get(&1).unwrap().held, 0.0);
        assert_eq!(state.get(&1).unwrap().total, 2.0);
        assert_eq!(state.get(&1).unwrap().locked, false);
    }

    #[test]
    fn test_resolve_regular_dispute() {
        let state = process_input("test_csvs/resolve1.csv").unwrap();
        assert_eq!(state.get(&1).unwrap().available, 3.5);
        assert_eq!(state.get(&1).unwrap().held, 0.0);
        assert_eq!(state.get(&1).unwrap().total, 3.5);
        assert_eq!(state.get(&1).unwrap().locked, false);
    }

    #[test]
    fn test_chargeback_regular_dispute() {
        let state = process_input("test_csvs/chargeback1.csv").unwrap();
        assert_eq!(state.get(&1).unwrap().available, 1.0);
        assert_eq!(state.get(&1).unwrap().held, 0.0);
        assert_eq!(state.get(&1).unwrap().total, 1.0);
        assert_eq!(state.get(&1).unwrap().locked, true);
    }

    #[test]
    fn test_chargeback_more_disputes() {
        let state = process_input("test_csvs/chargeback2.csv").unwrap();
        assert_eq!(state.get(&1).unwrap().available, 1.0);
        assert_eq!(state.get(&1).unwrap().held, 0.0);
        assert_eq!(state.get(&1).unwrap().total, 1.0);
        assert_eq!(state.get(&1).unwrap().locked, true);
    }
}
