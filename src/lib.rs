// TODO(andrea): make it multi threaded. Also, does this approach come from a
// paper? I'd be interested in other ones. https://db.in.tum.de/~muehlbau/papers/mvcc.pdf
// TODO(andrea): I want less stringly-typed stuff.
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap},
    rc::Rc,
};

/// Value at a specific version.
#[derive(Debug)]
struct Value {
    /// Transaction id at which the value is created.
    tx_start_id: u64,
    /// Transaction id at which the value is deleted.
    tx_end_id: u64,
    /// The actual value.
    value: String,
}

/// State of a transaction.
#[derive(PartialEq, Debug)]
enum TxState {
    InProgress,
    Aborted,
    Committed,
}

/// Available isolation levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    SnapshotIsolation,
    Serializable,
}

/// A transaction
#[derive(Debug)]
struct Tx {
    /// The isolation level we picked for this transaction.
    isolation_level: IsolationLevel,
    /// The transaction's state.
    state: TxState,

    /// Monotonically increasing transaction id.
    id: u64,

    /// What other transaction were in progress.
    // Only used by isolation levels >= RepeatableRead.
    in_progress: BTreeSet<u64>,

    // Only used by isolation levels >= SnapshotIsolation.
    /// What keys were written by the transaction.
    write_set: BTreeSet<String>,
    /// What keys were read by the transaction.
    read_set: BTreeSet<String>,
}

/// A database to store the key-value pairs and manage tx ids.
#[derive(Debug)]
struct Database {
    /// Mapping from key to value versions, sorted by version.
    store: HashMap<String, Vec<Value>>,

    txs: BTreeMap<u64, Tx>,

    /// Next available transaction id, `0` value is never used.
    next_tx_id: u64,

    isolation_level: IsolationLevel,
}

impl Database {
    fn new() -> Self {
        Self {
            store: HashMap::new(),
            txs: BTreeMap::new(),
            next_tx_id: 1,
            isolation_level: IsolationLevel::ReadUncommitted,
        }
    }

    fn with_isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.isolation_level = isolation_level;
        self
    }

    fn txs_in_progress(&self) -> BTreeSet<u64> {
        self.txs
            .iter()
            .filter_map(|(&tx_id, tx)| {
                if tx.state == TxState::InProgress {
                    Some(tx_id)
                } else {
                    None
                }
            })
            .collect()
    }

    fn new_tx(&mut self) -> &mut Tx {
        let tx = Tx {
            isolation_level: self.isolation_level,
            state: TxState::InProgress,
            id: self.next_tx_id,
            in_progress: self.txs_in_progress(),
            write_set: BTreeSet::new(),
            read_set: BTreeSet::new(),
        };
        self.next_tx_id += 1;
        self.txs.entry(tx.id).or_insert(tx)
    }

    // TODO(andrea): meh
    fn complete_tx(&mut self, tx_id: u64, new_state: TxState) -> Result<(), String> {
        let has_found_conflict = if new_state == TxState::Committed {
            let tx = self.txs.get(&tx_id).unwrap();
            match tx.isolation_level {
                IsolationLevel::SnapshotIsolation => self.has_conflict(tx, |tx_1, tx_2| {
                    !tx_1.write_set.is_disjoint(&tx_2.write_set)
                }),
                IsolationLevel::Serializable => self.has_conflict(tx, |tx_1, tx_2| {
                    !tx_1.write_set.is_disjoint(&tx_2.read_set)
                        || !tx_1.read_set.is_disjoint(&tx_2.write_set)
                }),
                _ => false,
            }
        } else {
            false
        };

        let tx = self.txs.get_mut(&tx_id).unwrap();
        if has_found_conflict {
            tx.state = TxState::Aborted;
            // TODO(andrea): mehhh
            if tx.isolation_level == IsolationLevel::SnapshotIsolation {
                return Err("write-write conflict".to_string());
            }
            if tx.isolation_level == IsolationLevel::Serializable {
                return Err("write-read conflict".to_string());
            }
        }
        tx.state = new_state;

        Ok(())
    }

    fn is_visible(&self, tx: &Tx, value: &Value) -> bool {
        match tx.isolation_level {
            IsolationLevel::ReadUncommitted => value.tx_end_id == 0,
            IsolationLevel::ReadCommitted => {
                let value_created_in_own_tx = value.tx_start_id == tx.id;
                let value_created_in_committed_tx =
                    self.txs[&value.tx_start_id].state == TxState::Committed;
                if !value_created_in_own_tx && !value_created_in_committed_tx {
                    return false;
                }

                let value_deleted_in_own_tx = value.tx_end_id == tx.id;
                let value_deleted_in_committed_tx = (value.tx_end_id > 0)
                    && (self.txs[&value.tx_end_id].state == TxState::Committed);
                if value_deleted_in_own_tx || value_deleted_in_committed_tx {
                    return false;
                }

                true
            }
            IsolationLevel::RepeatableRead
            | IsolationLevel::SnapshotIsolation
            | IsolationLevel::Serializable => {
                let value_created_in_tx_started_later = value.tx_start_id > tx.id;
                let value_created_in_in_progress_tx = tx.in_progress.contains(&value.tx_start_id);
                if value_created_in_tx_started_later || value_created_in_in_progress_tx {
                    return false;
                }

                let value_created_in_own_tx = value.tx_start_id == tx.id;
                let value_created_in_committed_tx =
                    self.txs[&value.tx_start_id].state == TxState::Committed;
                if !value_created_in_own_tx && !value_created_in_committed_tx {
                    return false;
                }

                let value_deleted_in_own_tx = value.tx_end_id == tx.id;
                if value_deleted_in_own_tx {
                    return false;
                }

                let value_deleted_in_committed_tx_that_started_before = value.tx_end_id < tx.id
                    && value.tx_end_id > 0
                    && self.txs[&value.tx_end_id].state == TxState::Committed
                    && !tx.in_progress.contains(&value.tx_end_id);
                if value_deleted_in_committed_tx_that_started_before {
                    return false;
                }

                true
            }
        }
    }

    fn has_conflict<F: Fn(&Tx, &Tx) -> bool>(&self, tx: &Tx, conflict_fn: F) -> bool {
        // For each transaction `other_tx` that was in progress when `tx` started.
        for in_progress_tx_id in tx.in_progress.iter() {
            if let Some(other_tx) = self.txs.get(in_progress_tx_id) {
                // If `other_tx` committed, then check whether it conflicts with `tx`.
                if other_tx.state == TxState::Committed {
                    if conflict_fn(tx, other_tx) {
                        return true;
                    }
                }
            }
        }

        // For each transaction `other_tx` that started after `tx`.
        for started_after_tx_id in tx.id..self.next_tx_id {
            if let Some(other_tx) = self.txs.get(&started_after_tx_id) {
                if other_tx.state == TxState::Committed {
                    // If `other_tx` committed, then check whether it conflicts with `tx`.
                    if conflict_fn(tx, other_tx) {
                        return true;
                    }
                }
            }
        }

        false
    }
}

enum Cmd {
    Begin,
    Abort,
    Commit,
    Get(String),
    Set(String, String),
    Delete(String),
}

struct Connection {
    tx_id: Option<u64>,
    db: Rc<RefCell<Database>>,
}

impl Connection {
    fn new(db: Rc<RefCell<Database>>) -> Self {
        Self { tx_id: None, db }
    }

    fn exec_cmd(&mut self, command: Cmd) -> Result<String, String> {
        match command {
            Cmd::Begin => {
                let mut db = self.db.borrow_mut();
                assert!(self.tx_id.is_none(), "A transaction is running already");
                let tx = db.new_tx();
                self.tx_id = Some(tx.id);
                let result = format!("{}", tx.id);
                return Ok(result);
            }
            Cmd::Abort => {
                let mut db = self.db.borrow_mut();
                assert_eq!(db.txs[&self.tx_id.unwrap()].state, TxState::InProgress);
                db.complete_tx(self.tx_id.unwrap(), TxState::Aborted)?;
                self.tx_id = None;
                return Ok("".to_string());
            }
            Cmd::Commit => {
                let mut db = self.db.borrow_mut();
                assert_eq!(db.txs[&self.tx_id.unwrap()].state, TxState::InProgress);
                db.complete_tx(self.tx_id.unwrap(), TxState::Committed)?;
                self.tx_id = None;
                return Ok("".to_string());
            }
            Cmd::Get(key) => {
                let mut db = self.db.borrow_mut();
                assert_eq!(db.txs[&self.tx_id.unwrap()].state, TxState::InProgress);

                let tx = db.txs.get_mut(&self.tx_id.unwrap()).unwrap();
                tx.read_set.insert(key.clone());

                let tx = db.txs.get(&self.tx_id.unwrap()).unwrap();

                let ok_not_found = Ok("key not found".to_string());
                if let Some(versions) = db.store.get(&key) {
                    for value in versions.iter().rev() {
                        if db.is_visible(&tx, value) {
                            return Ok(value.value.clone());
                        }
                    }
                }
                ok_not_found
            }
            Cmd::Set(key, value) => {
                let search_result = self.find_visible(&key);

                // TODO(andrea): understand why the line below does not work. Hint: "reborrow"
                // let mut db = self.db.borrow_mut();
                // But the one below works.
                let db = &mut *self.db.borrow_mut();

                // If we have found it, then make it not visible anymore.
                for (ix, tx_id) in search_result {
                    db.store.get_mut(&key).unwrap()[ix].tx_end_id = tx_id;
                }

                let tx = db.txs.get_mut(&self.tx_id.unwrap()).unwrap();
                tx.write_set.insert(key.clone());

                db.store.entry(key).or_insert(vec![]).push(Value {
                    tx_start_id: tx.id,
                    tx_end_id: 0,
                    value: value.clone(),
                });

                return Ok(value);
            }
            Cmd::Delete(key) => {
                let search_result = self.find_visible(&key);
                let db = &mut *self.db.borrow_mut();

                // If we have found it, then make it not visible anymore.
                for (ix, tx_id) in &search_result {
                    db.store.get_mut(&key).unwrap()[*ix].tx_end_id = *tx_id;
                }

                if search_result.len() > 0 {
                    let tx = db.txs.get_mut(&self.tx_id.unwrap()).unwrap();
                    tx.write_set.insert(key.clone());
                    Ok("".to_string())
                } else {
                    Err("cannot delete key that does not exist".to_string())
                }
            }
        }
    }

    // TODO(andrea): I really dislike this function, the double borrow and the for loop, I wish I could find a way
    // to avoid
    fn find_visible(&mut self, key: &String) -> Vec<(usize, u64)> {
        let db = self.db.borrow();
        assert_eq!(db.txs[&self.tx_id.unwrap()].state, TxState::InProgress);

        let tx = &db.txs[&self.tx_id.unwrap()];

        let mut found = vec![];
        // Find the version that will stop being visible, if it exists.
        if let Some(versions) = db.store.get(key) {
            for (ix, value) in versions.iter().enumerate().rev() {
                if db.is_visible(&tx, value) {
                    found.push((ix, tx.id));
                }
            }
        }
        found
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;

    #[test]
    #[should_panic(expected = "A transaction is running already")]
    fn creating_transaction_twice() {
        let db = Rc::new(RefCell::new(Database::new()));
        let mut c = Connection::new(db);
        assert_eq!(c.exec_cmd(Cmd::Begin), Ok("1".to_string()));
        let _ = c.exec_cmd(Cmd::Begin);
    }

    #[test]
    fn aborting_transaction() {
        let db = Rc::new(RefCell::new(Database::new()));
        let mut c = Connection::new(db);
        assert_eq!(c.exec_cmd(Cmd::Begin), Ok("1".to_string()));
        assert_eq!(c.exec_cmd(Cmd::Abort), Ok("".to_string()));
    }

    #[test]
    fn committing_transaction() {
        let db = Rc::new(RefCell::new(Database::new()));
        let mut c = Connection::new(db);
        assert_eq!(c.exec_cmd(Cmd::Begin), Ok("1".to_string()));
        assert_eq!(c.exec_cmd(Cmd::Commit), Ok("".to_string()));
    }

    #[test]
    fn reading_non_existing_key() {
        let db = Rc::new(RefCell::new(Database::new()));
        let mut c = Connection::new(db);
        assert_eq!(c.exec_cmd(Cmd::Begin), Ok("1".to_string()));
        assert_eq!(
            c.exec_cmd(Cmd::Get("k1".to_string())),
            Ok("key not found".to_string())
        );
        assert_eq!(c.exec_cmd(Cmd::Commit), Ok("".to_string()));
    }

    #[test]
    fn read_uncommitted_read() -> Result<(), String> {
        let db = Rc::new(RefCell::new(Database::new()));
        let mut c = Connection::new(db.clone());

        let n_ops = 5;

        // Set then get some keys
        for i in 0..n_ops {
            let k = format!("k_{}", i);
            let v = format!("v_{}", i);

            assert_eq!(c.exec_cmd(Cmd::Begin), Ok(format!("{}", 2 * i + 1)));
            assert_eq!(c.exec_cmd(Cmd::Set(k.clone(), v.clone())), Ok(v.clone()));
            assert_eq!(c.exec_cmd(Cmd::Commit), Ok("".to_string()));

            assert_eq!(c.exec_cmd(Cmd::Begin), Ok(format!("{}", 2 * i + 2)));
            assert_eq!(c.exec_cmd(Cmd::Get(k.clone())), Ok(v.clone()));
            assert_eq!(c.exec_cmd(Cmd::Commit), Ok("".to_string()));
        }

        // Do it again so that we have multiple versions
        for i in 0..n_ops {
            let k = format!("k_{}", i);
            let v = format!("v_{}", i);

            c.exec_cmd(Cmd::Begin)?;
            assert_eq!(c.exec_cmd(Cmd::Set(k.clone(), v.clone())), Ok(v.clone()));
            c.exec_cmd(Cmd::Commit)?;

            c.exec_cmd(Cmd::Begin)?;
            assert_eq!(c.exec_cmd(Cmd::Get(k.clone())), Ok(v.clone()));
            c.exec_cmd(Cmd::Commit)?;
        }

        // And finally check that we have the correct versions.
        for (_, versions) in &db.borrow().store {
            assert_eq!(versions.len(), 2);
        }
        Ok(())
    }

    #[test]
    fn test_read_uncommitted() -> Result<(), String> {
        let db = Rc::new(RefCell::new(Database::new()));

        let mut c1 = Connection::new(db.clone());
        c1.exec_cmd(Cmd::Begin)?;
        let mut c2 = Connection::new(db.clone());
        c2.exec_cmd(Cmd::Begin)?;

        c1.exec_cmd(Cmd::Set("x".to_string(), "hey".to_string()))?;

        // x should be visible internally.
        assert_eq!(c1.exec_cmd(Cmd::Get("x".to_string()))?, "hey");

        // x should be visible by other transactions too, becase of read uncommitted.
        // this is the "dirty read".
        assert_eq!(c2.exec_cmd(Cmd::Get("x".to_string()))?, "hey");

        // and deletes will be immediately visible to everyone.
        assert_eq!(c1.exec_cmd(Cmd::Delete("x".to_string()))?, "");
        assert_eq!(c1.exec_cmd(Cmd::Get("x".to_string()))?, "key not found");
        assert_eq!(c2.exec_cmd(Cmd::Get("x".to_string()))?, "key not found");
        Ok(())
    }

    #[test]
    fn test_read_committed() -> Result<(), String> {
        let db = Rc::new(RefCell::new(
            Database::new().with_isolation_level(IsolationLevel::ReadCommitted),
        ));

        let mut c1 = Connection::new(db.clone());
        let mut c2 = Connection::new(db.clone());
        let mut c3 = Connection::new(db.clone());
        let mut c4 = Connection::new(db.clone());

        c1.exec_cmd(Cmd::Begin)?;
        c2.exec_cmd(Cmd::Begin)?;

        c1.exec_cmd(Cmd::Set("x".to_string(), "hey".to_string()))?;
        assert_eq!(c1.exec_cmd(Cmd::Get("x".to_string()))?, "hey");
        // c2 cannot see uncommitted values.
        assert_eq!(c2.exec_cmd(Cmd::Get("x".to_string()))?, "key not found");

        c1.exec_cmd(Cmd::Commit)?;
        // after c1 commits c2 should then be able to see the value, this is a "non-repeatable read"
        assert_eq!(c2.exec_cmd(Cmd::Get("x".to_string()))?, "hey");

        c3.exec_cmd(Cmd::Begin)?;

        // local change must be visible locally.
        c3.exec_cmd(Cmd::Set("x".to_string(), "yall".to_string()))?;
        assert_eq!(c3.exec_cmd(Cmd::Get("x".to_string()))?, "yall");

        // but not to others.
        assert_eq!(c2.exec_cmd(Cmd::Get("x".to_string()))?, "hey");

        // even after it aborts.
        c3.exec_cmd(Cmd::Abort)?;
        assert_eq!(c2.exec_cmd(Cmd::Get("x".to_string()))?, "hey");

        // and if we delete it, it is indeed deleted locally.
        c2.exec_cmd(Cmd::Delete("x".to_string()))?;
        assert_eq!(c2.exec_cmd(Cmd::Get("x".to_string()))?, "key not found");

        c2.exec_cmd(Cmd::Commit)?;

        // Given that c2 has deleted x and commited, no other transaction should see it.
        c4.exec_cmd(Cmd::Begin)?;
        assert_eq!(c4.exec_cmd(Cmd::Get("x".to_string()))?, "key not found");

        Ok(())
    }

    #[test]
    fn test_repeatable_read() -> Result<(), String> {
        let db = Rc::new(RefCell::new(
            Database::new().with_isolation_level(IsolationLevel::RepeatableRead),
        ));

        let mut c1 = Connection::new(db.clone());
        let mut c2 = Connection::new(db.clone());
        let mut c3 = Connection::new(db.clone());
        let mut c4 = Connection::new(db.clone());
        let mut c5 = Connection::new(db.clone());

        c1.exec_cmd(Cmd::Begin)?;
        c2.exec_cmd(Cmd::Begin)?;

        // Local change is visible locally.
        c1.exec_cmd(Cmd::Set("x".to_string(), "hey".to_string()))?;
        assert_eq!(c1.exec_cmd(Cmd::Get("x".to_string()))?, "hey");

        // Update not available to this transaction since it is committed.
        assert_eq!(c2.exec_cmd(Cmd::Get("x".to_string()))?, "key not found");

        // Even after committing, it is not visible in an existing transaction.
        c1.exec_cmd(Cmd::Commit)?;
        assert_eq!(c2.exec_cmd(Cmd::Get("x".to_string()))?, "key not found");

        // But it should be visible in a new transaction.
        c3.exec_cmd(Cmd::Begin)?;
        assert_eq!(c3.exec_cmd(Cmd::Get("x".to_string()))?, "hey");

        // Local change is visible locally.
        c3.exec_cmd(Cmd::Set("x".to_string(), "yall".to_string()))?;
        assert_eq!(c3.exec_cmd(Cmd::Get("x".to_string()))?, "yall");

        // But not to others.
        assert_eq!(c2.exec_cmd(Cmd::Get("x".to_string()))?, "key not found");

        // And still not visible, regardless of the abort.
        c3.exec_cmd(Cmd::Abort)?;
        assert_eq!(c2.exec_cmd(Cmd::Get("x".to_string()))?, "key not found");

        // And the aborted set is still not on a new transaction.
        c4.exec_cmd(Cmd::Begin)?;
        assert_eq!(c4.exec_cmd(Cmd::Get("x".to_string()))?, "hey");

        c4.exec_cmd(Cmd::Delete("x".to_string()))?;
        c4.exec_cmd(Cmd::Commit)?;

        // But the delete is visible to new transactions now that it is committed.
        c5.exec_cmd(Cmd::Begin)?;
        assert_eq!(c5.exec_cmd(Cmd::Get("x".to_string()))?, "key not found");

        Ok(())
    }

    #[test]
    fn test_snapshot_isolation() -> Result<(), String> {
        let db = Rc::new(RefCell::new(
            Database::new().with_isolation_level(IsolationLevel::SnapshotIsolation),
        ));

        let mut c1 = Connection::new(db.clone());
        let mut c2 = Connection::new(db.clone());
        let mut c3 = Connection::new(db.clone());

        c1.exec_cmd(Cmd::Begin)?;
        c2.exec_cmd(Cmd::Begin)?;
        c3.exec_cmd(Cmd::Begin)?;

        // Then c1 should be able to commit without issues.
        c1.exec_cmd(Cmd::Set("x".to_string(), "hey".to_string()))?;
        c1.exec_cmd(Cmd::Commit)?;

        // But c2, which started at the same time, should crash.
        c2.exec_cmd(Cmd::Set("x".to_string(), "hey".to_string()))?;
        assert!(c2.exec_cmd(Cmd::Commit) == Err("write-write conflict".to_string()));

        // Finally, c3 touches unrelated keys, so no conflict.
        c3.exec_cmd(Cmd::Set("y".to_string(), "no conflict".to_string()))?;
        c3.exec_cmd(Cmd::Commit)?;

        Ok(())
    }

    #[test]
    fn test_serializable() -> Result<(), String> {
        let db = Rc::new(RefCell::new(
            Database::new().with_isolation_level(IsolationLevel::Serializable),
        ));

        let mut c1 = Connection::new(db.clone());
        let mut c2 = Connection::new(db.clone());
        let mut c3 = Connection::new(db.clone());

        c1.exec_cmd(Cmd::Begin)?;
        c2.exec_cmd(Cmd::Begin)?;
        c3.exec_cmd(Cmd::Begin)?;

        // Then c1 should be able to commit without issues.
        c1.exec_cmd(Cmd::Set("x".to_string(), "hey".to_string()))?;
        c1.exec_cmd(Cmd::Commit)?;

        // c2 should not see x as c1 has not committed.
        assert_eq!(c2.exec_cmd(Cmd::Get("x".to_string()))?, "key not found");
        // And still, this is not serializable because if they were serializable, x would exists since c1 finished.
        assert!(c2.exec_cmd(Cmd::Commit) == Err("write-read conflict".to_string()));

        // Again, unrelated keys don't conflict.
        c3.exec_cmd(Cmd::Set("y".to_string(), "no conflict".to_string()))?;
        c3.exec_cmd(Cmd::Commit)?;

        Ok(())
    }
}
