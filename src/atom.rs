use rocksdb::{self, DBIterator, DBOptions, ReadOptions, SeekKey, Writable, DB};

pub struct QueryIter<'a> {
    inner: DBIterator<&'a DB>,
    reverse: bool,
}

impl<'a> QueryIter<'a> {
    pub fn new(db: &'a DB, seek: &'a [u8], reverse: bool) -> QueryIter<'a> {
        let opt = ReadOptions::default();
        let mut iter = db.iter_opt(opt);
        if reverse {
            iter.seek_for_prev(SeekKey::Key(seek));
        } else {
            iter.seek(SeekKey::Key(seek));
        }
        QueryIter {
            inner: iter,
            reverse,
        }
    }

    pub fn each<F>(&mut self, mut f: F)
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        loop {
            if !self.inner.valid() {
                break;
            }
            let key = self.inner.key();
            let value = self.inner.value();
            let cont = f(key, value);
            if !cont {
                break;
            }
            if self.reverse {
                self.inner.prev();
            } else {
                self.inner.next();
            }
        }
    }
}

pub struct AtomDB {
    db: DB,
}

impl AtomDB {
    pub fn new(path: &str) -> Result<Self, String> {
        let mut opt = DBOptions::default();
        opt.create_if_missing(true);
        let db = DB::open(opt, path)?;
        Ok(AtomDB { db })
    }

    pub fn put_item(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.db.put(key, value)?;
        Ok(())
    }

    pub fn get_item(&self, key: &[u8], buf: &mut Vec<u8>) -> Result<bool, String> {
        if let Some(value) = self.db.get(key)? {
            buf.extend_from_slice(&*value);
            return Ok(true);
        }
        return Ok(false);
    }

    pub fn query<'a>(&'a self, seek: &'a [u8], scan_reverse: bool) -> QueryIter<'a> {
        QueryIter::new(&self.db, seek, scan_reverse)
    }
}

#[cfg(test)]
mod tests {
    //use std::path::Path;
    use tempdir::TempDir;
    use super::*;

    fn open_test_db() -> (TempDir, AtomDB) {
        let dir = TempDir::new("tinyamodb_test").unwrap();
        let s = dir.path().to_str().unwrap();
        let atomdb = AtomDB::new(s).unwrap();
        (dir, atomdb)
    }

    #[test]
    fn test_put_get() {
        let (_, atomdb) = open_test_db();
        atomdb.put_item(b"hoge", b"hogevalue").unwrap();
        let mut buf = vec![];
        atomdb.get_item(b"hoge", &mut buf).unwrap();
        assert_eq!(b"hogevalue", buf.as_slice());
    }

    #[test]
    fn test_query() {
        let (_, atomdb) = open_test_db();
        atomdb.put_item(b"hogehoge", b"hogehogevalue").unwrap();
        atomdb.put_item(b"hogenanika", b"hogenanikavalue").unwrap();
        atomdb.put_item(b"hugahuga", b"hugahugavalue").unwrap();

        let mut ret = vec![];
        atomdb.query(b"hoge", false).each(|key, value| {
            ret.push((key.to_owned(), value.to_owned()));
            true
        });
        assert_eq!(vec![
            (b"hogehoge".to_vec(), b"hogehogevalue".to_vec()),
            (b"hogenanika".to_vec(), b"hogenanikavalue".to_vec()),
            (b"hugahuga".to_vec(), b"hugahugavalue".to_vec()),
        ], ret);

        ret.clear();
        atomdb.query(b"hoge", false).each(|key, value| {
            ret.push((key.to_owned(), value.to_owned()));
            false
        });
        assert_eq!(vec![
            (b"hogehoge".to_vec(), b"hogehogevalue".to_vec()),
        ], ret);

        ret.clear();
        atomdb.query(b"hoge", true).each(|key, value| {
            ret.push((key.to_owned(), value.to_owned()));
            true
        });
        assert_eq!(vec![] as Vec<(Vec<u8>, Vec<u8>)>, ret);

        ret.clear();
        atomdb.query(b"huga", false).each(|key, value| {
            ret.push((key.to_owned(), value.to_owned()));
            true
        });
        assert_eq!(vec![(b"hugahuga".to_vec(), b"hugahugavalue".to_vec())], ret);

        ret.clear();
        atomdb.query(b"huga", true).each(|key, value| {
            ret.push((key.to_owned(), value.to_owned()));
            true
        });
        assert_eq!(vec![
            (b"hogenanika".to_vec(), b"hogenanikavalue".to_vec()),
            (b"hogehoge".to_vec(), b"hogehogevalue".to_vec()),
        ], ret);
    }
}
