use rusqlite::{params, Connection, OptionalExtension};
use time::OffsetDateTime;

#[derive(Clone)]
pub struct FileState {
    pub path: String,
    pub mtime: i64,
    pub size: i64,
    pub hash: String,
    pub lines_read: i64,
    pub bytes_read: i64,
    pub last_line: Option<String>,
    pub last_line_hash: Option<String>,
    pub last_line_index: Option<i64>,
    pub updated_at: i64,
}

pub struct Record {
    pub sn: String,
    pub datetime: String,
    pub result: String,
    pub source_path: String,
    pub row_index: i64,
    pub line_hash: String,
}

pub struct Db {
    conn: Connection,
}

impl Db {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        let s = Self { conn };
        s.init()?;
        Ok(s)
    }

    fn init(&self) -> anyhow::Result<()> {
        self.conn.execute_batch(
            r#"
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS file_state(
              path TEXT PRIMARY KEY,
              mtime INTEGER NOT NULL,
              size INTEGER NOT NULL,
              hash TEXT NOT NULL,
              lines_read INTEGER NOT NULL,
              bytes_read INTEGER NOT NULL,
              last_line TEXT,
              last_line_hash TEXT,
              last_line_index INTEGER,
              updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS records(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              sn TEXT NOT NULL,
              datetime TEXT NOT NULL,
              result TEXT NOT NULL,
              source_path TEXT NOT NULL,
              row_index INTEGER NOT NULL,
              line_hash TEXT NOT NULL,
              created_at INTEGER NOT NULL,
              UNIQUE(source_path, row_index),
              UNIQUE(sn, datetime, result, source_path)
            );
            "#,
        )?;
        Ok(())
    }

    pub fn load_file_state(&self, path: &str) -> anyhow::Result<Option<FileState>> {
        let mut stmt = self.conn.prepare(
            "SELECT path,mtime,size,hash,lines_read,bytes_read,last_line,last_line_hash,last_line_index,updated_at
             FROM file_state WHERE path=?1",
        )?;
        let row = stmt
            .query_row(params![path], |r| {
                Ok(FileState {
                    path: r.get(0)?,
                    mtime: r.get(1)?,
                    size: r.get(2)?,
                    hash: r.get(3)?,
                    lines_read: r.get(4)?,
                    bytes_read: r.get(5)?,
                    last_line: r.get(6)?,
                    last_line_hash: r.get(7)?,
                    last_line_index: r.get(8)?,
                    updated_at: r.get(9)?,
                })
            })
            .optional()?;
        Ok(row)
    }

    pub fn upsert_file_state(&self, s: &FileState) -> anyhow::Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO file_state(path,mtime,size,hash,lines_read,bytes_read,last_line,last_line_hash,last_line_index,updated_at)
            VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
            ON CONFLICT(path) DO UPDATE SET
              mtime=excluded.mtime,
              size=excluded.size,
              hash=excluded.hash,
              lines_read=excluded.lines_read,
              bytes_read=excluded.bytes_read,
              last_line=excluded.last_line,
              last_line_hash=excluded.last_line_hash,
              last_line_index=excluded.last_line_index,
              updated_at=excluded.updated_at
            "#,
            params![
                s.path,
                s.mtime,
                s.size,
                s.hash,
                s.lines_read,
                s.bytes_read,
                s.last_line,
                s.last_line_hash,
                s.last_line_index,
                s.updated_at
            ],
        )?;
        Ok(())
    }

    pub fn insert_record(&self, r: &Record) -> anyhow::Result<()> {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        self.conn.execute(
            r#"
            INSERT OR IGNORE INTO records(sn,datetime,result,source_path,row_index,line_hash,created_at)
            VALUES(?1,?2,?3,?4,?5,?6,?7)
            "#,
            params![
                r.sn,
                r.datetime,
                r.result,
                r.source_path,
                r.row_index,
                r.line_hash,
                now
            ],
        )?;
        Ok(())
    }
}
