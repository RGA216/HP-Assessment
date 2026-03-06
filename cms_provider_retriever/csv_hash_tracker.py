# standard library imports
from ast import Return
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


class CSVDownloadHashTracker:

    """ Persist SHA-256 hashes for downloaded CSV files. """


    def __init__(self, db_path='download_hashes.db'):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_db()

    def _initialize_db(self):
        """ Create the database and table if they don't exist. """
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                ''' CREATE TABLE IF NOT EXISTS csv_file_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source_url TEXT NOT NULL,
                        local_path TEXT NOT NULL,
                        sha256 TEXT NOT NULL,
                        file_size_bytes INTEGER NOT NULL,
                        download_date DATE NOT NULL
                    )
                '''
            )

    def _record_download(self, source_url, local_path, file_size_bytes, sha256):
        """ Record file metadata and hash for a completed CSV write. """
        download_date = datetime.now(timezone.utc).date().isoformat()
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                ''' INSERT INTO csv_file_history (
                        source_url,
                        local_path,
                        sha256,
                        file_size_bytes,
                        download_date
                    ) VALUES (?, ?, ?, ?, ?)
                ''',
                (source_url, local_path, sha256, file_size_bytes, download_date,),
            )
        return sha256

    def _check_for_existing_hash(self, source_url: str, sha256: str):
        """ Return True when the same source URL/hash pair already exists. """
        with sqlite3.connect(self.db_path) as connection:
            row = connection.execute(
                ''' SELECT 1
                    FROM csv_file_history
                    WHERE source_url = ? AND sha256 = ?
                    LIMIT 1
                ''',
                (source_url, sha256),
            ).fetchone()
        return row is not None

    def _latest_hash_for_url(self, source_url: str):
        """ Return latest hash for a source URL, or None when no history exists. """
        with sqlite3.connect(self.db_path) as connection:
            row = connection.execute(
                ''' SELECT sha256
                    FROM csv_file_history
                    WHERE source_url = ?
                    ORDER BY id DESC
                    LIMIT 1
                ''',
                (source_url,),
            ).fetchone()
        return row[0] if row else None
