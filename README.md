# HP Assessment - CMS CSV Downloader

This project downloads a CMS CSV dataset as a stream, normalizes column names to `snake_case`, writes a date-stamped output file, and tracks file hashes in SQLite to avoid duplicate history entries.

## Files

- `health_partners_assessment.py`: Main script that downloads and processes the CSV.
- `csv_hash_tracker.py`: SQLite hash tracking utility (`csv_file_history` table).
- `requirements.txt`: Python dependencies.

## Requirements

- Python 3.9+ (3.14 used to design the project)

Install dependencies:

Linux:
```bash
pip install -r requirements.txt
python ./cms_provider_retriever/health_partners_assessment.py
```

Windows:
```cmd
pip install -r requirements.txt
python .\cms_provider_retriever\health_partners_assessment.py
```


## Output

- CSV written to `output/provider_data_YYYYMMDD.csv`
- SQLite database `download_hashes.db` with table `csv_file_history`:
  - `source_url`
  - `local_path`
  - `sha256`
  - `file_size_bytes`
  - `download_date`

## Behavior

- Response is downloaded using HTTP streaming.
- SHA-256 is computed while streaming for duplicatation check.
- Data is loaded into pandas, normalized with `json_normalize`, and columns are converted to clean `snake_case`.
- If the same `source_url` + `sha256` already exists, the run is marked as skipped.

