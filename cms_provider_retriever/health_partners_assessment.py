# standard library imports
import hashlib
import json
import re
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

# non-standard library imports
from pandas import json_normalize, read_csv, set_option
from requests import Session

# local imports
from csv_hash_tracker import CSVDownloadHashTracker

# exception imports
from requests.exceptions import RequestException


class CMSProviderDataRetriever(CSVDownloadHashTracker):

    PROVIDER_DATA_URL = 'https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items'
    BASE_DIR = Path(__file__).resolve().parent

    def __init__(self, hash_db_path=None):
        if hash_db_path is None:
            hash_db_path = self.BASE_DIR / 'download_hashes.db'
        super().__init__(hash_db_path)
        self.session = Session()
        self.sha256 = None
        self.response = None
        self.response_size = 0

    def _column_mapper(self, columns):
        """Normalize mixed headers into clean snake_case."""
        if not isinstance(columns, list):
            raise TypeError('Input must be a list of strings.')
        new_columns = {}
        for column in columns:
            name = str(column)
            name = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', '_', name)
            name = re.sub(r'(?<=[A-Z])(?=[A-Z][a-z])', '_', name)
            name = re.sub(r'[^A-Za-z0-9]+', '_', name)
            name = re.sub(r'_+', '_', name).strip('_').lower()
            new_columns[column] = name
        return new_columns

    def _dated_output_path(self, output_path):
        """Append UTC date suffix to the filename."""
        target = Path(output_path)
        date_suffix = datetime.now(timezone.utc).strftime('%Y%m%d')
        extension = target.suffix if target.suffix else '.csv'
        return target.with_name(f'{target.stem}_{date_suffix}{extension}')

    def _resolve_tracked_path(self, tracked_path):
        """Resolve relative tracked paths predictably across working directories."""
        candidate = Path(tracked_path)
        if candidate.is_absolute():
            return candidate
        base_candidate = (self.BASE_DIR / candidate).resolve()
        if base_candidate.exists():
            return base_candidate
        return candidate.resolve()

    def _is_valid_written_csv(self, csv_path):
        """Treat files with no data rows as invalid cached outputs."""
        if not csv_path.exists() or csv_path.stat().st_size == 0:
            return False
        try:
            sample = read_csv(csv_path, nrows=1)
        except Exception:
            return False
        return (len(sample.columns) > 0) and (not sample.empty)

    def _parse_downloaded_payload(self, stream_buffer):
        """Parse JSON payload from CMS metadata endpoint."""
        payload_bytes = stream_buffer.getvalue()
        try:
            payload = json.loads(payload_bytes.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None

        if isinstance(payload, list):
            return json_normalize(payload)
        if isinstance(payload, dict):
            if isinstance(payload.get('data'), list):
                return json_normalize(payload['data'])
            return json_normalize([payload])
        return None

    def stream_data(self):
        """Stream response content while updating SHA-256 state."""
        if self.response is None:
            raise TypeError('No response available to stream.')
        self.sha256 = hashlib.sha256()
        header_length = self.response.headers.get('Content-Length')
        self.response_size = int(header_length) if header_length else 0
        for chunk in self.response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                self.sha256.update(chunk)
                if not header_length:
                    self.response_size += len(chunk)
                yield chunk

    def download_csv_and_track(self, output_path):
        """Download streamed CSV, normalize/map columns, write dated file, and track hash."""
        output_file = self._dated_output_path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        stream_buffer = BytesIO()
        try:
            with self.session.get(
                url=self.PROVIDER_DATA_URL,
                stream=True,
                timeout=120,
            ) as response:
                # set class attribute response for use in stream_data and
                # post-download processing so that only the responses
                # created in this class are able to be used.
                self.response = response
                self.response.raise_for_status()
                for chunk in self.stream_data():
                    stream_buffer.write(chunk)
        except RequestException as error:
            print(f'An error occurred while downloading CSV data: {error}')
            return
        finally:
            self.response = None
        sha256_hex = self.sha256.hexdigest()
        download_date = datetime.now(timezone.utc).date().isoformat()
        has_existing_hash = self._check_for_existing_hash(
            source_url=self.PROVIDER_DATA_URL,
            sha256=sha256_hex,
        )
        if has_existing_hash:
            existing_local_path = self._latest_local_path_for_hash(
                source_url=self.PROVIDER_DATA_URL,
                sha256=sha256_hex,
            )
            if existing_local_path:
                existing_path = self._resolve_tracked_path(existing_local_path)
                if self._is_valid_written_csv(existing_path):
                    print(f'File with hash {sha256_hex} already tracked for this URL.')
                    return {
                        'source_url': self.PROVIDER_DATA_URL,
                        'local_path': str(existing_path),
                        'sha256': sha256_hex,
                        'file_size_bytes': self.response_size,
                        'download_date': download_date,
                        'skipped': True,
                    }
                output_file = existing_path
                output_file.parent.mkdir(parents=True, exist_ok=True)
        normalized = self._parse_downloaded_payload(stream_buffer)
        if normalized is None or normalized.empty:
            print('No JSON rows parsed from downloaded payload; skipping file write.')
            return None
        normalized.rename(columns=self._column_mapper(list(normalized.columns)), inplace=True)
        normalized.to_csv(path_or_buf=output_file, index=False)
        if not has_existing_hash:
            self._record_download(
                source_url=self.PROVIDER_DATA_URL,
                local_path=str(output_file.resolve()),
                file_size_bytes=self.response_size,
                sha256=sha256_hex,
            )
        return {
            'source_url': self.PROVIDER_DATA_URL,
            'local_path': str(output_file.resolve()),
            'sha256': sha256_hex,
            'file_size_bytes': self.response_size,
            'download_date': download_date,
            'skipped': False,
        }

    def main(self):
        """Main method to download and track the provider CSV."""
        try:
            output_path = self.BASE_DIR / 'output' / 'provider_data.csv'
            result = self.download_csv_and_track(output_path=output_path)
            print(result)
            return result
        finally:
            self.session.close()


if __name__ == '__main__':
    set_option('display.max_columns', 25)
    retriever = CMSProviderDataRetriever()
    retriever.main()
