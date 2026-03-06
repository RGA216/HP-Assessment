# standard library imports
import hashlib
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

    def __init__(self, hash_db_path='download_hashes.db'):
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
                self.response = response
                self.response.raise_for_status()
                for chunk in self.stream_data():
                    stream_buffer.write(chunk)
        except RequestException as error:
            print(f'An error occurred while downloading CSV data: {error}')
            return None
        finally:
            self.response = None
        sha256_hex = self.sha256.hexdigest()
        download_date = datetime.now(timezone.utc).date().isoformat()
        if self._check_for_existing_hash(
            source_url=self.PROVIDER_DATA_URL,
            sha256=sha256_hex,
        ):
            print(f'File with hash {sha256_hex} already tracked for this URL.')
            existing_local_path = self._latest_local_path_for_hash(
                source_url=self.PROVIDER_DATA_URL,
                sha256=sha256_hex,
            )
            return {
                'source_url': self.PROVIDER_DATA_URL,
                'local_path': existing_local_path or str(output_file),
                'sha256': sha256_hex,
                'file_size_bytes': self.response_size,
                'download_date': download_date,
                'skipped': True,
            }
        stream_buffer.seek(0)
        normalized = json_normalize(read_csv(stream_buffer).to_dict(orient='records'))
        normalized.rename(columns=self._column_mapper(list(normalized.columns)), inplace=True)
        normalized.to_csv(path_or_buf=output_file, index=False)
        self._record_download(
            source_url=self.PROVIDER_DATA_URL,
            local_path=str(output_file),
            file_size_bytes=self.response_size,
            sha256=sha256_hex,
        )
        return {
            'source_url': self.PROVIDER_DATA_URL,
            'local_path': str(output_file),
            'sha256': sha256_hex,
            'file_size_bytes': self.response_size,
            'download_date': download_date,
            'skipped': False,
        }

    def main(self):
        """Main method to download and track the provider CSV."""
        try:
            result = self.download_csv_and_track(output_path='output/provider_data.csv')
            print(result)
            return result
        finally:
            self.session.close()


if __name__ == '__main__':
    set_option('display.max_columns', 25)
    retriever = CMSProviderDataRetriever()
    retriever.main()
