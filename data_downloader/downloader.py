import logging
import requests

from pathlib import Path


_logger = logging.getLogger(__name__)


class FileDownloader:

    def __init__(self, url, path):
        self.url = url
        self.path = path

    def save(self, filename):
        pass

    def download(self):
        raise NotImplementedError


class HttpFileDownloader(FileDownloader):

    def __init__(self, url, path):
        super().__init__(url, path)

    def download(self):
        local_filename = Path(self.path) / self.url.split('/')[-1]
        _logger.info(f"downloading file from {self.url} and saving into {local_filename}")
        with requests.get(self.url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        f.flush()


class FtpFileDownloader(FileDownloader):

    def __init__(self, url, path):
        super().__init__(url, path)


