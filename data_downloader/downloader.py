import logging
import requests
import ftplib

from pathlib import Path
from urllib.parse import urlparse


_logger = logging.getLogger(__name__)


class FileDownloader:

    def __init__(self, url, output_dir, chunk_size=8192, timeout=30):
        self.url = url
        self.chunk_size = chunk_size
        self.timeout = timeout
        self._parsed = urlparse(url)
        self.output_file = str(Path(output_dir) / self.file)

    @property
    def host(self):
        return self._parsed.netloc

    @property
    def file(self):
        return self._parsed.path.split('/')[-1]

    @property
    def path(self):
        return '/'.join(self._parsed.path.split('/')[:-1])

    def download(self):
        raise NotImplementedError


class HttpFileDownloader(FileDownloader):

    def __init__(self, url, output_dir, chunk_size, timeout):
        super().__init__(url, output_dir, chunk_size, timeout)

    def download(self):
        _logger.info(f"downloading file from {self.url} and saving into {self.output_file}.")
        with requests.get(self.url, timeout=self.timeout, stream=True) as r:
            r.raise_for_status()
            with open(self.output_file, 'wb') as f:
                for chunk in r.iter_content(chunk_size=self.chunk_size):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        f.flush()

        _logger.info(f"successfully downloaded file {self.url}. {self.output_file} saved.")


class FtpFileDownloader(FileDownloader):

    def __init__(self, url, output_dir, chunk_size, timeout, username, password):
        super().__init__(url, output_dir, chunk_size, timeout)
        self.username = username
        self.password = password

    def download(self):
        _logger.info(f"downloading file from {self.url} and saving into {self.output_file}.")
        with ftplib.FTP(self.host, timeout=self.timeout) as ftp:
            ftp.login(self.username, self.password)
            ftp.cwd(self.path)
            with open(self.output_file, 'wb') as f:
                ftp.retrbinary(f'RETR {self.file}', f.write, blocksize=self.chunk_size)
        _logger.info(f"successfully downloaded file {self.url}. {self.output_file} saved.")


def get_downloader(url, output_dir, chunk_size, timeout, **kwargs):
    parsed = urlparse(url)
    protocol = parsed.scheme
    if protocol == "http":
        return HttpFileDownloader(url, output_dir, chunk_size=chunk_size, timeout=timeout)
    elif protocol == "ftp":
        return FtpFileDownloader(url, output_dir, chunk_size=chunk_size, timeout=timeout,
                                 username=kwargs['ftp_username'], password=kwargs['ftp_password'])
    else:
        raise ValueError(f"no protocol supported for {url}")


