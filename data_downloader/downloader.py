import logging
import requests
import ftplib

from pathlib import Path
from urllib.parse import urlparse


_logger = logging.getLogger(__name__)


class FileDownloader:
    """Base class for various types of file downloader to implement.

    Attributes
    ----------
    url : str
        url of the file to download.

    output_file : str
        output file.

    chunk_size : int
        maximum chunk_size of file to read at a time.

    timeout : int
        request timeout.

    host : str
        host url

    file : str
        file name

    path : str
        path to the file based on the url. For example, the path of
        `ftp://ftp.ensemblgenomes.org/pub/plants/release-44/summary.txt` should be `/pub/plants/release-44`
    """

    def __init__(self, url: str, output_dir: str, chunk_size: int=8192, timeout: int=30):
        self.url = url
        self.chunk_size = chunk_size
        self.timeout = timeout
        self._parsed = urlparse(url)
        self.output_file = str(Path(output_dir) / self.file)

    @property
    def host(self) -> str:
        return self._parsed.netloc

    @property
    def file(self) -> str:
        return self._parsed.path.split('/')[-1]

    @property
    def path(self) -> str:
        return '/'.join(self._parsed.path.split('/')[:-1])

    def download(self) -> None:
        raise NotImplementedError


class HttpFileDownloader(FileDownloader):
    """A class for HTTP-protocol download.

    Attributes
    ----------
    url : str
        url of the file to download.

    output_file : str
        output file.

    chunk_size : int
        maximum chunk_size of file to read at a time.

    timeout : int
        request timeout.

    host : str
        host url.

    file : str
        file name.

    path : str
        path to the file based on the url. For example, the path of
        `ftp://ftp.ensemblgenomes.org/pub/plants/release-44/summary.txt` should be `/pub/plants/release-44`.
    """

    def __init__(self, url: str, output_dir: str, chunk_size: int, timeout: int):
        super().__init__(url, output_dir, chunk_size, timeout)

    def download(self) -> None:
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
    """A class for FTP-protocol download.

    Attributes
    ----------
    url : str
        url of the file to download.

    output_file : str
        output file.

    chunk_size : int
        maximum chunk_size of file to read at a time.

    timeout : int
        request timeout.

    host : str
        host url.

    file : str
        file name.

    path : str
        path to the file based on the url. For example, the path of
        `ftp://ftp.ensemblgenomes.org/pub/plants/release-44/summary.txt` should be `/pub/plants/release-44`.

    username : str
        ftp username.

    password : str
        ftp password.
    """

    def __init__(self, url: str, output_dir: str, chunk_size: int, timeout: int, username: str, password: str):
        super().__init__(url, output_dir, chunk_size, timeout)
        self.username = username
        self.password = password

    def download(self) -> None:
        _logger.info(f"downloading file from {self.url} and saving into {self.output_file}.")
        with ftplib.FTP(self.host, timeout=self.timeout) as ftp:
            ftp.login(self.username, self.password)
            ftp.cwd(self.path)
            with open(self.output_file, 'wb') as f:
                ftp.retrbinary(f'RETR {self.file}', f.write, blocksize=self.chunk_size)
        _logger.info(f"successfully downloaded file {self.url}. {self.output_file} saved.")


def get_downloader(url: str, output_dir: str, chunk_size: int, timeout: int, **kwargs):
    """returns an instance of either HttpFileDownloader and FtpFileDownloader given an input url.

    Parameters
    ----------
    url : url of the file to download.

    output_dir : output directory where all successfully downloaded files will be stored.

    chunk_size : maximum chunk_size of file to read at a time.

    timeout : request timeout.

    Returns
    -------
    an instance of either HttpFileDownloader and FtpFileDownloader

    Raises
    ------
    ValueError: raised if the protocol is not supported.
    """
    parsed = urlparse(url)
    protocol = parsed.scheme
    if protocol == "http":
        return HttpFileDownloader(url, output_dir, chunk_size=chunk_size, timeout=timeout)
    elif protocol == "ftp":
        return FtpFileDownloader(url, output_dir, chunk_size=chunk_size, timeout=timeout,
                                 username=kwargs['ftp_username'], password=kwargs['ftp_password'])
    else:
        raise ValueError(f"no protocol supported for {url}")
