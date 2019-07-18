import logging
import requests
import ftplib

from pathlib import Path


_logger = logging.getLogger(__name__)


class FileDownloader:

    def __init__(self, url):
        self.url = url

    def download(self, path):
        raise NotImplementedError


class HttpFileDownloader(FileDownloader):

    def __init__(self, url):
        super().__init__(url)

    def download(self, path):
        filename = Path(path) / self.url.split('/')[-1]
        _logger.info(f"downloading file from {self.url} and saving into {filename}")
        with requests.get(self.url, stream=True) as r:
            r.raise_for_status()
            with open(filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        f.flush()

        _logger.info(f"successfully downloaded file {self.url}. {filename} saved")


class FtpFileDownloader(FileDownloader):

    def __init__(self, url, host, username, password):
        super().__init__(url)
        self.host = host
        self.username = username
        self.password = password

    def download(self, path):
        #TODO: refactor and check error for this case
        splits = self.url.split('/')
        ftp_filename = splits[-1]
        local_filename =  str(Path(path) / ftp_filename)
        print('filename', local_filename)
        filename_path = '/'.join(splits[:-1])
        with ftplib.FTP(self.host) as ftp:
            ftp.login(self.username, self.password)
            data = []
            ftp.dir(data.append)
            print(data)
            ftp.cwd('/pub/plants/release-44')
            with open(local_filename, 'wb') as f:
                ftp.retrbinary(f'RETR {ftp_filename}', f.write, blocksize=8192)


def get_downloader(url, *args, **kwargs):
    if url.startswith("http"):
        return HttpFileDownloader(url)
    elif url.startswith("ftp"):
        return FtpFileDownloader(url, 'ftp.ensemblgenomes.org', username=kwargs['ftp_username'], password=kwargs['ftp_password'])
        # return FtpFileDownloader(url, 'ftp.ensemblgenomes.org', username=None, password=None)
    else:
        raise ValueError(f"no protocal supported for {url}")


