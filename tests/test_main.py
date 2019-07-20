import argparse
import unittest as ut
from unittest.mock import patch, MagicMock

from data_downloader.main import main
from data_downloader.downloader import HttpFileDownloader


class MainTest(ut.TestCase):

    @patch('os.makedirs')
    @patch('data_downloader.downloader.get_downloader')
    def test_main(self, get_downloader, makedirs):

        url = 'http://files.fast.ai/data/ml-latest-small.zip'
        output_dir = 'output_dir'

        http_downloader = HttpFileDownloader(url, output_dir, chunk_size=8192, timeout=60)
        http_downloader.download = lambda: None
        get_downloader.return_value = http_downloader
        parsed = argparse.Namespace(url=[[url]], ftp_username='', ftp_password='', chunk_size=8192, threads=2,
                                    timeout=10, output=output_dir)

        main(parsed)

    @patch('os.remove')
    @patch('os.path.exists')
    @patch('data_downloader.main._logger')
    @patch('os.makedirs')
    @patch('data_downloader.downloader.get_downloader')
    def test_main_failure(self, get_downloader, makedirs, _logger, exists, remove):
        def mock_download():
            raise Exception("Network error!")

        url = 'http://files.fast.ai/data/ml-latest-small.zip'
        output_dir = 'output_dir'

        exists.return_value = True
        http_downloader = HttpFileDownloader(url, output_dir, chunk_size=8192, timeout=60)
        http_downloader.download = mock_download
        get_downloader.return_value = http_downloader
        parsed = argparse.Namespace(url=[[url]], ftp_username='',
                                    ftp_password='', chunk_size=8192, threads=2, timeout=10,
                                    output=output_dir)

        main(parsed)

        _logger.error.assert_called_once_with("failed to download file from {}, {}.".format(url, "Network error!"))
        remove.assert_called_once_with(http_downloader.output_file)
        _logger.info.assert_called_once_with("file {} deleted.".format(http_downloader.output_file))


if __name__ == '__main__':
    ut.main()