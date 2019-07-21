import logging
import argparse
import unittest as ut
from unittest.mock import patch, MagicMock

from data_downloader.main import main, parse, get_log_level
from data_downloader.downloader import HttpFileDownloader


class MainTest(ut.TestCase):

    def test_parse(self):
        command = ['python', '--url', 'http://abc.com/test.jpg', '--url', 'http://def.com/def.jpg',
                   '--output', 'output_dir', '--chunk_size', '1234', '--timeout', '20', '--threads', '8']
        with patch('argparse._sys.argv', command):
            actual = parse()
            self.assertEqual(actual.url, [['http://abc.com/test.jpg'], ['http://def.com/def.jpg']])
            self.assertEqual(actual.chunk_size, 1234)
            self.assertEqual(actual.output, 'output_dir')
            self.assertEqual(actual.timeout, 20)
            self.assertEqual(actual.ftp_username, '')
            self.assertEqual(actual.ftp_password, '')
            self.assertEqual(actual.threads, 8)

    def test_get_log_level(self):
        param_list = [('info', logging.INFO), ('WarNing', logging.WARNING), ('DEBUG', logging.DEBUG),
                      ('error', logging.ERROR), ('xxx', logging.INFO)]
        for param, exp in param_list:
            with self.subTest():
                actual = get_log_level(param)
                self.assertEqual(actual, exp)

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

    @patch('os.path.exists')
    @patch('data_downloader.main._logger')
    @patch('os.makedirs')
    @patch('data_downloader.downloader.get_downloader')
    def test_main_failure(self, get_downloader, makedirs, _logger, exists):
        def mock_download():
            raise Exception("Network error!")

        url = 'http://files.fast.ai/data/ml-latest-small.zip'
        output_dir = 'output_dir'

        exists.return_value = True
        http_downloader = HttpFileDownloader(url, output_dir, chunk_size=8192, timeout=60)
        http_downloader.download = mock_download
        http_downloader.delete = MagicMock()
        get_downloader.return_value = http_downloader
        parsed = argparse.Namespace(url=[[url]], ftp_username='',
                                    ftp_password='', chunk_size=8192, threads=2, timeout=10,
                                    output=output_dir)

        main(parsed)

        _logger.error.assert_called_once_with("failed to download file from {}, {}.".format(url, "Network error!"))
        http_downloader.delete.assert_called_once()


if __name__ == '__main__':
    ut.main()