import os
import shutil
import argparse
import unittest as ut
from pathlib import Path
from unittest.mock import patch

from data_downloader.main import main
from data_downloader.downloader import get_downloader, HttpFileDownloader
from .. import configs as cfg


class MainTest(ut.TestCase):

    def setUp(self):
        os.makedirs(cfg.OUTPUT_DIR, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(cfg.OUTPUT_DIR)

    def test_main(self):
        urls = list(map(lambda x: [x], cfg.TEST_URLS))
        parsed = argparse.Namespace(url=urls, ftp_username='',
                                    ftp_password='', chunk_size=8192, threads=2, timeout=10,
                                    output=cfg.OUTPUT_DIR)

        main(parsed)

        for url in cfg.TEST_URLS:
            file = url.split('/')[-1]
            path = Path(cfg.OUTPUT_DIR) / file
            self.assertTrue(path.exists())

    @patch('data_downloader.main.downloader')
    def test_main_file_not_successfully_downloaded(self, mock_downloader):
        # this test case mocks the behavior of a partially downloaded file (might be due to network error or other other
        # issue) by throwing an exception after successfully loading the file. The file should be removed afterwards.

        def mock_get_downloader(url, output_dir, chunk_size, timeout, **kwargs):
            downloader = get_downloader(url, output_dir, chunk_size, timeout, **kwargs)

            def mock_download():
                downloader.download()
                raise Exception("Mock Exception")

            if isinstance(downloader, HttpFileDownloader):
                downloader.download = mock_download

            return downloader

        mock_downloader.get_downloader = mock_get_downloader

        urls = list(map(lambda x: [x], cfg.TEST_URLS))
        parsed = argparse.Namespace(url=urls, ftp_username='',
                                    ftp_password='', chunk_size=8192, threads=2, timeout=10,
                                    output=cfg.OUTPUT_DIR)

        main(parsed)

        for url in cfg.TEST_URLS:
            file = url.split('/')[-1]
            path = Path(cfg.OUTPUT_DIR) / file
            if url.startswith('http'):
                self.assertFalse(path.exists())
            else:
                self.assertTrue(path.exists())


if __name__ == '__main__':
    ut.main()