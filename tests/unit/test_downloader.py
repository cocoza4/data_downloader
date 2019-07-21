import unittest as ut
from unittest.mock import patch
from data_downloader.downloader import get_downloader, HttpFileDownloader, FtpFileDownloader


class DownloaderTest(ut.TestCase):

    @patch('os.remove')
    @patch('os.path.exists')
    @patch('data_downloader.downloader._logger')
    def test_ftp_downloader_delete(self, _logger, exists, remove):
        exists.return_value = True
        url = 'http://files.fast.ai/data/cifar10.tgz'
        output_dir = 'output_dir'
        downloader = get_downloader(url, output_dir, chunk_size=8192, timeout=60)
        downloader.delete()
        remove.assert_called_once_with(downloader.output_file)

    def test_get_downloader_http(self):
        url = 'http://files.fast.ai/data/cifar10.tgz'
        output_dir = 'output_dir'
        actual = get_downloader(url, output_dir, chunk_size=8192, timeout=60)
        self.assertTrue(isinstance(actual, HttpFileDownloader))
        self.assertEqual(actual.url, url)
        self.assertEqual(actual.chunk_size, 8192)
        self.assertEqual(actual.timeout, 60)
        self.assertEqual(actual.output_file, 'output_dir/cifar10.tgz')
        self.assertEqual(actual.host, 'files.fast.ai')
        self.assertEqual(actual.file, 'cifar10.tgz')
        self.assertEqual(actual.path, '/data')

    def test_get_downloader_ftp(self):
        url = 'ftp://ftp.ensemblgenomes.org/pub/plants/release-44/summary.txt'
        output_dir = 'output_dir'
        kwargs = {'ftp_username': 'xxx', 'ftp_password': 'yyy'}
        actual = get_downloader(url, output_dir, 8192, 60, **kwargs)
        self.assertTrue(isinstance(actual, FtpFileDownloader))
        self.assertEqual(actual.url, url)
        self.assertEqual(actual.chunk_size, 8192)
        self.assertEqual(actual.timeout, 60)
        self.assertEqual(actual.output_file, 'output_dir/summary.txt')
        self.assertEqual(actual.host, 'ftp.ensemblgenomes.org')
        self.assertEqual(actual.file, 'summary.txt')
        self.assertEqual(actual.path, '/pub/plants/release-44')
        self.assertEqual(actual.username, 'xxx')
        self.assertEqual(actual.password, 'yyy')

    def test_get_downloader_not_supported_protocol(self):
        url = 'sftp://ftp.ensemblgenomes.org/pub/plants/release-44/summary.txt'
        output_dir = 'output_dir'
        kwargs = {'ftp_username': 'xxx', 'ftp_password': 'yyy'}
        with self.assertRaisesRegex(ValueError, f"no protocol supported for {url}"):
            get_downloader(url, output_dir, 8192, 60, **kwargs)

    def test_get_downloader_no_protocol_defined(self):
        url = 'files.fast.ai/data/cifar10.tgz'
        output_dir = 'output_dir'
        kwargs = {'ftp_username': 'xxx', 'ftp_password': 'yyy'}
        with self.assertRaisesRegex(ValueError, f"no protocol supported for {url}"):
            get_downloader(url, output_dir, 8192, 60, **kwargs)


if __name__ == '__main__':
    ut.main()
