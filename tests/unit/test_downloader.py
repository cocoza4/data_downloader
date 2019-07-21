import unittest as ut
from unittest.mock import call, patch, mock_open, MagicMock
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

    @patch('data_downloader.downloader._logger.info')
    @patch('requests.get')
    def test_http_file_downloader_download(self, get, info):

        m = mock_open()
        mock_request = MagicMock()
        mock_request.iter_content.return_value = ['xxx']
        get.return_value.__enter__.return_value = mock_request

        url = 'http://files.fast.ai/data/cifar10.tgz'
        output_dir = 'output_dir'
        downloader = get_downloader(url, output_dir, chunk_size=8192, timeout=60)

        with patch('data_downloader.downloader.open', m):
            downloader.download()

        info.assert_has_calls([call(f"downloading file from {url} and saving into {downloader.output_file}."),
                               call(f"successfully downloaded file {url}. {downloader.output_file} saved.")])
        m.assert_called_once_with(downloader.output_file, 'wb')
        handle = m()
        handle.write.assert_called_once_with('xxx')
        get.assert_called_once_with(url, timeout=60, stream=True)
        mock_request.raise_for_status.assert_called_once()
        mock_request.iter_content.assert_called_once_with(chunk_size=8192)

    @patch('data_downloader.downloader._logger.info')
    @patch('ftplib.FTP')
    def test_ftp_file_downloader_download(self, FTP, info):
        m = mock_open()
        mock_ftp = MagicMock()
        FTP.return_value.__enter__.return_value = mock_ftp

        url = 'ftp://files.fast.ai/data/cifar10.tgz'
        output_dir = 'output_dir'
        downloader = get_downloader(url, output_dir, chunk_size=8192, timeout=60, ftp_username='xxx', ftp_password='yyy')

        with patch('data_downloader.downloader.open', m):
            downloader.download()

        info.assert_has_calls([call(f"downloading file from {url} and saving into {downloader.output_file}."),
                               call(f"successfully downloaded file {url}. {downloader.output_file} saved.")])

        m.assert_called_once_with(downloader.output_file, 'wb')
        handle = m()
        FTP.assert_called_once_with('files.fast.ai', timeout=60)
        mock_ftp.login.assert_called_once_with('xxx', 'yyy')
        mock_ftp.cwd.assert_called_once_with(downloader.path)
        mock_ftp.retrbinary.assert_called_once_with(f'RETR {downloader.file}', handle.write, blocksize=8192)


if __name__ == '__main__':
    ut.main()
