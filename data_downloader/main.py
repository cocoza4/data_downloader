import os
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from data_downloader import downloader

_logger = logging.getLogger(__name__)


def main(args):
    os.makedirs(args.output, exist_ok=True)

    with ThreadPoolExecutor(args.threads) as executor:
        futures = dict()
        for url in args.url:
            file_downloader = downloader.get_downloader(url[0], args.output, args.chunk_size, args.timeout,
                                                        ftp_username=args.ftp_username, ftp_password=args.ftp_password)
            future = executor.submit(file_downloader.download)
            futures[future] = file_downloader

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                # this is to ensure to delete partially downloaded file.
                file_downloader = futures[future]
                _logger.error("failed to download file from {}, {}.".format(file_downloader.url, e))
                file_downloader.delete()


def get_log_level(log_level: str):
    """returns logging level.

    Parameters
    ----------
    log_level : str
        string of log level

    Returns
    -------
    logging level.
    """
    if log_level.upper() == 'INFO':
        return logging.INFO
    elif log_level.upper() == 'WARNING':
        return logging.WARNING
    elif log_level.upper() == 'DEBUG':
        return logging.DEBUG
    elif log_level.upper() == 'ERROR':
        return logging.ERROR
    else:
        return logging.INFO


def parse():
    """parse user input arguments"""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--url', help='Comma-separated url to download a file.',
                        action='append', nargs='+', required=True),
    parser.add_argument('--ftp_username', help='FTP username. Ignore if no FTP protocol is requested', default='')
    parser.add_argument('--ftp_password', help='FTP username. Ignore if no FTP protocol is requested', default='')
    parser.add_argument('--chunk_size', help='File chunk size, default to 8192.', type=int, default=8192)
    parser.add_argument('--threads', help='Number of worker threads.', type=int, default=4)
    parser.add_argument('--timeout', help='Timeout in seconds, defaul tto 30 seconds.', type=int, default=30)
    parser.add_argument('--output', help='Output path.', required=True)
    parser.add_argument('--log_level', help='Log level, default=INFO', default='INFO')

    return parser.parse_args()


if __name__ == "__main__":
    args = parse()
    logging.basicConfig(level=get_log_level(args.log_level))
    main(args)
