import os
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from data_downloader import downloader

logging.basicConfig(level=logging.INFO)
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
                if file_downloader.file and os.path.exists(file_downloader.output_file):
                    os.remove(file_downloader.output_file)
                    _logger.info("file {} deleted.".format(file_downloader.output_file))


def parse():
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

    return parser.parse_args()


if __name__ == "__main__":
    args = parse()
    main(args)
