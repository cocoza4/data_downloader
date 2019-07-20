import os
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from data_downloader.downloader import get_downloader

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)


def main(args):
    os.makedirs(args.output, exist_ok=True)

    with ThreadPoolExecutor(args.threads) as executor:
        futures = dict()
        for url in args.url:
            downloader = get_downloader(url[0], args.output, args.chunk_size, args.timeout, ftp_username=args.ftp_username,
                                        ftp_password=args.ftp_password)
            future = executor.submit(downloader.download)
            futures[future] = downloader

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                # this is to ensure to delete partially downloaded file.
                downloader = futures[future]
                _logger.error("failed to download file from {}, {}.".format(downloader.url, e))
                if downloader.file and os.path.exists(downloader.output_file):
                    os.remove(downloader.output_file)
                    _logger.info("file {} deleted.".format(downloader.output_file))


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

    # import time
    # def run(num):
    #     return num
    #
    #
    # from concurrent.futures import ThreadPoolExecutor, as_completed
    # with ThreadPoolExecutor(4) as executor:
    #     futures = [executor.submit(run, x) for x in range(130)]
    #     for future in as_completed(futures):
    #         try:
    #             print(future.result())
    #         except ValueError as e:
    #             print(e)
    # with ThreadPool(4) as pool:
    #     # results = pool.map(run, [x for x in list(range(120))])
    #     results = [pool.apply_async(run, [x]) for x in list(range(30))]
    #     for async_result in results:
    #         try:
    #             print(async_result.get())
    #         except ValueError as e:
    #             print(e)