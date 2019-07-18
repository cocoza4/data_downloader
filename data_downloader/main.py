import os
import logging
import argparse
from multiprocessing.dummy import Pool as ThreadPool
from concurrent.futures import ThreadPoolExecutor, as_completed

from data_downloader.downloader import get_downloader

logging.basicConfig(level=logging.INFO)


def execute(url, path, *args, **kwargs):
    downloader = get_downloader(url, *args, **kwargs)
    print(downloader)
    downloader.download(path)


def main(args):
    print(args)
    os.makedirs(args.output, exist_ok=True)

    with ThreadPoolExecutor(args.threads) as executor:

        futures = [executor.submit(execute, url[0], args.output,
                                   ftp_username=args.ftp_username, ftp_password=args.ftp_password) for url in args.url]

        # for url in args.url:
        #     print(url)
        #     executor.submit(execute, url[0], args.output, ftp_username=args.ftp_username, ftp_password=args.ftp_password)

        for future in as_completed(futures):
            print('xxx', future)
            try:
                print(future.result())
            except ValueError as e:
                print(e)


def parse():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--url', help='Comma-separated url to download a file.',
                        action='append', nargs='+', required=True),
    parser.add_argument('--ftp_username', help='FTP username. Ignore if no FTP protocal is requested', default='')
    parser.add_argument('--ftp_password', help='FTP username. Ignore if no FTP protocal is requested', default='')
    parser.add_argument('--chunk_size', help='File chunk size, default to 8192.', type=int, default=8192)
    parser.add_argument('--threads', help='Number of worker threads.', type=int, default=4)
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