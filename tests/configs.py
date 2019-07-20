import os

# default output directory for storing downloaded files. This is only used in integration test.
OUTPUT_DIR = os.getenv('OUTPUT_DIR', "/tmp/file_output")

TEST_URLS = ['http://files.fast.ai/data/ml-latest-small.zip',
             'ftp://ftp.ensemblgenomes.org/pub/plants/release-44/summary.txt']