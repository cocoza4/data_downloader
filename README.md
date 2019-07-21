# data_downloader
A program used to download data from multiple sources and protocols 
(currently supported HTTP and FTP) to local disk. It is designed to be extensible to support
more protocols, scalable even when memory is limited and partially downloaded files are deleted.

## Installation
For development and testing,
```bash
pip install -r requirements-dev.txt
```
For production,
```bash
pip install -r requirements.txt
```

## Run tests
All tests
```bash
nosetests
```
Unit tests
```bash
nosetests tests/unit
```
Integration tests
```bash
nosetests tests/integration
```
Note actual files are downloaded into the path defined in environment variable `OUTPUT_DIR` 
(default to `/tmp/file_output`) and removed after the tests run.

## Run
Setup
```bash
cd <project_path>
PYTHONPATH=$PYTHONPATH:.
```
Run
```bash
python data_downloader/main.py --url <url1> --url <url2> --output <output_dir>
```
For help
```bash
python data_downloader/main.py -h
```