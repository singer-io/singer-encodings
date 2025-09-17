import gzip
from io import BytesIO
import zipfile
import logging

LOGGER = logging.getLogger()

def infer(iterable, file_name, conn = None):
    """Uses the incoming file_name and checks the end of the string
    for supported compression types"""
    if not file_name:
        raise Exception("Need file name")

    if file_name.endswith('.tar.gz'):
        raise NotImplementedError("tar.gz not supported")
    elif file_name.endswith('.gz'):
        yield gzip.GzipFile(fileobj=iterable)
    elif file_name.endswith('.zip'):
        try:
            with zipfile.ZipFile(iterable) as zip:
                for name in zip.namelist():
                    yield zip.open(name)
        except zipfile.BadZipFile:
            iterable = conn.get_file_handle({'filepath': file_name})
            LOGGER.info(f'Failed to extract the ZIP file {file_name}, attempting to load the entire file(size - {iterable.stat().st_size/(1024 * 1024)} MB) into memory.')

            zip_bytes = iterable.read()
            with zipfile.ZipFile(BytesIO(zip_bytes)) as zip:
                for name in zip.namelist():
                    yield zip.open(name)
    else:
        yield iterable
