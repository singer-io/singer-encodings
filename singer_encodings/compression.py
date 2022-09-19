import gzip
import io
import zipfile
import singer
from singer_encodings import gzip_utils

LOGGER = singer.get_logger()

def infer(iterable, file_name):
    """Uses the incoming file_name and checks the end of the string for supported compression types"""
    if not file_name:
        raise Exception("Need file name")

    if file_name.endswith('.tar.gz'):
        LOGGER.warning('Skipping "%s" file as .tar.gz extension is not supported', file_name)
        yield from []
    elif file_name.endswith('.gz'):
        file_bytes = iterable.read()
        gz_file_name = None
        try:
            gz_file_name = gzip_utils.get_file_name_from_gzfile(fileobj=io.BytesIO(file_bytes))
        except AttributeError:
            # If a file is compressed using gzip command with --no-name attribute,
            # It will not return the file name and timestamp. Hence we will skip such files.
            LOGGER.warning('Skipping "%s" file as we did not get the original file name.', file_name)
        # Send file object and file name
        yield [gzip.GzipFile(fileobj=io.BytesIO(file_bytes)), gz_file_name]
    elif file_name.endswith('.zip'):
        # with zipfile.ZipFile(iterable) as zip:
        with zipfile.ZipFile(io.BytesIO(iterable.read())) as zip:
            for name in zip.namelist():
                yield zip.open(name)
    else:
        yield iterable
