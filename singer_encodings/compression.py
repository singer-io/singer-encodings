import gzip
import zipfile
import tarfile

def infer(iterable, file_name):
    """Uses the incoming file_name and checks the end of the string
    for supported compression types"""
    if not file_name:
        raise Exception("Need file name")

    if file_name.endswith('.tar.gz') or file_name.endswith('.tar.bz2') or file_name.endswith('.tar'):
        with tarfile.open(fileobj=iterable, mode='r|*') as tar:
            for tarmember in tar.getmembers():
                yield tar.extractfile(tarmember)
    elif file_name.endswith('.gz'):
        yield gzip.GzipFile(fileobj=iterable)
    elif file_name.endswith('.zip'):
        with zipfile.ZipFile(iterable) as zip:
            for name in zip.namelist():
                yield zip.open(name)
    else:
        yield iterable
