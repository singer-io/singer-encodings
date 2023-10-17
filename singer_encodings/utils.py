import codecs
import singer

LOGGER = singer.get_logger()

def is_valid_encoding(encoding_format):

    try:
        # Attempt to look up the codecs for the specified encoding format
        codecs.lookup(encoding_format)
    except LookupError as err:
        # If a LookupError occurs (invalid encoding format), log a warning
        LOGGER.warning(err)
        return False
    return True
