
import gzip

# TODO: use stream/direct-to-buffer conversions instead of memcopy


compress = {'GZIP': gzip.compress}
decompress = {'GZIP': gzip.decompress}
try:
    import snappy
    compress['SNAPPY'] = snappy.compress
    decompress['SNAPPY'] = snappy.decompress
except ImportError:
    pass
try:
    import lzo
    compress['LZO'] = lzo.compress
    decompress['LZO'] =lzo. decompress
except ImportError:
    pass
try:
    import brotli
    compress['BROTLI'] = brotli.compress
    decompress['BROTLI'] = brotli.decompress
except ImportError:
    pass


def compress_data(data, algorithm='gzip'):
    if algorithm not in compress:
        raise RuntimeError("Compression '%s' not available" % compress)
    return compress[algorithm](data)


def decompress_data(data, algorithm='gzip'):
    if algorithm not in compress:
        raise RuntimeError("Decompression '%s' not available" % compress)
    return decompress[algorithm](data)
