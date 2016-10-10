
# TODO: use stream/direct-to-buffer conversions instead of memcopy

compress = {'gzip': gzip.compress}
decompress = {'gzip': gzip.decompress}
try:
    from snappy import decompress, compress
    compress['snappy'] = compress
    decompress['snappy'] = decompress
except ImportError:
    pass
try:
    from lzo import decompress, _compression
    compress['lzo'] = compress
    decompress['lzo'] = decompress
except ImportError:
    pass
try:
    from brotli import decompress, _compression
    compress['brotli'] = compress
    decompress['brotli'] = decompress
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
