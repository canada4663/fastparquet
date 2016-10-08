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
