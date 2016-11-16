
import gzip
import io
import sys
from .thrift_structures import parquet_thrift
# TODO: use stream/direct-to-buffer conversions instead of memcopy

# TODO: enable ability to pass kwargs to compressor

PY2 = sys.version_info.major == 2

if PY2:
    def gzip_compress(b):
        i = io.BytesIO()
        g = gzip.GzipFile(fileobj=i, mode='wb')
        g.write(b)
        g.close()
        return i.getvalue()
    gzip.compress = gzip_compress

    def gzip_decompress(b):
        i = io.BytesIO(b)
        g = gzip.GzipFile(fileobj=i, mode='rb')
        return g.read()
    gzip.decompress = gzip_decompress

compressions = {'GZIP': gzip.compress,
                'UNCOMPRESSED': lambda x: x}

decompressions = {'GZIP': gzip.decompress,
                  'UNCOMPRESSED': lambda x: x}
try:
    import snappy
    compressions['SNAPPY'] = snappy.compress
    decompressions['SNAPPY'] = snappy.decompress
except ImportError:
    pass
try:
    import lzo
    compressions['LZO'] = lzo.compress
    decompressions['LZO'] =lzo.decompress
except ImportError:
    pass
try:
    import brotli
    compressions['BROTLI'] = brotli.compress
    decompressions['BROTLI'] = brotli.decompress
except ImportError:
    pass

compressions = {k.upper(): v for k, v in compressions.items()}
decompressions = {k.upper(): v for k, v in decompressions.items()}

rev_map = {getattr(parquet_thrift.CompressionCodec, key): key for key in
           dir(parquet_thrift.CompressionCodec) if key in compressions}


def compress_data(data, algorithm='gzip'):
    if isinstance(algorithm, int):
        algorithm = rev_map[algorithm]
    if algorithm.upper() not in compressions:
        raise RuntimeError("Compression '%s' not available.  Options: %s" %
                (algorithm, sorted(compressions)))
    return compressions[algorithm.upper()](data)


def decompress_data(data, algorithm='gzip'):
    if isinstance(algorithm, int):
        algorithm = rev_map[algorithm]
    if algorithm.upper() not in decompressions:
        raise RuntimeError("Decompression '%s' not available.  Options: %s" %
                (algorithm.upper(), sorted(decompressions)))
    return decompressions[algorithm.upper()](data)
