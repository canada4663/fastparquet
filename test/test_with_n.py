import io
import numpy as np
import os
from parquet import encoding, core, ParquetFile, schema

here = os.path.dirname(__file__)
count = 1000

def test_read_bitpacked():
    results = np.empty(1000000, dtype=np.int32)
    with open(os.path.join(here, 'bitpack')) as f:
        for i, l in enumerate(f):
            if i > count:
                break
            raw, head, wid, res = eval(l)
            i = encoding.Numpy8(np.frombuffer(memoryview(raw), dtype=np.uint8))
            o = encoding.Numpy32(results)
            encoding.read_bitpacked(i, head, wid, o)
            assert (res == o.so_far()).all()


def test_rle():
    results = np.empty(1000000, dtype=np.int32)
    with open(os.path.join(here, 'rle')) as f:
        for i, l in enumerate(f):
            if i > count:
                break
            data, head, width, res = eval(l)
            i = encoding.Numpy8(np.frombuffer(memoryview(data), dtype=np.uint8))
            o = encoding.Numpy32(results)
            encoding.read_rle(i, head, width, o)
            assert (res == o.so_far()).all()


def test_uvarint():
    with open(os.path.join(here, 'uvarint')) as f:
        for i, l in enumerate(f):
            if i > count:
                break
            data, res = eval(l)
            i = encoding.Numpy8(np.frombuffer(memoryview(data), dtype=np.uint8))
            o = encoding.read_unsigned_var_int(i)
            assert (res == o)


def test_hybrid():
    results = np.empty(1000000, dtype=np.int32)
    with open(os.path.join(here, 'hybrid')) as f:
        for i, l in enumerate(f):
            if i > count // 20:
                break
            (data, width, length, res) = eval(l)
            i = encoding.Numpy8(np.frombuffer(memoryview(data), dtype=np.uint8))
            o = encoding.Numpy32(results)
            encoding.read_rle_bit_packed_hybrid(i, width, length, o)
            assert (res == o.so_far()).all()


def test_read_data():
    with open(os.path.join(here, 'read_data')) as f:
        for i, l in enumerate(f):
            (data, fo_encoding, value_count, bit_width, res) = eval(l)
            i = encoding.Numpy8(np.frombuffer(memoryview(data), dtype=np.uint8))
            out = core.read_data(i, fo_encoding, value_count,
                                   bit_width)
            for o, r in zip(out, res):
                # result from old version is sometimes 1 value too long
                assert o == r


def test_to_pandas():
    fname = '/Users/mdurant/Downloads/parquet-python/test-data/airlines_parquet/4345e5eef217aa1b-c8f16177f35fd983_1150363067_data.1.parq'
    pf = ParquetFile(fname)
    out = pf.to_pandas()
    print(out.head())