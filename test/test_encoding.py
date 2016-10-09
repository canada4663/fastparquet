"""test_encoding.py - tests for deserializing parquet data."""
import array
import io
import struct

import parquet.encoding
from parquet import parquet_thrift


def test_int32():
    """Test reading bytes containing int32 data."""
    assert 999 == parquet.encoding.read_plain_int32(
             io.BytesIO(struct.pack(b"<i", 999)), 1)[0]

def test_int64():
    """Test reading bytes containing int64 data."""
    assert 999 == parquet.encoding.read_plain_int64(
            io.BytesIO(struct.pack(b"<q", 999)), 1)[0]

def test_int96():
    """Test reading bytes containing int96 data."""
    assert 999 == parquet.encoding.read_plain_int96(
            io.BytesIO(struct.pack(b"<qi", 0, 999)), 1)[0]

def test_float():
    """Test reading bytes containing float data."""
    assert (9.99 - parquet.encoding.read_plain_float(
            io.BytesIO(struct.pack(b"<f", 9.99)), 1)[0] < 0.01)

def test_double():
    """Test reading bytes containing double data."""
    assert (9.99 - parquet.encoding.read_plain_float(
            io.BytesIO(struct.pack(b"<d", 9.99)), 1)[0] < 0.01)

def test_fixed():
    """Test reading bytes containing fixed bytes data."""
    data = b"foobar"
    fo = io.BytesIO(data)
    assert data[:3] == parquet.encoding.read_plain_byte_array_fixed(fo, 3)
    assert data[3:] == parquet.encoding.read_plain_byte_array_fixed(fo, 3)

def test_fixed_read_plain():
    """Test reading bytes containing fixed bytes data."""
    data = b"foobar"
    fo = io.BytesIO(data)
    assert data[:3] == parquet.encoding.read_plain(
            fo, parquet_thrift.Type.FIXED_LEN_BYTE_ARRAY, 3)

def test_boolean():
    """Test reading bytes containing boolean data."""
    data = 0b1101
    fo = io.BytesIO(struct.pack(b"<i", data))
    assert [True, False, True, True] == parquet.encoding.read_plain_boolean(fo, 1)[:4]


def testFourByteValue():
    """Test reading a run with a single four-byte value."""
    fo = io.BytesIO(struct.pack(b"<i", 1 << 30))
    out = parquet.encoding.read_rle(fo, 2 << 1, 30, True)
    assert [1 << 30] * 2 ==  list(out)


def testSingleByte():
    """Test reading a single byte value."""
    fo = io.BytesIO(struct.pack(b"<B", 0x7F))
    out = parquet.encoding.read_unsigned_var_int(fo)
    assert 0x7F == out

def testFourByte():
    """Test reading a four byte value."""
    fo = io.BytesIO(struct.pack(b"<BBBB", 0xFF, 0xFF, 0xFF, 0x7F))
    out = parquet.encoding.read_unsigned_var_int(fo)
    assert 0x0FFFFFFF == out


def testFromExample():
    """Test a simple example."""
    raw_data_in = [0b10001000, 0b11000110, 0b11111010]
    encoded_bitstring = array.array('B', raw_data_in).tostring()
    fo = io.BytesIO(encoded_bitstring)
    count = 3 << 1
    res = parquet.encoding.read_bitpacked(fo, count, 3, True)
    assert list(range(8)) == res


def testFromExample():
    """Test a simple example."""
    encoded_bitstring = array.array(
        'B', [0b00000101, 0b00111001, 0b01110111]).tostring()
    fo = io.BytesIO(encoded_bitstring)
    res = parquet.encoding.read_bitpacked_deprecated(fo, 3, 8, 3, True)
    assert list(range(8)) == res


def testWidths():
    """Test all possible widths for a single byte."""
    assert 0 == parquet.encoding.width_from_max_int(0)
    assert 1 == parquet.encoding.width_from_max_int(1)
    assert 2 == parquet.encoding.width_from_max_int(2)
    assert 2 == parquet.encoding.width_from_max_int(3)
    assert 3 == parquet.encoding.width_from_max_int(4)
    assert 3 == parquet.encoding.width_from_max_int(5)
    assert 3 == parquet.encoding.width_from_max_int(6)
    assert 3 == parquet.encoding.width_from_max_int(7)
    assert 4 == parquet.encoding.width_from_max_int(8)
    assert 4 == parquet.encoding.width_from_max_int(15)
    assert 5 == parquet.encoding.width_from_max_int(16)
    assert 5 == parquet.encoding.width_from_max_int(31)
    assert 6 == parquet.encoding.width_from_max_int(32)
    assert 6 == parquet.encoding.width_from_max_int(63)
    assert 7 == parquet.encoding.width_from_max_int(64)
    assert 7 == parquet.encoding.width_from_max_int(127)
    assert 8 == parquet.encoding.width_from_max_int(128)
    assert 8 == parquet.encoding.width_from_max_int(255)
