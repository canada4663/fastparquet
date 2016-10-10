
import io
import numpy as np
import os
import shutil
import struct
import thriftpy

from .core import TFileTransport, TCompactProtocolFactory, parquet_thrift

MARKER = b'PAR1'
NaT = np.timedelta64(None).tobytes()  # require numpy version >= 1.7

typemap = {  # primitive type, converted type, bit width (if not standard)
    np.bool: (parquet_thrift.Type.BOOLEAN, None, None),
    np.int32: (parquet_thrift.Type.INT32, None, None),
    np.int64: (parquet_thrift.Type.INT64, None, None),
    np.int8: (parquet_thrift.Type.INT32, parquet_thrift.ConvertedType.INT_8, None),
    np.int16: (parquet_thrift.Type.INT64, parquet_thrift.ConvertedType.INT_16, None),
    np.uint8: (parquet_thrift.Type.INT32, parquet_thrift.ConvertedType.UINT_8, None),
    np.uint16: (parquet_thrift.Type.INT64, parquet_thrift.ConvertedType.UINT_16, None),
    np.float32: (parquet_thrift.Type.FLOAT, None, None),
    np.float64: (parquet_thrift.Type.DOUBLE, None, None),
    np.float16: (parquet_thrift.Type.FLOAT, None, 16),
}

def find_type(data):
    """ Get appropriate typecodes for column dtype

    Data conversion does not happen here, only at write time.

    The user is expected to transform their data into the appropriate dtype
    before saving to parquet, we will not make any assumptions for them.

    If the dtype is "object" the first ten items will be examined, and is str
    or bytes, will be stored as variable length byte strings; if dict or list,
    (nested data) will be stored with BSON encoding.

    To be stored as fixed-length byte strings, the dtype must be "bytesXX"
    (pandas notation) or "|SXX" (numpy notation)

    In the case of catagoricals, the data type refers to the labels; the data
    (codes) will be stored as int. The labels are usually variable length
    strings.

    BOOLs will be bitpacked using bytearray. To instead keep the default numpy
    representation of one byte per value, change to int8 or uint8 type

    Known types that cannot be represented (must be first converted another
    type or to raw binary): float128, complex

    Parameters
    ----------
    A pandas series.

    Returns
    -------
    - a thrift schema element
    - a thrift typecode to be passed to the column chunk writer

    """
    if data.dtype in typemap:
        type, converted_type, width = typemap[data.dtype]
    elif "S" in str(data.dtype):
        type, converted_type, width = (parquet_thrift.Type.FIXED_LEN_BYTE_ARRAY,
                                       None, data.dtype.itemsize)
    elif data.dtype == "O":
        if all(isinstance(i, str) for i in data[:10]):
            type, converted_type, width = (parquet_thrift.Type.BYTE_ARRAY,
                                           parquet_thrift.ConvertedType.UTF8, None)
        elif all(isinstance(i, bytes) for i in data[:10]):
            type, converted_type, width = parquet_thrift.Type.BYTE_ARRAY, None, None
        elif all(isinstance(i, list) for i in data[:10]) or all(isinstance(i, dict) for i in data[:10]):
            type, converted_type, width = (parquet_thrift.Type.BYTE_ARRAY,
                                           parquet_thrift.ConvertedType.BSON, None)
        else:
            raise ValueError("Data type conversion unknown: %s" % data.dtype)
    elif str(data.dtype).startswith("datetime64"):
        type, converted_type, width = (parquet_thrift.Type.INT64,
                                       parquet_thrift.ConvertedType.TIMESTAMP_MICROS, None)
    elif str(data.dtype).startswith("timedelta64"):
        type, converted_type, width = (parquet_thrift.Type.INT64,
                                       parquet_thrift.ConvertedType.TIME_MICROS, None)
    else:
        raise ValueError("Don't know how to convert data type: %s" % data.dtype)
    # TODO: pandas has no explicit support for Decimal
    se = parquet_thrift.SchemaElement(name=data.name, type_length=width,
                                      converted_type=converted_type)
    return se, type


def thrift_print(structure, offset=0):
    """
    Handy recursive text ouput for thrift structures
    """
    if not isinstance(structure, thriftpy.thrift.TPayload):
        return str(structure)
    s = str(structure.__class__) + '\n'
    for key in dir(structure):
        if key.startswith('_') or key in ['thrift_spec', 'read', 'write',
                                          'default_spec']:
            continue
        s = s +' ' * offset +  key + ': ' + thrift_print(getattr(structure, key), offset+2) + '\n'
    return s
thriftpy.thrift.TPayload.__str__ = thrift_print
thriftpy.thrift.TPayload.__repr__ = thrift_print


def write_thrift(fobj, thrift):
    """Write binary compact representation of thiftpy structured object

    Parameters
    ----------
    fobj: open file-like object (binary mode)
    thrift: thriftpy object to write

    Returns
    -------
    Number of bytes written
    """
    t0 = fobj.tell()
    tout = TFileTransport(fobj)
    pout = TCompactProtocolFactory().get_protocol(tout)
    thrift.write(pout)
    return fobj.tell() - t0


def write_column(f, data):
    """
    If f is a filename, opens data-only file to write to

    Returns ColumnChunk structure

    Options to implement:
        per-column encoding
        per-column compression
        specific dtype
        NaN/null handling
    """
    start = f.tell()
    rows = len(data)

    dph = parquet_thrift.DataPageHeader(num_values=rows,
            encoding=parquet_thrift.Encoding.PLAIN,
            definition_level_encoding=parquet_thrift.Encoding.RLE,
            repetition_level_encoding=parquet_thrift.Encoding.BIT_PACKED)
    l = data.nbytes

    # optional compress here

    ph = parquet_thrift.PageHeader(type=parquet_thrift.PageType.DATA_PAGE,
                                   uncompressed_page_size=l,
                                   compressed_page_size=l,
                                   data_page_header=dph, crc=None)

    write_thrift(f, ph)
    f.write(data)

    compressed_size = f.tell() - start
    uncompressed_size = compressed_size

    offset = f.tell()
    s = parquet_thrift.Statistics(max=data.max().tostring(),
                                  min=data.min().tostring(),
                                  null_count=0)

    p = parquet_thrift.PageEncodingStats(page_type=parquet_thrift.PageType.DATA_PAGE,
                                         encoding=parquet_thrift.Encoding.PLAIN,
                                         count=1)

    cmd = parquet_thrift.ColumnMetaData(type=parquet_thrift.Type.INT64,
            encodings=[parquet_thrift.Encoding.RLE,
                       parquet_thrift.Encoding.BIT_PACKED,
                       parquet_thrift.Encoding.PLAIN],
            path_in_schema=['num'],
            codec=parquet_thrift.CompressionCodec.UNCOMPRESSED,
            num_values=rows,
            data_page_offset=start,
            key_value_metadata=[],
            statistics=s,
            total_uncompressed_size=uncompressed_size,
            total_compressed_size=compressed_size,
            encoding_stats=[p])
    chunk = parquet_thrift.ColumnChunk(file_offset=offset,
                                       meta_data=cmd)
    write_thrift(f, chunk)
    return chunk


def make_row_group(f, data, schema, file_path=None):
    rows = len(data)
    rg = parquet_thrift.RowGroup(num_rows=rows, total_byte_size=0,
                                 columns=[])

    for col in schema:
        if col.type is not None:
            chunk = write_column(f, data)
            rg.columns.append(chunk)
    rg.total_byte_size = sum([c.meta_data.total_uncompressed_size for c in
                              rg.columns])

    return rg


def make_part_file(partname, data, schema):
    with open(partname, 'wb') as f:
        f.write(MARKER)
        rg = make_row_group(f, data, schema)
        fmd = parquet_thrift.FileMetaData(num_rows=len(data),
                                          schema=schema,
                                          version=1,
                                          created_by='parquet-python',
                                          row_groups=[rg])
        foot_size = write_thrift(f, fmd)
        f.write(struct.pack(b"<i", foot_size))
        f.write(MARKER)
    for chunk in rg.columns:
        chunk.file_path = os.path.abspath(partname)
    return rg


def write(filename, data, partitions=[0, 500], encoding=parquet_thrift.Encoding.PLAIN,
        compression=parquet_thrift.CompressionCodec.UNCOMPRESSED,
        file_scheme='simple'):
    """ data is a 1d int array for starters

    Provisional parameters
    ----------------------
    filename: string
        File contains everything (if file_scheme='same'), else contains the
        metadata only
    data: pandas-like dataframe
        simply could be dict of numpy arrays (in which case not sure if
        partitions should be allowed)
    partitions: list of row index values to start new row groups
    encoding: single value from parquet_thrift.Encoding, if applied to all
        columns, or dict of name:parquet_thrift.Encoding for a different
        encoding per column.
    file_scheme: 'simple'|'hive'
        If simple: all goes in a single file
        If hive: each row group is in a separate file, and filename contains
        only the metadata
    """
    if file_scheme == 'simple':
        f = open(filename, 'wb')
    else:
        os.makedirs(filename, exist_ok=True)
        f = open(os.path.join(filename, '_metadata'), 'wb')
    f.write(MARKER)
    root = parquet_thrift.SchemaElement(name='schema',
                                        num_children=0)
    fmd = parquet_thrift.FileMetaData(num_rows=len(data),
                                      schema=[root],
                                      version=1,
                                      created_by='parquet-python',
                                      row_groups=[])

    for col in ['num']:
        se = parquet_thrift.SchemaElement(name=col,
                                          repetition_type=parquet_thrift.FieldRepetitionType.REQUIRED,
                                          type=parquet_thrift.Type.INT64,
                                          type_length=64,
                                          num_children=None)
        fmd.schema.append(se)
        root.num_children += 1

    for i, start in enumerate(partitions):
        end = partitions[i+1] if i < (len(partitions) - 1) else None
        if file_scheme == 'simple':
            rg = make_row_group(f, data[start:end], fmd.schema)
        else:
            partname = os.path.join(filename, 'part.%i.parquet'%i)
            rg = make_part_file(partname, data[start:end], fmd.schema)
        fmd.row_groups.append(rg)

    foot_size = write_thrift(f, fmd)
    f.write(struct.pack(b"<i", foot_size))
    f.write(MARKER)
    f.close()
    if file_scheme != 'simple':
        f = open(os.path.join(filename, '_common_metadata'), 'wb')
        f.write(MARKER)
        fmd.row_groups = []
        foot_size = write_thrift(f, fmd)
        f.write(struct.pack(b"<i", foot_size))
        f.write(MARKER)
        f.close()


def make_unsigned_var_int(result):
    """Byte representation used for length-of-next-block"""
    bit = b''
    while result > 127:
        bit = bit + ((result & 0x7F) | 0x80).to_bytes(1, 'little')
        result >>= 7
    return bit + result.to_bytes(1, 'little')


def make_rle_string(count, value):
    """Byte representation of a single value run: count occurrances of value"""
    import struct
    header = (count << 1)
    header_bytes = make_unsigned_var_int(header)
    val_part = value.to_bytes(1, 'little')
    length = struct.pack('<l', len(header_bytes)+1)
    return length + header_bytes + val_part
