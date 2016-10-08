
import io
import os
import shutil
import struct

from .core import TFileTransport, TCompactProtocolFactory, parquet_thrift

MARKER = b'PAR1'
import thriftpy


def thrift_print(structure, offset=0):
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
