from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import gzip
import io
import json
import logging
import os
import struct
import sys
from collections import OrderedDict, defaultdict

import thriftpy
from thriftpy.protocol.compact import TCompactProtocolFactory

from . import encoding
from . import schema
from .converted_types import convert_column
from .thrift_filetransport import TFileTransport
from .thrift_structures import parquet_thrift

PY3 = sys.version_info > (3,)
logger = logging.getLogger("parquet")  # pylint: disable=invalid-name


class ParquetFormatException(Exception):
    """Generic Exception related to unexpected data format when
     reading parquet file."""
    pass


def _read_page_header(file_obj):
    """Read the page_header from the given fo."""
    tin = TFileTransport(file_obj)
    pin = TCompactProtocolFactory().get_protocol(tin)
    page_header = parquet_thrift.PageHeader()
    page_header.read(pin)
    return page_header


def read_footer(filename):
    """Read the footer and return the FileMetaData for the specified filename."""
    with open(filename, 'rb') as file_obj:
        if not _check_header_magic_bytes(file_obj) or \
           not _check_footer_magic_bytes(file_obj):
            raise ParquetFormatException("{0} is not a valid parquet file "
                                         "(missing magic bytes)"
                                         .format(filename))
        return _read_footer(file_obj)


def _get_name(type_, value):
    """Return the name for the given value of the given type_.

    The value `None` returns empty string.
    """
    return type_._VALUES_TO_NAMES[value] if value is not None else "None"  # pylint: disable=protected-access


def _get_offset(cmd):
    """Return the offset into the cmd based upon if it's a dictionary page or a data page."""
    dict_offset = cmd.dictionary_page_offset
    data_offset = cmd.data_page_offset
    if dict_offset is None or data_offset < dict_offset:
        return data_offset
    return dict_offset


def dump_metadata(filename, show_row_group_metadata, out=sys.stdout):
    """Dump metadata about the parquet object with the given filename.

    Dump human-readable metadata to specified `out`. Optionally dump the row group metadata as well.
    """
    def println(value):
        """Write a new line containing `value` to `out`."""
        out.write(value + "\n")
    footer = read_footer(filename)
    println("File Metadata: {0}".format(filename))
    println("  Version: {0}".format(footer.version))
    println("  Num Rows: {0}".format(footer.num_rows))
    println("  k/v metadata: ")
    if footer.key_value_metadata and len(footer.key_value_metadata) > 0:
        for item in footer.key_value_metadata:
            println("    {0}={1}".format(item.key, item.value))
    else:
        println("    (none)")
    println("  schema: ")
    for element in footer.schema:
        println("    {name} ({type}): length={type_length}, "
                "repetition={repetition_type}, "
                "children={num_children}, "
                "converted_type={converted_type}".format(
                    name=element.name,
                    type=parquet_thrift.Type._VALUES_TO_NAMES[element.type]  # pylint: disable=protected-access
                    if element.type else None,
                    type_length=element.type_length,
                    repetition_type=_get_name(parquet_thrift.FieldRepetitionType,
                                              element.repetition_type),
                    num_children=element.num_children,
                    converted_type=element.converted_type))
    if show_row_group_metadata:
        println("  row groups: ")
        for row_group in footer.row_groups:
            num_rows = row_group.num_rows
            size_bytes = row_group.total_byte_size
            println(
                "  rows={num_rows}, bytes={bytes}".format(num_rows=num_rows,
                                                          bytes=size_bytes))
            println("    chunks:")
            for col_group in row_group.columns:
                cmd = col_group.meta_data
                println("      type={type} file_offset={offset} "
                        "compression={codec} "
                        "encodings={encodings} path_in_schema={path_in_schema} "
                        "num_values={num_values} uncompressed_bytes={raw_bytes} "
                        "compressed_bytes={compressed_bytes} "
                        "data_page_offset={data_page_offset} "
                        "dictionary_page_offset={dictionary_page_offset}".format(
                            type=_get_name(parquet_thrift.Type, cmd.type),
                            offset=col_group.file_offset,
                            codec=_get_name(parquet_thrift.CompressionCodec, cmd.codec),
                            encodings=",".join(
                                [_get_name(
                                    parquet_thrift.Encoding, s) for s in cmd.encodings]),
                            path_in_schema=cmd.path_in_schema,
                            num_values=cmd.num_values,
                            raw_bytes=cmd.total_uncompressed_size,
                            compressed_bytes=cmd.total_compressed_size,
                            data_page_offset=cmd.data_page_offset,
                            dictionary_page_offset=cmd.dictionary_page_offset))
                with open(filename, 'rb') as file_obj:
                    offset = _get_offset(cmd)
                    file_obj.seek(offset, 0)
                    values_read = 0
                    println("      pages: ")
                    while values_read < num_rows:
                        page_header = _read_page_header(file_obj)
                        # seek past current page.
                        file_obj.seek(page_header.compressed_page_size, 1)
                        daph = page_header.data_page_header
                        type_ = _get_name(parquet_thrift.PageType, page_header.type)
                        raw_bytes = page_header.uncompressed_page_size
                        num_values = None
                        if page_header.type == parquet_thrift.PageType.DATA_PAGE:
                            num_values = daph.num_values
                            values_read += num_values
                        if page_header.type == parquet_thrift.PageType.DICTIONARY_PAGE:
                            pass

                        encoding_type = None
                        def_level_encoding = None
                        rep_level_encoding = None
                        if daph:
                            encoding_type = _get_name(parquet_thrift.Encoding, daph.encoding)
                            def_level_encoding = _get_name(
                                parquet_thrift.Encoding, daph.definition_level_encoding)
                            rep_level_encoding = _get_name(
                                parquet_thrift.Encoding, daph.repetition_level_encoding)

                        println("        page header: type={type} "
                                "uncompressed_size={raw_bytes} "
                                "num_values={num_values} encoding={encoding} "
                                "def_level_encoding={def_level_encoding} "
                                "rep_level_encoding={rep_level_encoding}".format(
                                    type=type_,
                                    raw_bytes=raw_bytes,
                                    num_values=num_values,
                                    encoding=encoding_type,
                                    def_level_encoding=def_level_encoding,
                                    rep_level_encoding=rep_level_encoding))


def _read_page(file_obj, page_header, column_metadata):
    """Read the data page from the given file-object and convert it to raw, uncompressed bytes (if necessary)."""
    bytes_from_file = file_obj.read(page_header.compressed_page_size)
    codec = column_metadata.codec
    if codec is not None and codec != parquet_thrift.CompressionCodec.UNCOMPRESSED:
        if column_metadata.codec == parquet_thrift.CompressionCodec.SNAPPY:
            raw_bytes = snappy.decompress(bytes_from_file)
        elif column_metadata.codec == parquet_thrift.CompressionCodec.GZIP:
            io_obj = io.BytesIO(bytes_from_file)
            with gzip.GzipFile(fileobj=io_obj, mode='rb') as file_data:
                raw_bytes = file_data.read()
        else:
            raise ParquetFormatException(
                "Unsupported Codec: {0}".format(codec))
    else:
        raw_bytes = bytes_from_file

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "Read page with compression type %s. Bytes %d -> %d",
            _get_name(parquet_thrift.CompressionCodec, codec),
            page_header.compressed_page_size,
            page_header.uncompressed_page_size)
    assert len(raw_bytes) == page_header.uncompressed_page_size, \
        "found {0} raw bytes (expected {1})".format(
            len(raw_bytes),
            page_header.uncompressed_page_size)
    return raw_bytes


def _read_data(file_obj, fo_encoding, value_count, bit_width):
    """Read data from the file-object using the given encoding.

    The data could be definition levels, repetition levels, or actual values.
    """
    vals = []
    if fo_encoding == parquet_thrift.Encoding.RLE:
        seen = 0
        while seen < value_count:
            values = encoding.read_rle_bit_packed_hybrid(file_obj, bit_width)
            if values is None:
                break  # EOF was reached.
            vals += values
            seen += len(values)
    elif fo_encoding == parquet_thrift.Encoding.BIT_PACKED:
        raise NotImplementedError("Bit packing not yet supported")

    return vals


def read_data_page(file_obj, schema_helper, page_header, column_metadata,
                   dictionary):
    """Read the data page from the given file-like object based upon the parameters.

    Metadata in the the schema_helper, page_header, column_metadata, and (optional) dictionary
    are used for parsing data.

    Returns a list of values.
    """
    daph = page_header.data_page_header
    raw_bytes = _read_page(file_obj, page_header, column_metadata)
    io_obj = io.BytesIO(raw_bytes)
    vals = []

    # definition levels are skipped if data is required.
    definition_levels = None
    num_nulls = 0
    max_definition_level = -1
    if not schema_helper.is_required(column_metadata.path_in_schema[-1]):
        max_definition_level = schema_helper.max_definition_level(
            column_metadata.path_in_schema)
        bit_width = encoding.width_from_max_int(max_definition_level)
        if bit_width == 0:
            definition_levels = [0] * daph.num_values
        else:
            definition_levels = _read_data(io_obj,
                                           daph.definition_level_encoding,
                                           daph.num_values,
                                           bit_width)[:daph.num_values]

        # any thing that isn't at max definition level is a null.
        num_nulls = len(definition_levels) - definition_levels.count(max_definition_level)

    # repetition levels are skipped if data is at the first level.
    repetition_levels = None  # pylint: disable=unused-variable
    if len(column_metadata.path_in_schema) > 1:
        max_repetition_level = schema_helper.max_repetition_level(
            column_metadata.path_in_schema)
        bit_width = encoding.width_from_max_int(max_repetition_level)
        repetition_levels = _read_data(io_obj,
                                       daph.repetition_level_encoding,
                                       daph.num_values,
                                       bit_width)

    # NOTE: The repetition levels aren't yet used.
    if daph.encoding == parquet_thrift.Encoding.PLAIN:
        read_values = encoding.read_plain(io_obj, column_metadata.type, daph.num_values - num_nulls)
        schema_element = schema_helper.schema_element(column_metadata.path_in_schema[-1])
        read_values = convert_column(read_values, schema_element) \
            if schema_element.converted_type is not None else read_values
        if definition_levels:
            itr = iter(read_values)
            vals.extend([next(itr) if level == max_definition_level else None for level in definition_levels])
        else:
            vals.extend(read_values)

    elif daph.encoding == parquet_thrift.Encoding.PLAIN_DICTIONARY:
        # bit_width is stored as single byte.
        bit_width = struct.unpack(b"<B", io_obj.read(1))[0]

        dict_values_bytes = io_obj.read()
        dict_values_io_obj = io.BytesIO(dict_values_bytes)
        # read_values stores the bit-packed values. If there are definition levels and the data contains nulls,
        # the size of read_values will be less than daph.num_values
        read_values = []
        while dict_values_io_obj.tell() < len(dict_values_bytes):
            read_values.extend(encoding.read_rle_bit_packed_hybrid(
                dict_values_io_obj, bit_width, len(dict_values_bytes)))

        if definition_levels:
            itr = iter(read_values)
            # add the nulls into a new array, values, but using the definition_levels data.
            values = [dictionary[next(itr)] if level == max_definition_level else None for level in definition_levels]
        else:
            values = [dictionary[v] for v in read_values]

        # there can be extra values on the end of the array because the last bit-packed chunk may be zero-filled.
        if len(values) > daph.num_values:
            values = values[0: daph.num_values]
        vals.extend(values)

    else:
        raise ParquetFormatException("Unsupported encoding: %s",
                                     _get_name(parquet_thrift.Encoding, daph.encoding))
    return vals


def _read_dictionary_page(file_obj, schema_helper, page_header, column_metadata):
    """Read a page containing dictionary data.

    Consumes data using the plain encoding and returns an array of values.
    """
    raw_bytes = _read_page(file_obj, page_header, column_metadata)
    io_obj = io.BytesIO(raw_bytes)
    values = encoding.read_plain(
        io_obj,
        column_metadata.type,
        page_header.dictionary_page_header.num_values
    )
    # convert the values once, if the dictionary is associated with a converted_type.
    schema_element = schema_helper.schema_element(column_metadata.path_in_schema[-1])
    return convert_column(values, schema_element) if schema_element.converted_type is not None else values


def reader(file_obj, footer, columns=None):
    """
    Reader for a parquet file object.

    This function is a generator returning a list of values for each row
    of data in the parquet file.

    :param file_obj: the file containing parquet data
    :param columns: the columns to include. If None (default), all columns
                    are included. Nested values are referenced with "." notation
    """
    if hasattr(file_obj, 'mode') and 'b' not in file_obj.mode:
        logger.error("parquet.reader requires the fileobj to be opened in binary mode!")
    schema_helper = schema.SchemaHelper(footer.schema)
    keys = columns if columns else [s.name for s in
                                    footer.schema if s.type]
    for row_group in footer.row_groups:
        res = defaultdict(list)
        row_group_rows = row_group.num_rows
        for col_group in row_group.columns:
            dict_items = []
            cmd = col_group.meta_data
            # skip if the list of columns is specified and this isn't in it
            if columns and not ".".join(cmd.path_in_schema) in columns:
                continue

            offset = _get_offset(cmd)
            file_obj.seek(offset, 0)
            values_seen = 0
            while values_seen < row_group_rows:
                page_header = _read_page_header(file_obj)
                if page_header.type == parquet_thrift.PageType.DATA_PAGE:
                    values = read_data_page(file_obj, schema_helper, page_header, cmd,
                                            dict_items)
                    res[".".join(cmd.path_in_schema)] += values
                    values_seen += page_header.data_page_header.num_values
                elif page_header.type == parquet_thrift.PageType.DICTIONARY_PAGE:
                    if debug_logging:
                        logger.debug(page_header)
                    assert dict_items == []
                    dict_items = _read_dictionary_page(file_obj, schema_helper, page_header, cmd)
                else:
                    logger.info("Skipping unknown page type=%s",
                                _get_name(parquet_thrift.PageType, page_header.type))

        for i in range(row_group.num_rows):
            yield [res[k][i] for k in keys if res[k]]
