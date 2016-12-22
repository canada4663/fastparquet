# -#- coding: utf-8 -#-
"""
Deal with parquet logical types (aka converted types), higher-order things built from primitive types.

The implementations in this class are pure python for the widest compatibility,
but they're not necessarily the most performant.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import codecs
import datetime
import json
import logging
import numba
import numpy as np
import os
import pandas as pd
import struct
import sys
from decimal import Decimal

from .thrift_structures import parquet_thrift
logger = logging.getLogger('parquet')  # pylint: disable=invalid-name

try:
    from bson import BSON
    unbson = bson.BSON.decode
    tobson = bson.BSON.encode
except ImportError:
    try:
        import bson
        unbson = bson.loads
        tobson = bson.dumps
    except:
        def unbson(x):
            raise ImportError("BSON not found")
        def tobson(x):
            raise ImportError("BSON not found")

DAYS_TO_MILLIS = 86400000000000
"""Number of millis in a day. Used to convert a Date to a date"""
nat = np.datetime64('NaT').view('int64')

simple = {parquet_thrift.Type.INT32: np.dtype('int32'),
          parquet_thrift.Type.INT64: np.dtype('int64'),
          parquet_thrift.Type.FLOAT: np.dtype('float32'),
          parquet_thrift.Type.DOUBLE: np.dtype('float64'),
          parquet_thrift.Type.BOOLEAN: np.dtype('bool'),
          parquet_thrift.Type.INT96: np.dtype('S12'),
          parquet_thrift.Type.BYTE_ARRAY: np.dtype("O"),
          parquet_thrift.Type.FIXED_LEN_BYTE_ARRAY: np.dtype("O")}
complex = {parquet_thrift.ConvertedType.UTF8: np.dtype("O"),
           parquet_thrift.ConvertedType.DECIMAL: np.dtype('float64'),
           parquet_thrift.ConvertedType.UINT_8: np.dtype('uint8'),
           parquet_thrift.ConvertedType.UINT_16: np.dtype('uint16'),
           parquet_thrift.ConvertedType.UINT_32: np.dtype('uint32'),
           parquet_thrift.ConvertedType.UINT_64: np.dtype('uint64'),
           parquet_thrift.ConvertedType.INT_8: np.dtype('int8'),
           parquet_thrift.ConvertedType.INT_16: np.dtype('int16'),
           parquet_thrift.ConvertedType.INT_32: np.dtype('int32'),
           parquet_thrift.ConvertedType.INT_64: np.dtype('int64'),
           parquet_thrift.ConvertedType.TIME_MILLIS: np.dtype('<m8[ns]'),
           parquet_thrift.ConvertedType.DATE: np.dtype('<M8[ns]'),
           parquet_thrift.ConvertedType.TIMESTAMP_MILLIS: np.dtype('<M8[ns]'),
           parquet_thrift.ConvertedType.TIME_MICROS: np.dtype('<m8[ns]'),
           parquet_thrift.ConvertedType.TIMESTAMP_MICROS: np.dtype('<M8[ns]')
           }


def typemap(se):
    """Get the final dtype - no actual conversion"""
    if se.converted_type is None:
        if se.type in simple:
            return simple[se.type]
        else:
            return np.dtype("S%i" % se.type_length)
    if se.converted_type in complex:
        return complex[se.converted_type]
    return np.dtype("O")


def _temp_convert_array(dtypein, dtypeout):
    """For the given dtypes, do we need a temporary array?"""
    return dtypein.itemsize == dtypeout.itemsize


def _shares_memory(arr1, arr2):
    return (
        arr1.__array_interface__['data'] ==
        arr2.__array_interface__['data'] and
        arr1.__array_interface__['shape'] ==
        arr2.__array_interface__['shape'])


def convert(data, se, out):
    """Convert known types from primitive to rich.

    Parameters
    ----------
    data: numpy array of primitive type
    se: a schema element.
    out: numpy array to assign to (may be the same as data, or share the same
        memory buffer)
    """
    ctype = se.converted_type
    if ctype is None or ctype == parquet_thrift.ConvertedType.INTERVAL:
        pass
    elif ctype == parquet_thrift.ConvertedType.UTF8:
        out[:] = [s.decode('utf8') for s in data]
    elif ctype == parquet_thrift.ConvertedType.DECIMAL:
        scale_factor = 10**-se.scale
        if data is output:
            out *= scale_factor
        elif data.dtype.kind in ['i', 'f']:
            out[:] = data * scale_factor
        else:  # byte-string
            # NB: general but slow method
            # could optimize when data.dtype.itemsize <= 8
            # NB: `from_bytes` may be py>=3.4 only
            out[:] = (int.from_bytes(d, byteorder='big', signed=True) *
                      scale_factor for d in data)
    elif ctype == parquet_thrift.ConvertedType.DATE:
        # out.view('int64')[:] = data * DAYS_TO_MILLIS ?
        out.view('int64')[:] = data
        out.view('int64')[:] *= DAYS_TO_MILLIS
    elif ctype == parquet_thrift.ConvertedType.TIME_MILLIS:
        time_shift(data, out.view('int64'), 1000000)
    elif ctype == parquet_thrift.ConvertedType.TIMESTAMP_MILLIS:
        time_shift(data, out.view('int64'), 1000000)
    elif ctype == parquet_thrift.ConvertedType.TIME_MICROS:
        time_shift(data, out.view('int64'))
    elif ctype == parquet_thrift.ConvertedType.TIMESTAMP_MICROS:
        time_shift(data, out.view('int64'))
    elif ctype in (parquet_thrift.ConvertedType.UINT_8,
                   parquet_thrift.ConvertedType.UINT_16,
                   parquet_thrift.ConvertedType.UINT_32,
                   parquet_thrift.ConvertedType.UINT_64,
                   parquet_thrift.ConvertedType.INT_8,
                   parquet_thrift.ConvertedType.INT_16,
                   parquet_thrift.ConvertedType.INT_32,
                   parquet_thrift.ConvertedType.INT_64):
        if not _shares_memory(out, data):
            out[:] = data
    elif ctype == parquet_thrift.ConvertedType.JSON:
        out[:] = [json.loads(d.decode('utf8')) for d in data]
    elif ctype == parquet_thrift.ConvertedType.BSON:
        out[:] = [unbson(d) for d in data]
    else:
        logger.info("Converted type '%s'' not handled",
                    parquet_thrift.ConvertedType._VALUES_TO_NAMES[ctype])  # pylint:disable=protected-access


@numba.njit(nogil=True)
def time_shift(indata, outdata, factor=1000):  # pragma: no cover
    for i in range(len(indata)):
        if indata[i] == nat:
            outdata[i] = nat
        else:
            outdata[i] = indata[i] * factor
