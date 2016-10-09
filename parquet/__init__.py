"""parquet - read parquet files."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import struct

import thriftpy

from .core import (parquet_thrift, reader, TFileTransport, TCompactProtocolFactory,
                   ParquetFormatException)
from .writer import write


class ParquetFile(object):
    """For now: metadata representation"""

    def __init__(self, fname, verify=True):
        self.fname = fname
        self.verify = verify
        if isinstance(fname, str):
            f = open(fname, 'rb')
        else:
            f = fname
        self._parse_header(f, verify)

    def _parse_header(self, f, verify=True):
        try:
            if verify:
                assert f.read(4) == b'PAR1'
            f.seek(-8, 2)
            head_size = struct.unpack('<i', f.read(4))[0]
            if verify:
                assert f.read() == b'PAR1'
        except (AssertionError, struct.error):
            raise ParquetFormatException('File parse failed: %s' % self.fname)

        f.seek(-(head_size+8), 2)
        try:
            fmd = parquet_thrift.FileMetaData()
            tin = TFileTransport(f)
            pin = TCompactProtocolFactory().get_protocol(tin)
            fmd.read(pin)
        except thriftpy.transport.TTransportException:
            raise ParquetFormatException('Metadata parse failed: %s' % self.fname)
        self.fmd=fmd
        self.head_size = head_size
        self.version = fmd.version
        self.schema = fmd.schema
        self.row_groups = fmd.row_groups
        self.key_value_metadata = fmd.key_value_metadata
        self.created_by = fmd.created_by

    @property
    def columns(self):
        return [f.name for f in self.schema if f.num_children is None]

    def to_pandas(self):
        # 4345e5eef217aa1b-c8f16177f35fd983_1150363067_data.1.parq
        # read time: 30.3s
        import pandas as pd
        return pd.DataFrame(data=self.read(), columns=self.columns)

    def read(self, columns=None):
        return list(reader(open(self.fname, 'rb'), self.fmd, columns))

    def __str__(self):
        return "<Parquet File '%s'>" % self.fname

    __repr__ = __str__
