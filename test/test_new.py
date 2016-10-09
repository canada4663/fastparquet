import numpy as np
import os
from parquet import ParquetFile
from parquet import write
import pytest
import shutil
import tempfile
pyspark = pytest.importorskip("pyspark")
sc = pyspark.SparkContext.getOrCreate()
sql = pyspark.SQLContext(sc)


@pytest.yield_fixture()
def tempdir():
    d = tempfile.mkdtemp()
    yield d
    if os.path.exists(d):
        shutil.rmtree(d, ignore_errors=True)


def test_pyspark_roundtrip(tempdir):
    data = np.arange(1000, dtype=np.int64)
    fname = os.path.join(tempdir, 'test.parquet')
    write(fname, data, file_scheme='simple')

    r = ParquetFile(fname)

    # default reader produces a list per row
    assert sum(r.read(), []) == data.tolist()

    df = sql.read.parquet(fname)
    ddf = df.toPandas()
    assert (ddf.num.values == data).all()

    fname = os.path.join(tempdir, 'test.dir.parquet')
    write(fname, data, file_scheme='hive')
    df = sql.read.parquet(fname)
    ddf = df.toPandas()
    assert (ddf.num.values == data).all()


