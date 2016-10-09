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


@pytest.mark.parametrize("scheme,partitions", [
    ('simple', [0]),
    ('simple', [0, 500]),
    ('hive', [0]),
    ('hive', [0, 500])
])
def test_pyspark_roundtrip(tempdir, scheme, partitions):
    data = np.arange(1000, dtype=np.int64)
    fname = os.path.join(tempdir, 'test.parquet')
    write(fname, data, file_scheme=scheme, partitions=partitions)

    df = sql.read.parquet(fname)
    ddf = df.toPandas()
    assert (ddf.num.values == data).all()


@pytest.mark.parametrize('partitions', [[0], [0, 500]])
def test_roundtrip(tempdir, partitions):
    data = np.arange(1000, dtype=np.int64)
    fname = os.path.join(tempdir, 'test.parquet')
    write(fname, data, file_scheme='simple', partitions=partitions)

    r = ParquetFile(fname)

    # default reader produces a list per row
    assert (r.to_pandas().num == data).all()

