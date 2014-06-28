from koalas import DataFrame
import numpy as np

def test_simple():
	df = DataFrame.from_map({"a": np.random.random((10,)), "b": np.random.random((10,))})
	assert set(df.columns) == {"a", "b"} 
	assert df.nb_rows == 10
	assert df.nb_cols == 2
	assert df["a"].shape == (10,)
