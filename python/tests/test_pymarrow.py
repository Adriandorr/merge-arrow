import unittest
import pyarrow
import pymarrow
import pandas as pd

class TestPyMarrow(unittest.TestCase):
    def test_add_index(self):
        batch = pyarrow.RecordBatch.from_arrays([
            [5, 4, 3, 2, 1],
            [1, 2, 3, 4, 5]
        ], ["a", "b"])
        actual = pymarrow.add_index(batch, ["a"])
        expected = pyarrow.RecordBatch.from_arrays([
            pyarrow.array([4, 3, 2, 1, 0], pyarrow.int8()),
            [5, 4, 3, 2, 1],
            [1, 2, 3, 4, 5]
        ], ["__marrow_index", "a", "b"], metadata={"_marrow:index": "a"})
        pd.testing.assert_frame_equal(actual.to_pandas(), expected.to_pandas())
        self.assertTrue(actual.equals(expected))

    def test_sort(self):
        batch = pyarrow.RecordBatch.from_arrays([
            [5, 4, 3, 2, 1],
            [1, 2, 3, 4, 5]
        ], ["a", "b"])
        actual = pymarrow.sort(batch, ["a"])
        expected = pyarrow.RecordBatch.from_arrays([
            [1, 2, 3, 4, 5],
            [5, 4, 3, 2, 1]
        ], ["a", "b"], metadata={"_marrow:index": "a"})
        pd.testing.assert_frame_equal(actual.to_pandas(), expected.to_pandas())
        self.assertTrue(actual.equals(expected))

    def test_merge(self):
        batch1 = pyarrow.RecordBatch.from_arrays([
            [1, 1, 2, 3, 4, 5],
            [6, 5, 4, 3, 2, 1]
        ], ["a", "b"], metadata={"_marrow:index": "a"})
        batch2 = pyarrow.RecordBatch.from_arrays([
            [1, 2, 3, 4, 5, 5],
            [5, 4, 3, 2, 1, 0]
        ], ["a", "c"], metadata={"_marrow:index": "a"})
        actual = pymarrow.merge(batch1, batch2, on=["a"], how="inner")
        expected = pyarrow.RecordBatch.from_arrays([
            [1, 1, 2, 3, 4, 5, 5],
            [6, 5, 4, 3, 2, 1, 1],
            [5, 5, 4, 3, 2, 1, 0]
        ], ["a", "b", "c"])
        pd.testing.assert_frame_equal(actual.to_pandas(), expected.to_pandas())
        self.assertTrue(actual.equals(expected))


if __name__ == '__main__':
    unittest.main()
