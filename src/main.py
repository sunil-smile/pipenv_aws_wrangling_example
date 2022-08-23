import numpy as np
import pandas as pd

input_df = pd.DataFrame(
    {
        "stock": pd.Series(data=["AA", "CC"], index=[0, 2]),
        "price": pd.Series(data=[200, 750], index=[0, 1]),
        "buyer": pd.Series(data=["sunil", "VidhyA", "MaHathi"]),
    }
)
input_df.info(verbose=True)
print(input_df.head(10))

infer_type = lambda x: pd.api.types.infer_dtype(x, skipna=True)
input_df.apply(infer_type, axis=0)
input_df.info(verbose=True)
