import sys

import pandas as pd

print ('arguments', sys.argv)

month = int(sys.argv[1])

df = pd.DataFrame({"Day": [1, 2], "num_passengers": [3, 4]})
df['Month']=month
print(df.head())

df.to_parquet(f"output_{month}.parquet")

print(f'hello pipeline, month={month}')