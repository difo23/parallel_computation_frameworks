import dask.dataframe as dd
import pandas as pd

# Partition dataframe into 4
df = pd.read_csv("./data/athlete_events.csv", )
df = dd.from_pandas(df, npartitions=4)

# Run parallel computation on each partition
result_df = df.groupby("Year").Age.mean().compute()

