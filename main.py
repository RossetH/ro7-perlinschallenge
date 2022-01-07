from utils import *
import dask.dataframe as dd

download_folder = Path('./data')

# Get yellow cab (yc) data for 2019
for month in [str(i).zfill(2) for i in range(1,13)]: 
    get_yc_file(2019,month,download_folder)

df = dd.read_csv('data/yc_2019_*.csv')

total_amount_mean = df["total_amount"].mean()

x = total_amount_mean.compute()

total_amount_std = df["total_amount"].std()

y = total_amount_std.compute()

print(f'The answer is {x+y}')