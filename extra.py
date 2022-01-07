from utils import *
import dask.dataframe as dd

download_folder = Path('./data')

for year in [2018,2019,2020,2021]:
    for month in [str(i).zfill(2) for i in range(1,13)]: 
        get_yc_file(year,month,download_folder)

df = dd.read_csv('data/yc_*_*.csv')

total_amount_sum = df["total_amount"].sum()\
    .compute()

total_amount_mean = df["total_amount"].mean()\
    .compute()

total_amount_std = df["total_amount"].std()\
    .compute()

print(f"""
    The total amount mean of the rides is {round(total_amount_mean,2)}.   
    The total amount std of the rides is {round(total_amount_std,2)}.
    """)