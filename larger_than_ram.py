from pathlib import Path
import requests
import dask.dataframe as dd

download_folder = Path('./data')

if not download_folder.exists():
    download_folder.mkdir()

def get_yc_file(year,month):
    link = f'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{year}-{month}.csv'
    filename = Path(f'{str(download_folder)}/yc_{year}_{month}.csv')
    
    if not filename.exists():
        print(f'downloading file yellow cab file for {year}/{month}')
        r = requests.get(link)
        if r.status_code == 200:
            file = r.content
            with open(str(filename), 'wb') as f:
                f.write(file)
                f.close()
    else:
        print(f'yellow cab file for {year}/{month} already exists')

# Get 2019 data for 
for month in [str(i).zfill(2) for i in range(1,13)]:
    get_yc_file(2019,month)

df = dd.read_csv('data/yc_2019_*.csv')

total_amount_mean = df["total_amount"].mean()

x = total_amount_mean.compute()

total_amount_std = df["total_amount"].std()

y = total_amount_std.compute()

print(f'The answer is {x+y}')