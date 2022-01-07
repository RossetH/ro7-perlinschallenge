from pathlib import Path
import requests

def get_yc_file(year,month,download_folder):

    link = f'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{year}-{month}.csv'

    filename = Path(f'{str(download_folder)}/yc_{year}_{month}.csv')

    if not download_folder.exists():
        download_folder.mkdir()

    if not filename.exists():
        print(f'downloading file yellow cab file for {year}/{month}')
        r = requests.get(link)
        if r.status_code == 200:
            file = r.content
            with open(str(filename), 'wb') as f:
                f.write(file)
    else:
        print(f'yellow cab file for {year}/{month} already exists')

if __name__ == '__main__':

    download_folder = Path('./data')

    year = '2019'
    month = '01'

    get_yc_file(year,month)