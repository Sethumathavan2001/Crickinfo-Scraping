from akamai.edgeauth import EdgeAuth
import requests, pandas as pd
from collections import Counter
from time import time
import mysql.connector
import numpy as np, json
import warnings
import multiprocessing as mp

warnings.filterwarnings('ignore')

# Connect to MySQL (XAMPP)
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    database='cricinfo'
)
print("Database connection....")
cursor = conn.cursor()

insert_query = """
INSERT INTO series_summary (
    record_id, series_id, series, season, winner, margin,
    object_id, slug, title, status, winner_team_id
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

chunk_size = 500
ET_ENCRYPTION_KEY = '9ced54a89687e1173e91c1f225fc02abf275a119fda8a41d731d2b04dac95ff5'
DEFAULT_WINDOW_SECONDS = 60

et = EdgeAuth(**{'key': ET_ENCRYPTION_KEY,
                 'window_seconds': DEFAULT_WINDOW_SECONDS,
                 'escape_early': True
                 })

query = "SELECT series_id,object_id,status,season FROM `series_summary`;"
cursor.execute(query)
ser_df = pd.DataFrame([row for row in cursor.fetchall()], columns=['series_id', 'object_id', 'status', 'season'])
unique_series_ids = ser_df.series_id.unique().tolist()

h = {
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'dnt': '1',
    'origin': 'https://www.espncricinfo.com',
    'priority': 'u=1, i',
    'referer': 'https://www.espncricinfo.com/',
    'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
}

recordId = '335434'
url3 = f"https://hs-consumer-api.espncricinfo.com/v1/pages/record/format/results?lang=en&recordId={recordId}"
h['x-hsci-auth-token'] = et.generate_url_token(url3.replace('https://hs-consumer-api.espncricinfo.com', ''))
response = requests.get(url3, headers=h)
data3 = response.json()


def process_row(i):
    try:
        seriesId = int(i['items'][0]['link'].split('/')[-2].split('-')[-1])
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='cricinfo'
        )
        cursor = conn.cursor()

        if seriesId not in unique_series_ids:
            print(seriesId)
            series = i['items'][0]['value']
            season = i['items'][1]['value']
            winner = i['items'][2]['value']
            margin = i['items'][3]['value']
            url4 = f'https://hs-consumer-api.espncricinfo.com/v1/pages/series/schedule?lang=en&seriesId={seriesId}'
            h['x-hsci-auth-token'] = et.generate_url_token(url4.replace('https://hs-consumer-api.espncricinfo.com', ''))
            response = requests.get(url4, headers=h)
            data4 = response.json()
            match_df = pd.DataFrame(data4['content']['matches'])[['objectId', 'slug', 'title', 'status', 'winnerTeamId']]
            match_df['recordId'] = recordId
            match_df['seriesId'] = seriesId
            match_df['series'] = series
            match_df['season'] = season
            match_df['winner'] = winner
            match_df['margin'] = margin
            match_df = pd.concat([match_df.iloc[:, -6:], match_df.iloc[:, :-6]], axis=1)
            all_match_df = match_df.replace({np.nan: None, 'nan': None, 'NaN': None})
            all_match_df = all_match_df.where(pd.notnull(all_match_df), None)
            data = [
                (
                    row['recordId'], row['seriesId'], row['series'], row['season'],
                    row['winner'], row['margin'], row['objectId'], row['slug'],
                    row['title'], row['status'], row['winnerTeamId']
                )
                for _, row in all_match_df.iterrows()
            ]
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]
                cursor.executemany(insert_query, chunk)
                conn.commit()
                print(f"✅ Inserted rows {i} to {i + len(chunk)}")
        else:
            temp_df = ser_df[
                (ser_df['series_id'] == seriesId) &
                (ser_df['status'] != 'RESULT') &
                (
                    ser_df['season'].str.contains('2024', na=False) |
                    ser_df['season'].str.contains('2025', na=False)
                )
            ]
            if not temp_df.empty:
                print(f"Checking New Results: {seriesId}")
                url4 = f'https://hs-consumer-api.espncricinfo.com/v1/pages/series/schedule?lang=en&seriesId={seriesId}'
                h['x-hsci-auth-token'] = et.generate_url_token(url4.replace('https://hs-consumer-api.espncricinfo.com', ''))
                response = requests.get(url4, headers=h)
                data4 = response.json()['content']['matches']
                for v in data4:
                    if v['objectId'] in temp_df['object_id'].tolist() and v['status'] == 'RESULT':
                        update_query = "UPDATE series_summary SET status = 'RESULT', winner_team_id = %s WHERE object_id = %s"
                        cursor.execute(update_query, (v['winnerTeamId'], v['objectId']))
                        conn.commit()
                        print(f"{cursor.rowcount} row(s) updated in series_summary.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error in processing series: {e}")


if __name__ == '__main__':
    bt = time()
    rows = data3['content']['tables'][0]['rows']

    with mp.Pool(processes=mp.cpu_count()) as pool:
        pool.map(process_row, rows)

    print(f"🏁 Done in {time() - bt:.2f} seconds")
