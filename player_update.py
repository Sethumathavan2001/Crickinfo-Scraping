import pandas as pd
import numpy as np
import requests
import json
import mysql.connector
from collections import Counter
from time import time
from akamai.edgeauth import EdgeAuth
import warnings
import multiprocessing as mp
import traceback

warnings.filterwarnings("ignore")

# Akamai Token setup
ET_ENCRYPTION_KEY = '9ced54a89687e1173e91c1f225fc02abf275a119fda8a41d731d2b04dac95ff5'
et = EdgeAuth(key=ET_ENCRYPTION_KEY, window_seconds=60, escape_early=True)

h = {
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'en-US,en;q=0.9',
    'origin': 'https://www.espncricinfo.com',
    'referer': 'https://www.espncricinfo.com/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
}
# playerid = 1460485
def process_player(playerid):
    print(f"PlayerID: {playerid}")
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='',
        database='cricinfo'
    )
    cursor = conn.cursor()
    url = f"https://hs-consumer-api.espncricinfo.com/v1/pages/player/home?playerId={playerid}"
    h['x-hsci-auth-token'] = et.generate_url_token(url.replace('https://hs-consumer-api.espncricinfo.com',''))
    response = requests.get(url, headers=h)
    data = response.json()
    try:
        d = dict(
            PlayerId=playerid,
            BattingName=data['player'].get('battingName'),
            FieldingName=data['player'].get('fieldingName'),
            FullName=data['player'].get('fullName'),
            Gender=data['player'].get('gender'),
            BirthYear=data['player'].get('dateOfBirth', {}).get('year') if data['player'].get('dateOfBirth', {}) else None,
            BirthMonth=data['player'].get('dateOfBirth', {}).get('month') if data['player'].get('dateOfBirth', {}) else None,
            BirthDate=data['player'].get('dateOfBirth', {}).get('date') if data['player'].get('dateOfBirth', {}) else None,
            PlaceOfBirth=data['player'].get('placeOfBirth'),
            Nationality= data['player'].get('country',{}).get('name') if data['player'].get('country',{}) else None,
            BattingStyle=data['player'].get('battingStyles', [None])[0] if data['player'].get('battingStyles') else None,
            BowlingStyles=data['player'].get('bowlingStyles', [None])[0] if data['player'].get('bowlingStyles') else None,
            PlayingRoles=','.join(data['player'].get('playingRoles', [])) if data['player'].get('playingRoles') else None,
            Teams=str([
                {'teamid': i['team'].get('objectId'), 'team': i['team'].get('longName')}
                for i in data.get('content', {}).get('teams', [])
            ]) if data.get('content', {}).get('teams') else None
        )
    
        columns = ', '.join(d.keys())
        placeholders = ', '.join(['%s'] * len(d))
        
        insert_player_query = f"""
        INSERT INTO Players ({columns})  
        VALUES ({placeholders});
        """
        values = tuple(d.values())
    
        cursor.execute(insert_player_query, values)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ Player {playerid} processed.")
    except Exception as e:
        with open('error.txt', 'a') as f:
            f.write(str(playerid) + '\n')
            f.write(str(e) + '\n\n\n')
            f.write("\nAn error occurred:\n")
            traceback.print_exc(file=f)
        


if __name__ == "__main__":
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='',
        database='cricinfo'
    )
    cursor = conn.cursor()

    query = """
    SELECT DISTINCT ss.striker_object_id
FROM match_details ss
WHERE ss.striker_object_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 
      FROM players md 
      WHERE md.PlayerId = ss.striker_object_id
  );

    """

    cursor.execute(query)
    unique_object_ids = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    print(f"💼 Total players to process: {len(unique_object_ids)}")

    start = time()
    with mp.Pool(mp.cpu_count()) as pool:
        pool.map(process_player, unique_object_ids)
    print(f"🏁 Completed in {time() - start:.2f} seconds")
    
    
    