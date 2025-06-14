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

def clean(val):
    if isinstance(val, (list, dict)):
        return json.dumps(val)  # convert to JSON string for safe DB insert
    if pd.isna(val):
        return None
    if isinstance(val, str) and val.strip().lower() in ['nan', 'none', 'null']:
        return None
    return val



def match_results(seriesId,matchId):
    print(f"Series ID: {seriesId}, Match Id: {matchId}")
    l = []
    for inning in [1,2]:
        n = "0"
        while n:
            url = f"https://hs-consumer-api.espncricinfo.com/v1/pages/match/comments?lang=en&seriesId={seriesId}&matchId={matchId}&inningNumber={inning}&commentType=ALL&sortDirection=ASC&fromInningOver={n}"
            h['x-hsci-auth-token'] = et.generate_url_token(url.replace('https://hs-consumer-api.espncricinfo.com',''))
            response = requests.get(url, headers=h)
            data = response.json()
            l.extend(data['comments'])
            n = data['nextInningOver']
            print(data['nextInningOver'])
    df = pd.DataFrame(l)
    if l:
        
        df['wicket_type'] = df['dismissalText'].apply(lambda x:"NO" if pd.isna(x) else x.get('short'))
        df['isWicket'] = df['isWicket'].apply(lambda x:"YES" if x else "NO")
        df["InvolvedPerson"] = df['dismissalText'].apply(lambda x:None if pd.isna(x) else x.get('fielderText'))
        df["SeriesId"] = seriesId
        df["MatchId"] = matchId
    url2 = f"https://hs-consumer-api.espncricinfo.com/v1/pages/match/scorecard?lang=en&seriesId={seriesId}&matchId={matchId}"
    h['x-hsci-auth-token'] = et.generate_url_token(url2.replace('https://hs-consumer-api.espncricinfo.com',''))
    response = requests.get(url2, headers=h)
    data2 = response.json()
    match_date = data2['match']['startDate'].split('T')[0]
    match_format = data2['match']['format']
    toss_winner_team_id = data2['match']['tossWinnerTeamId']
    if data2['match']['teams'][0]['inningNumbers'][0] == 1:
        batting_first_team_id = data2['match']['teams'][0]['team']['id']
        batting_first_team_name = data2['match']['teams'][0]['team']['name']
        batting_second_team_id = data2['match']['teams'][1]['team']['id']
        batting_second_team_name = data2['match']['teams'][1]['team']['name']
    else:
        batting_first_team_id = data2['match']['teams'][1]['team']['id']
        batting_first_team_name = data2['match']['teams'][1]['team']['name']
        batting_second_team_id = data2['match']['teams'][0]['team']['id']
        batting_second_team_name = data2['match']['teams'][0]['team']['name']
        
    winner_team_id = data2['match']['winnerTeamId']
    if winner_team_id == batting_first_team_id:
        winner_team_name = batting_first_team_name
    else:
        winner_team_name = batting_second_team_name
    
    match_result = data2['match']['statusText']
    ground_id = data2['match']['ground']['objectId']
    ground_name = data2['match']['ground']['name']
    
    l2 = []
    inning = 1
    for i in data2['content']['innings']:
        bow_df = pd.DataFrame(i['inningBowlers'])
        bow_df['StrikerObjectId'] = bow_df['player'].apply(lambda x:x.get('objectId',None))
        bow_df['BowlerId'] = bow_df['player'].apply(lambda x:x.get('id',None))
        bow_df['BowlerName'] = bow_df['player'].apply(lambda x:x.get('name',None))
        bow_df['BowlingStyle'] = bow_df['player'].apply(lambda x:x.get('bowlingStyles',None)[0] if x.get('bowlingStyles',None) else None)
        try:
            bow_df['BowlerOverPosition'] = bow_df['BowlerId'].apply(lambda x: df[df["bowlerPlayerId"]==x]['overNumber'].unique().tolist())
        except:
            bow_df['BowlerOverPosition'] = None
        try:
            bow_df['BowlerWicketType'] = bow_df['BowlerId'].apply(lambda x: dict(Counter(df[(df["bowlerPlayerId"]==x) & (df['isWicket'] =="YES")]['wicket_type'].tolist())))
        except:
            bow_df['BowlerWicketType'] = None
        bow_df['Innings'] = inning
        
        bat_df = pd.DataFrame(i['inningBatsmen'])
        # bat_df = bat_df[~bat_df['balls'].isna()]
        bat_df = bat_df.sort_values(by='battedType',ascending=False)
        bat_df['StrikerId'] = bat_df['player'].apply(lambda x:x.get('id',None))
        bat_df['StrikerObjectId'] = bat_df['player'].apply(lambda x:x.get('objectId',None))
        bat_df['StrikerName'] = bat_df['player'].apply(lambda x:x.get('name',None))
        bat_df['battingStyles'] = bat_df['player'].apply(lambda x:x.get('battingStyles',None)[0] if x.get('battingStyles',None) else None)
        bat_df['Innings'] = inning
        
        if i['inningNumber'] == 1:
            bat_df['BattingPosition'] = [chr(65+_) for _ in range(len(bat_df))]
        elif i['inningNumber'] == 2:
            bat_df['BattingPosition'] = [chr(77+_) for _ in range(len(bat_df))]
        bat_df[["WiketTakingBowlerId", "WiketTakingBowlerName"]] = bat_df['dismissalBowler'].apply(
            lambda x: (
                x.get('id', None),
                x.get('name', None)
            ) if x else (None, None)
        ).apply(pd.Series)
        
        bat_df["WiketType"] = bat_df['dismissalText'].apply(lambda x:x.get('short',None) if x else None)
        bat_df[['WiketTakingPersonId', 'WiketTakingPersonName']] = bat_df['dismissalFielders'].apply(
            lambda x: (
                x[0].get('player', {}).get('id', None),
                x[0].get('player', {}).get('name', None)
            ) if x and x[0].get('player') else (None, None)
        ).apply(pd.Series)
        l2.extend([bat_df,bow_df])
        inning+=1
        # break
    df3 = pd.concat(l2)
    
    df3["MatchDate"] = match_date
    df3["MatchFormat"] = match_format
    df3["TossWinnerTeamId"] = toss_winner_team_id
    df3["BattingFirstTeamId"] = batting_first_team_id
    df3["BattingFirstTeamName"] = batting_first_team_name
    df3["BattingSecondTeamId"] = batting_second_team_id
    df3["BattingSecondTeamName"] = batting_second_team_name
    df3["WinnerTeamId"] = winner_team_id
    df3["WinnerTeamName"] = winner_team_name
    df3["MatchResult"] = match_result
    df3["GroundId"] = ground_id
    df3["GroundName"] = ground_name
    df3["SeriesId"] = seriesId
    df3["MatchId"] = matchId
    
    df3["No. of Catch"] = df3['StrikerId'].apply(lambda x:len(df3[(df3['WiketTakingPersonId']==x)&(df3['WiketType']=='caught')]))
    df3["No. of Stumped"] = df3['StrikerId'].apply(lambda x:len(df3[(df3['WiketTakingPersonId']==x)&(df3['WiketType']=='stumped')]))
    df3["No. of RunOut"] = df3['StrikerId'].apply(lambda x:len(df3[(df3['WiketTakingPersonId']==x)&(df3['WiketType']=='run out')]))
    try:
        df['BowlerName'] = df['bowlerPlayerId'].apply(lambda x:df3[df3['BowlerId']==x]["BowlerName"].iloc[0])
    except: 
        df['BowlerName'] = None
    try:    
        df['StrikerName'] = df['batsmanPlayerId'].apply(lambda x:df3[df3['StrikerId']==x]["StrikerName"].iloc[0])
    except:
        df['StrikerName'] =None
    try:
        df['NonStrikerName'] = df['nonStrikerPlayerId'].apply(lambda x:df3[df3['StrikerId']==x]["StrikerName"].iloc[0])
    except:
        df['NonStrikerName'] = None
    col = {
    'SeriesId':'Series ID',
    'MatchId':'Match ID',
    'inningNumber' :'Innings Number',
      'oversActual': 'BALL NUMBER',
     'bowlerPlayerId': 'BOWLER ID',
     'BowlerName': 'BOWLER NAME',
     'pitchLength': 'BOWLING TYPE',
     'pitchLine': 'PITCHING SPOT',
     'batsmanPlayerId': 'STRIKER ID',
     'StrikerName':'STRIKER NAME',
     'nonStrikerPlayerId': 'NON STRIKER ID',
     'NonStrikerName' :'NON STRIKER NAME',
     'shotType': 'SHOT SPOT',
     'totalRuns': 'RUNS SCORED',
     'isWicket': 'WICKETS',
     'wicket_type': 'WICKET TYPE'
     }
    try:
        df2 = df[col.keys()].rename(columns=col)
    except:
        df2 = pd.DataFrame()
    col2 = [ 'SeriesId', 'MatchId', 'MatchDate',
    'MatchFormat', 'TossWinnerTeamId', 'BattingFirstTeamId',
    'BattingFirstTeamName', 'BattingSecondTeamId', 'BattingSecondTeamName',
    'WinnerTeamId', 'WinnerTeamName', 'MatchResult', 'GroundId',
    'GroundName','Innings', 'StrikerId','StrikerObjectId', 'StrikerName','BattingPosition',
    'battingStyles', 'balls','runs','fours','sixes','strikerate',
    'WiketTakingBowlerId', 'WiketTakingBowlerName','WiketType','WiketTakingPersonId', 'WiketTakingPersonName',
    'BowlerId', 'BowlerName',
    'BowlingStyle', 'BowlerOverPosition','overs','maidens','conceded','wickets',
    'BowlerWicketType','economy','dots',"No. of Catch","No. of Stumped","No. of RunOut"]
    df4 = df3[col2]
    return df2,df4

def process_match(idx):
    try:
        matchId, seriesId = idx
        print(f"▶ Processing MatchID: {matchId} | SeriesID: {seriesId}")

        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='cricinfo'
        )
        cursor = conn.cursor()

        # def clean(val):
        #     if pd.isna(val) or str(val).strip().lower() == 'nan':
        #         return None
        #     return val

        df2, df4 = match_results(seriesId, matchId)

        commentary_flag = match_details_flag = None

        if not df2.empty:
            df2 = df2.replace({np.nan: None})
            df2 = df2.where(pd.notnull(df2), None)
            insert_commentary_query = """
            INSERT INTO commentary (
                series_id, match_id, innings_number, ball_number, bowler_id, bowler_name, 
                bowling_type, pitching_spot, striker_id, striker_name, non_striker_id, 
                non_striker_name, shot_spot, runs_scored, wickets, wicket_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            commentary_data = [
                (
                    row['Series ID'], row['Match ID'], row['Innings Number'], row['BALL NUMBER'],
                    row['BOWLER ID'], row['BOWLER NAME'], row['BOWLING TYPE'], row['PITCHING SPOT'],
                    row['STRIKER ID'], row['STRIKER NAME'], row['NON STRIKER ID'], row['NON STRIKER NAME'],
                    row['SHOT SPOT'], row['RUNS SCORED'], row['WICKETS'], row['WICKET TYPE']
                )
                for _, row in df2.iterrows()
            ]
            for i in range(0, len(commentary_data), 500):
                chunk = commentary_data[i:i + 500]
                cursor.executemany(insert_commentary_query, chunk)
                conn.commit()
            commentary_flag = "Yes"

        if not df4.empty:
            df4 = df4.replace({np.nan: None})
            df4 = df4.where(pd.notnull(df4), None)
            insert_match_details_query = """
            INSERT INTO match_details (
                series_id, match_id, match_date, match_format, toss_winner_team_id, batting_first_team_id, 
                batting_first_team_name, batting_second_team_id, batting_second_team_name, winner_team_id, 
                winner_team_name, match_result, ground_id, ground_name, innings, striker_id,striker_object_id, striker_name, 
                batting_position, batting_styles, balls, runs, fours, sixes, strikerate, wicket_taking_bowler_id, 
                wicket_taking_bowler_name, wicket_type, wicket_taking_person_id, wicket_taking_person_name, bowler_id, 
                bowler_name, bowling_style, bowler_over_position, overs, maidens, conceded, wickets, bowler_wicket_type, economy, 
                dots, no_of_catch, no_of_stumped, no_of_runout
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            match_details_data = []
            for _, row in df4.iterrows():
                data = [
                    clean(row.get(col)) for col in [
                        'SeriesId', 'MatchId', 'MatchDate', 'MatchFormat', 'TossWinnerTeamId',
                        'BattingFirstTeamId', 'BattingFirstTeamName', 'BattingSecondTeamId',
                        'BattingSecondTeamName', 'WinnerTeamId', 'WinnerTeamName', 'MatchResult',
                        'GroundId', 'GroundName', 'Innings', 'StrikerId','StrikerObjectId', 'StrikerName',
                        'BattingPosition', 'battingStyles', 'balls', 'runs', 'fours', 'sixes',
                        'strikerate', 'WiketTakingBowlerId', 'WiketTakingBowlerName', 'WiketType',
                        'WiketTakingPersonId', 'WiketTakingPersonName', 'BowlerId', 'BowlerName',
                        'BowlingStyle', 'BowlerOverPosition', 'overs', 'maidens', 'conceded', 'wickets',
                        'BowlerWicketType', 'economy', 'dots', 'No. of Catch', 'No. of Stumped',
                        'No. of RunOut'
                    ]
                ]
                if isinstance(data[32], list):
                    data[32] = json.dumps(data[32])
                if isinstance(data[36], dict):
                    data[36] = json.dumps(data[36])
                match_details_data.append(tuple(data))

            for i in range(0, len(match_details_data), 500):
                chunk = match_details_data[i:i + 500]
                cursor.executemany(insert_match_details_query, chunk)
                conn.commit()
            match_details_flag = "Yes"

        update_query = "UPDATE series_summary SET commentary = %s, match_details = %s WHERE object_id = %s"
        cursor.execute(update_query, (commentary_flag, match_details_flag, matchId))
        conn.commit()

        cursor.close()
        conn.close()
        print(f"✅ Match {matchId} processed.")
    except Exception as e:
        with open('error.txt', 'a') as f:
            f.write(str(idx) + '\n')
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
    SELECT ss.object_id, ss.series_id, ss.status
    FROM series_summary ss
    WHERE ss.status = 'RESULT'
      AND NOT EXISTS (
        SELECT 1 
        FROM match_details md 
        WHERE md.match_id = ss.object_id
      )
    ORDER BY ss.object_id DESC;
    """

    cursor.execute(query)
    unique_object_ids = [(row[0], row[1]) for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    print(f"💼 Total matches to process: {len(unique_object_ids)}")

    start = time()
    with mp.Pool(mp.cpu_count()) as pool:
        pool.map(process_match, unique_object_ids)
    print(f"🏁 Completed in {time() - start:.2f} seconds")
