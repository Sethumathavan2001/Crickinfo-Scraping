from multiprocessing import Pool, cpu_count
import pandas as pd
import mysql.connector
import numpy as np
import ast
import warnings
import time
warnings.filterwarnings("ignore")

# ------------------ DATABASE ------------------
def get_db_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='',
        database='cricinfo'
    )

def get_match_data(unique_object_ids):
    if not unique_object_ids:
        return pd.DataFrame()  # Return empty DataFrame if no IDs
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SHOW COLUMNS FROM match_details;")
    columns = [i[0] for i in cursor.fetchall()]
    match_ids_str = ','.join(map(str, unique_object_ids))
    query = f"""
    SELECT * FROM match_details
    WHERE match_id IN ({match_ids_str})
    """
    cursor.execute(query)

    match_data = cursor.fetchall()
    conn.close()
    return pd.DataFrame(match_data, columns=columns)
# ------------------ CALCULATIONS ------------------
bowling_abbreviations = {
 'lab': ['L', 'A'],
 'lb': ['R', 'B'],
 'lbg': ['R', 'B'],
 'lf': ['L', 'C'],
 'lfm': ['L', 'C'],
 'lm': ['L', 'C'],
 'lmf': ['L', 'C'],
 'ls': ['L', 'C'],
 'lsm': ['L', 'C'],
 'lws': ['L', 'D'],
 'ob': ['R', 'E'],
 'rab': ['R', 'A'],
 'rf': ['R', 'F'],
 'rfm': ['R', 'F'],
 'rm': ['R', 'F'],
 'rmf': ['R', 'F'],
 'rs': ['R', 'C'],
 'rsm': ['R', 'C'],
 'sla': ['L', 'G']
 }
def assign_bowling_code(overs, innings):
    
    if not overs or pd.isna(overs):
        return '0'  # Did not bowl
    
    overs = eval(overs)
    over_ranges = {
        '1-6': any(1 <= over <= 6 for over in overs),
        '7-15': any(7 <= over <= 15 for over in overs),
        '16-20': any(16 <= over <= 20 for over in overs)
    }

    key = tuple([k for k, v in over_ranges.items() if v])

    code_map = {
        ('1-6',): {1: 'A', 2: 'D'},
        ('7-15',): {1: 'B', 2: 'E'},
        ('16-20',): {1: 'C', 2: 'F'},
        ('1-6', '7-15'): {1: 'H', 2: 'K'},
        ('1-6', '16-20'): {1: 'I', 2: 'L'},
        ('7-15', '16-20'): {1: 'J', 2: 'M'},
        ('1-6', '7-15', '16-20'): 'G'
    }

    if len(key) == 3:
        return 'G'
    elif key in code_map:
        if isinstance(code_map[key], dict):
            return code_map[key][innings]
        else:
            return code_map[key]
    else:
        return 'G'  # If it doesn’t match any known pattern
   
def get_bowling_quota_code(x):
    try:
        if pd.isna(x) or x=="":
            return "0"
        index = int(x)//25
        return ["A", "B", "C", "D"][index-1]
    except Exception as e:
        print(index)
        print(e)
        return "0"
    
def calculate_batting_points(df):
    df = df.copy()
    df['batting_points'] = 4 + df['runs'] + df['fours'] * 4 + df['sixes'] * 6

    df['batting_points'] += np.select(
        [
            df['runs'] >= 100,
            df['runs'] >= 75,
            df['runs'] >= 50,
            df['runs'] >= 25
        ],
        [16, 12, 8, 4],
        default=0
    )

    df['batting_points'] += np.where(
        (df['runs'] == 0) & df['Batting Opportunity'] & df['wicket_type'].notna(), -2, 0
    )

    sr_cond = df['balls'] >= 10
    sr_points = np.select(
        [
            df['strikerate'] > 170,
            (df['strikerate'] >= 150.01) & (df['strikerate'] <= 170),
            (df['strikerate'] >= 130) & (df['strikerate'] <= 150),
            (df['strikerate'] >= 60) & (df['strikerate'] <= 70),
            (df['strikerate'] >= 50) & (df['strikerate'] <= 50.99),
            df['strikerate'] < 50
        ],
        [6, 4, 2, -2, -4, -6],
        default=0
    )
    df['batting_points'] += np.where(sr_cond, sr_points, 0)
    return df['batting_points'].fillna(4)

def calculate_bowling_points(df):
    df = df.copy()
    df['bowling_points'] = (
        df['dots'] * 1 +
        df['wickets'] * 30 +
        df['maidens'] * 12 +
        df['bowler_wicket_type'].apply(lambda x: 8 if str(x).lower() in ['bowled', 'lbw'] else 0)
    )

    df['bowling_points'] += np.select(
        [
            df['wickets'] >= 5,
            df['wickets'] >= 4,
            df['wickets'] >= 3
        ],
        [12, 8, 4],
        default=0
    )

    eligible = df['bowled_balls'] >= 12
    eco_points = np.select(
        [
            df['economy'] < 5.00,
            (df['economy'] >= 5.00) & (df['economy'] < 6.00),
            (df['economy'] >= 6.00) & (df['economy'] <= 7.00),
            (df['economy'] >= 10.00) & (df['economy'] <= 11.00),
            (df['economy'] >= 11.01) & (df['economy'] <= 12.00),
            df['economy'] > 12.00
        ],
        [6, 4, 2, -2, -4, -6],
        default=0
    )
    df['bowling_points'] += np.where(eligible, eco_points, 0)
    return df['bowling_points'].fillna(0)

def calculate_fielding_points(df):
    return (
        df['no_of_catch'] * 8 +
        np.where(df['no_of_catch'] >= 3, 4, 0) +
        df['no_of_stumped'] * 12 +
        df['no_of_runout'].apply(lambda x: 12 if x == 1 else 6 * x)
    ).fillna(0)

def process_match(match_tuple):
    match_id, df = match_tuple
    print(f"Processing match: {match_id}")

    conn = get_db_connection()
    cursor = conn.cursor()
    bow_df = df[df['striker_id'].isna()][[
        'striker_object_id','bowler_id','bowler_name','bowling_style',
        'bowler_over_position','overs','maidens','conceded','wickets',
        'balls','fours','sixes','bowler_wicket_type','economy','dots'
    ]].rename(columns={
        'balls': 'bowled_balls',
        'fours': 'bowled_fours',
        'sixes': 'bowled_sixes'
    })

    bat_df = df[df['striker_id'].notna()].drop([
        'bowler_id','bowler_name','bowling_style','bowler_over_position',
        'overs','maidens','conceded','wickets','bowler_wicket_type',
        'economy','dots'
    ], axis=1)

    merged = bat_df.merge(bow_df, on='striker_object_id', how='left')
    merged['Batting Opportunity'] = merged['balls'].notna().astype(int)
    merged['Bowling Opportunity'] = merged['bowled_balls'].notna().astype(int)

    df2 = pd.DataFrame({
        'match_player_id': merged['match_id'].astype(str) + "_" + merged['striker_object_id'].astype(str),
        'match_id': merged['match_id'],
        'player_id': merged['striker_object_id'],
        'match_type': 6,
        'player_name': merged['striker_name'],
        'team_id': merged.apply(lambda x: x['batting_first_team_id'] if x['innings'] == 1 else x['batting_second_team_id'], axis=1),
        'team_name': merged.apply(lambda x: x['batting_first_team_name'] if x['innings'] == 1 else x['batting_second_team_name'], axis=1),
        'opponent_team_id': merged.apply(lambda x: x['batting_second_team_id'] if x['innings'] == 1 else x['batting_first_team_id'], axis=1),
        'opponent_team_name': merged.apply(lambda x: x['batting_second_team_name'] if x['innings'] == 1 else x['batting_first_team_name'], axis=1),
        'ground_id': merged['ground_id'],
        'match_date': merged['match_date'],
        'innings': merged['innings'],
        'batting_position': merged['batting_position'],
        'batting_points': calculate_batting_points(merged),
        'pp_overs_bowled': merged['bowler_over_position'].apply(lambda x: sum(1 for i in ast.literal_eval(x) if 1 <= i <= 6) if pd.notna(x) else 0),
        'middle_overs_bowled': merged['bowler_over_position'].apply(lambda x: sum(1 for i in ast.literal_eval(x) if 7 <= i <= 16) if pd.notna(x) else 0),
        'death_overs_bowled': merged['bowler_over_position'].apply(lambda x: sum(1 for i in ast.literal_eval(x) if 17 <= i <= 20) if pd.notna(x) else 0),
        'bowling_overs_slot': merged['bowler_over_position'],
        
        'bowling_points': calculate_bowling_points(merged),
        'fielding_points': calculate_fielding_points(merged)
    })

    df2['value_points'] = df2['batting_points'] + df2['bowling_points']
    df2['total_points'] = df2['value_points'] + df2['fielding_points']
    df2['player_rank'] = df2['total_points'].rank(method='min', ascending=False).astype(int)
    df2['in_dream_team'] = (df2['player_rank'] < 12).astype(int)
    df2['captain'] = (df2['player_rank'] == 1).astype(int)
    df2['vice_captain'] = (df2['player_rank'] == 2).astype(int)
    df2['bat_innings'] = merged['Batting Opportunity']
    df2['bowl_innings'] = merged['Bowling Opportunity']
    
    df2['batting_styles'] = merged['batting_styles']
    df2['batting_styles_code'] = df2['batting_styles'].apply(lambda x: "0" if pd.isna(x) else ('L' if x=='lhb' else 'R'))
    df2['batting_position_code'] = merged[['runs','batting_position']].apply(lambda x:"0" if pd.isna(x[0]) else x[1],axis=1)
    # df2['batting_code'] = df2['batting_styles_code']+df2['batting_position_code']
    df2['batting_code'] = df2[['batting_styles_code','batting_position_code']].apply(lambda x:"00" if x[1]=="0" else x[0]+x[1],axis=1)

    df2['bowling_style'] = merged['bowling_style']
    df2[['bowling_arm_code','bowling_style_code']] = df2['bowling_style'].apply(lambda x:pd.Series(bowling_abbreviations.get(x, ["0", "0"])))
    df2['bowler_over_position'] = merged['bowler_over_position']
    df2['bowler_over_code'] = merged.apply(lambda row: assign_bowling_code(row['bowler_over_position'], row['innings']), axis=1)

    df2['bowling_quota'] = merged['overs'].apply(lambda x:"" if pd.isna(x) else int(x)*25)
    df2['bowling_quota_code'] = df2['bowling_quota'].apply(get_bowling_quota_code)
    # df2['bowling_code'] = df2['bowling_arm_code']+df2['bowling_style_code']+df2['bowler_over_code']+df2['bowling_quota_code']
    df2['bowling_code'] = df2[['bowling_arm_code','bowling_style_code','bowler_over_code','bowling_quota_code']].apply(lambda x:"00" if x[3] == "0" else x[0]+x[1],axis=1)
    df2['merged_codes'] = df2['batting_code']+df2['bowling_code']
    df2 = df2.replace({np.nan: None, 'nan': None})
    insert_columns = [
        'match_player_id', 'match_id', 'player_id', 'match_type', 'player_name',
        'team_id', 'team_name', 'opponent_team_id', 'opponent_team_name', 'ground_id',
        'match_date', 'innings', 'batting_position', 'batting_points', 'pp_overs_bowled',
        'middle_overs_bowled', 'death_overs_bowled', 'bowling_overs_slot',
        'bowling_points', 'fielding_points', 'value_points', 'total_points', 'player_rank',
        'in_dream_team', 'captain', 'vice_captain', 'bat_innings', 'bowl_innings',
        'batting_styles',
               'batting_styles_code', 'batting_position_code', 'batting_code',
               'bowling_style', 'bowling_arm_code', 'bowling_style_code',
               'bowler_over_position', 'bowler_over_code', 'bowling_quota',
               'bowling_quota_code', 'bowling_code','merged_codes'
    ]
    
    insert_query = f"""
    INSERT INTO match_fantacy_points_combined (
        {', '.join(insert_columns)}
    ) VALUES ({', '.join(['%s'] * len(insert_columns))})
    """
    
    data = list(df2[insert_columns].itertuples(index=False, name=None))
    cursor.executemany(insert_query, data)
    conn.commit()
    conn.close()

def main():
    start_time = time.perf_counter()
    conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='cricinfo'
        )
    cursor = conn.cursor()

    query = """
    SELECT DISTINCT ss.match_id
FROM match_details ss
WHERE ss.match_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 
      FROM match_fantacy_points_combined md 
      WHERE md.match_id = ss.match_id
  );

    """

    cursor.execute(query)
    unique_object_ids = [row[0] for row in cursor.fetchall()]
    if unique_object_ids == []:
        return None
    all_df = get_match_data(unique_object_ids)
    match_groups = list(all_df.groupby('match_id'))

    with Pool(cpu_count() - 1) as pool:
        pool.map(process_match, match_groups)

    end_time = time.perf_counter()
    elapsed = end_time - start_time
    print(f"\n✅ Completed in {elapsed:.2f} seconds.")

if __name__ == '__main__':
    main()
