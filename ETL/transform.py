import pandas as pd

def OlxTransform(df):
    df = df.rename(columns={'city_name':'city'})
    df[['building_year', 'car', 'heating', 'lift', 'rent']] = None
    
    df['date'] = pd.to_datetime(df['date'])
    df['create_date'] = pd.to_datetime(df['create_date'].str.split('T').str[0])
    df['modify_date'] = pd.to_datetime(df['modify_date'].str.split('T').str[0])
    
    df['floor'] = df['floor'].str.split('_').str[1]
    df['furniture'] = df['furniture'].map({'yes':1, 'no':0})
    
    df['market_type'] = df['market_type'].str.upper()
    
    word_to_num = {
        'one': 1,
        'two': 2,
        'three': 3,
        'four': 4,
        'five': 5,
        'six': 6,
        'seven': 7,
        'eight': 8,
        'nine': 9,
        'ten': 10
    }
    df['rooms_num'] = df['rooms_num'].map(word_to_num)

    return df


def OtoDomTransform(df):
    df = df.drop(['building_material', 'media_types', 'security_types', 'windows_type', 'construction_status', 'outdoor'], axis=1)
    df['furniture'] = None
    
    df['date'] = pd.to_datetime(df['date'])
    df['create_date'] = pd.to_datetime(df['create_date'].str.split('T').str[0])
    df['modify_date'] = pd.to_datetime(df['modify_date'].str.split('T').str[0])
    
    df['lift'] = df['lift'].map({'::y':1, '::n':0})
    df['car'] = df['car'].str.replace('extras_types-85::garage', '1').fillna(0)
    df['rent'] = df['rent'].str.replace(' zł', '').str.replace(' ','')
    df['floor'] = df['floor'].map({'ground_floor':'floor_0', 'no::cellar':'floor_0', 'no::garret':'floor_0'}).str.split('_').str[-1]
    df['heating'] = df['heating'].str.split('::').str[-1]
    df['rooms_num'] = df['rooms_num'].str.replace('rooms_num::more', '11')

    return df


def join_data(df1, df2):
    AllData = pd.concat([df1, df2]).drop_duplicates()
    AllData['building_year'] = AllData['building_year'].astype('Int64')
    AllData.loc[AllData['building_year'] < 1900,  'building_year'] = None
    AllData = AllData.rename(columns={'furniture':'furniture_flg', 'car':'car_flg', 'lift':'lift_flg'})
    return AllData