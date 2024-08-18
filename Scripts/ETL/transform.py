import pandas as pd
import numpy as np

def OlxTransform(df=None):
    """
    Function transforming data from olx website.
    """
    if not df:
        df = pd.read_csv(r'C:\code\Projekt Data Scraping\data\OLX\OLX_Data.csv', sep=',')
    
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d')
    df['create_date'] = pd.to_datetime(df['create_date'].str.split('T').str[0]).dt.strftime('%Y%m%d')
    df['modify_date'] = pd.to_datetime(df['modify_date'].str.split('T').str[0]).dt.strftime('%Y%m%d')
    
    df['floor'] = df['floor'].astype(str).str.split('_').str[1]
    df['furniture'] = df['furniture'].map({'yes':'furniture', 'no':'no_furniture'}).fillna('Unknown')
    df['market_type'] = df['market_type'].str.upper()
    word_to_num = {'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5, 'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10}
    df['rooms_num'] = df['rooms_num'].map(word_to_num)

    df = df.rename(columns={'city_name':'city'})
    df['city'] = df['city'].replace('Wroclaw', 'Wrocław').replace('Krakow', 'Kraków')
    df[['car_garage', 'heating', 'lift']] = 'Unknown'
    df['rent'] = None
    df['building_year'] = -1

    df.to_csv(r'C:\code\Projekt Data Scraping\data\OLX\OLX_Data_Transformed.csv', sep=',', index=False)
    return df


def OtoDomTransform(df=None):
    """
    Function transforming data from OtoDom website.
    """
    if not df:
        df = pd.read_csv(r'C:\code\Projekt Data Scraping\data\OTODOM\OtoDom_Data.csv', sep=',')
    
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d')
    df['create_date'] = pd.to_datetime(df['create_date'].str.split('T').str[0]).dt.strftime('%Y%m%d')
    df['modify_date'] = pd.to_datetime(df['modify_date'].str.split('T').str[0]).dt.strftime('%Y%m%d')
    
    df['lift'] = df['lift'].map({'::y':'lift', '::n':'no_lift'})
    df['car_garage'] = df['car'].str.replace('extras_types-85::garage', 'garage').fillna('no_garage')
    df['rent'] = df['rent'].str.replace(' zł', '').str.replace(' ','')
    df['floor'] = df['floor'].map({'ground_floor':'floor_0', 'no::cellar':'floor_0', 'no::garret':'floor_0'}).str.split('_').str[-1]
    df['heating'] = df['heating'].str.split('::').str[-1]
    df['rooms_num'] = df['rooms_num'].str.replace('rooms_num::more', '11')

    df['heating'] = df['heating'].fillna('Unknown')
    df['lift'] = df['lift'].fillna('Unknown')
    df['furniture'] = 'Unknown'
    df = df.drop(['building_material', 'media_types', 'security_types', 'windows_type', 'construction_status', 'outdoor', 'car'], axis=1)

    df.to_csv(r'C:\code\Projekt Data Scraping\data\OTODOM\OtoDom_Data_Transformed.csv', sep=',', index=False)
    return df


def JoinEstateData(df1=None, df2=None):
    """
    Function joining and transforming both olx and otodom data,
    """
    if not df1:
        df1 = pd.read_csv(r'C:\code\Projekt Data Scraping\data\OLX\OLX_Data_Transformed.csv', sep=',')

    if not df2:
        df2 = pd.read_csv(r'C:\code\Projekt Data Scraping\data\OTODOM\OtoDom_Data_Transformed.csv', sep=',')

    AllData = pd.concat([df1, df2]).drop_duplicates()
    AllData['building_year'] = pd.to_numeric(AllData['building_year'], errors='coerce').fillna(-1).astype('Int64')
    AllData.loc[AllData['building_year'] < 1900,  'building_year'] = None

    #Foreign keys mapping
    AllData['market_type'] = AllData['market_type'].map({'Unknown':-1, 'PRIMARY':1, 'SECONDARY':2})
    AllData['source'] = AllData['source'].map({'Unknown':-1, 'OLX':1, 'OtoDom':2})
    AllData['city'] = AllData['city'].map({'Unknown':-1, 'Katowice':1, 'Kraków':2, 'Warszawa':3, 'Wrocław':4})

    dim_offer_characteristics = pd.read_csv(r'C:\code\Projekt Data Scraping\data\dim_offer_characteristics.csv').rename(columns={'id':'offer_characteristics_id'})
    AllData = AllData.merge(dim_offer_characteristics, how='left', on=['car_garage', 'heating', 'lift', 'furniture'])
    AllData = AllData.drop(['car_garage', 'heating', 'lift', 'furniture'], axis=1)

    #Renaming the values according to SQL Server Table column names
    AllData = AllData.rename(columns={'id':'dd_offer_id',
                                      'source':'source_id',
                                      'date':'snpt_date_id',
                                      'city':'city_id',
                                      'market_type':'market_type_id',
                                      'create_date':'create_date_id',
                                      'modify_date':'modify_date_id',
                                      'price_per_m':'price_per_square_m',
                                      'rooms_num':'rooms_number'
                                      })
    
    AllData['price'] = AllData['price'].astype('float64')
    AllData['price_per_square_m'] = AllData['price_per_square_m'].astype('float64')
    AllData['area'] = AllData['area'].astype('float64')
    AllData['rent'] = AllData['rent'].str.replace(',', '.').str.replace('EUR', '').astype('float64')

    AllData.to_csv(r'C:\code\Projekt Data Scraping\data\AllData.csv', sep=',', index=False)
    return AllData