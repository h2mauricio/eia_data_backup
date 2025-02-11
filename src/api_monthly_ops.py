#%%
import os
import requests
import pandas as pd
import datetime as dt
from time import sleep
from pathlib import Path
from dotenv import load_dotenv
#get current working directory using pathlib
cwd = Path.cwd()
#cwd = Path('Z:\\EIA_backup\\data')
month_elect_sales_path = Path(cwd).joinpath('..', 'data', 'operations_month', 'monthly_elect_sales')
month_power_ops_path = Path(cwd).joinpath('..', 'data', 'operations_month', 'monthly_power_ops')

month_elect_sales_path.mkdir(parents=True, exist_ok=True)
month_power_ops_path.mkdir(parents=True, exist_ok=True)

load_dotenv()
api_key = os.getenv("EIA_API_KEY")


#%%
def req_eia_month_elect_sales(api_url: str, api_key: str, 
                              year: int, month: int, 
                              offset:int=0, pause_time:float=0.3) -> pd.DataFrame:
    """
    Request electricity sales data for a specific year to the EIA API V2

    Parameters
    ----------
    api_key : str
        API key to access the EIA API. Can be requested at https://www.eia.gov/opendata/register.php
    year : int
        Year to request the data.

    Returns
    -------
    dataframe
        A dataframe with the requested data
    """
    
    data = {}
    params = {
        "api_key": api_key,
        "frequency": "monthly", # 'hourly' for UTC or 'local-hourly'
        'data[0]':'customers',
        'data[1]':'price',
        'data[2]':'revenue',
        'data[3]':'sales',
        'offset':str(offset),
        'length':'5000',
        'sort[0][direction]':'asc',
        'sort[0][column]':'period',
        'start':str(year) + '-' + str(month).zfill(2),
        'end':str(year) + '-' + str(month).zfill(2)}

    try:
        (r := requests.get(api_url, params=params)).raise_for_status()
        print(f'Period: {str(year) + '-' + str(month).zfill(2)}')
        data = r.json()['response']
    except Exception as e:
        print(f'Data acquisition failed due to {e}')
    #This pause is sometimes needed to avoid a 429 error from the API
    sleep(pause_time)
    return data
        
def req_eia_year_elect_sales(api_key:str, year: int) -> pd.DataFrame:
    """
    Request electricity sales data for a specific year to the EIA API V2

    Parameters
    ----------
    api_key : str
        API key to access the EIA API. Can be requested at https://www.eia.gov/opendata/register.php
    year : int
        Year to request the data.

    Returns
    -------
    dataframe
        A dataframe with the requested data
    """
    url = 'https://api.eia.gov/v2/electricity/retail-sales/data/?'
    
    df_eia = pd.DataFrame()
           
    print("--------------------------------------------")
    #print(f'Request {type_data} for year: {year}')
    # TODO: This method could be more efficient. EIA's API limits its data 
    # returns the first 5,000 rows
    ini_month = 1
    
    ini_month_dt= dt.datetime(year, ini_month, 1)
    print(f'Requesting electricity sales for: {ini_month_dt.year}')
    
    for month in range(1, 13):
        offset = 0
        while True:
            json_resp = req_eia_month_elect_sales(api_url= url,
                                                  api_key= api_key,
                                                  year= ini_month_dt.year,
                                                  month= month,
                                                  offset = offset)
            
            df_resp = pd.json_normalize(json_resp, record_path =['data'])
            #check id df_resp is empty
            if not df_resp.empty:
                df_eia = pd.concat([df_eia, df_resp], axis=0)
                df_eia['period'] = pd.to_datetime(df_eia['period'], format='%Y-%m')
            else:
                print(f'Empty response for: {ini_month_dt.year}-{month}') 
                break    
            
            if len(df_resp) == 5000:
                offset += 5000
                continue
            else:
                break
    # if df_eia is not empty, convert period to datetime
    if not df_eia.empty:
        df_eia['period'] = pd.to_datetime(df_eia['period'], utc=True)
        #df_eia = df_eia.loc[df_eia['period'].dt.date == day_dt]
    return df_eia
        
for year in range(2001, 2002):        
    df_elect_sales = req_eia_year_elect_sales(api_key, year)
    
    #Save data to csv
    file_pathname = month_elect_sales_path.joinpath(f'{year}_month_elect_sales.csv')
    print(f"Saving monthly electricity sales data for {year} in CSV")
    df_elect_sales.to_csv(file_pathname, index=False)
    sleep(0.3)
    
    
#%%

def req_month_operations(api_url: str, api_key: str, 
                         year: int, month: int, offset:int=0, 
                         pause_time:float=0.3) -> pd.DataFrame:
    """
    Request monthly electric power operations by state, sector, and energy source. Source: Form EIA-923
    
    Parameters
    ----------
    api_key : str
        API key to access the EIA API. Can be requested at https://www.eia.gov/opendata/register.php
    year : int
        Year to request the data.

    Returns
    -------
    dataframe
        A dataframe with the requested data
    """
    
    data = {}
    params = {
        "api_key": api_key,
        "frequency": "monthly", # 'hourly' for UTC or 'local-hourly'
        'data[0]':'ash-content',
        'data[1]':'consumption-for-eg',
        'data[2]':'consumption-for-eg-btu',
        'data[3]':'consumption-uto',
        'data[4]':'consumption-uto-btu',
        'data[5]':'cost',
        'data[6]':'cost-per-btu',
        'data[7]':'generation',
        'data[8]':'heat-content',
        'data[9]':'receipts',
        'data[10]':'receipts-btu',
        'data[11]':'stocks',
        'data[12]':'sulfur-content',
        'data[13]':'total-consumption',
        'data[14]':'total-consumption-btu',
        'offset':str(offset),
        'length':'5000',
        'sort[0][direction]':'asc',
        'sort[0][column]':'period',
        'start':str(year) + '-' + str(month).zfill(2),
        'end':str(year) + '-' + str(month).zfill(2)}
    
    #https://api.eia.gov/v2/electricity/monthly/data/?api_key=097E0917746D669FC846A22990D6F9CB&frequency=monthly&data[0]=ash-content&data[1]=consumption-for-eg&offset=0&length=5000&sort[0][direction]=asc&sort[0][column]=period&start=2001-01&end=2001-01
    #https://api.eia.gov/v2/electricity/electric-power-operational-data/data/?frequency=monthly&data[0]=ash-content&data[1]=consumption-for-eg&start=2001-01&end=2001-01&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000
    try:
        (r := requests.get(api_url, params=params)).raise_for_status()
        print(f'Period: {str(year) + '-' + str(month).zfill(2)}')
        data = r.json()['response']
    except Exception as e:
        print(f'Data acquisition failed due to {e}')
    #This pause is sometimes needed to avoid a 429 error from the API
    sleep(pause_time)
    return data


def req_year_operations(api_key:str, year: int) -> pd.DataFrame:
    """
    Request annual electric power operations by state, sector, and energy source. Source: Form EIA-923
    

    Parameters
    ----------
    api_key : str
        API key to access the EIA API. Can be requested at https://www.eia.gov/opendata/register.php
    year : int
        Year to request the data.

    Returns
    -------
    dataframe
        A dataframe with the requested data
    """
    url = 'https://api.eia.gov/v2/electricity/electric-power-operational-data/data/?'
    
    df_eia = pd.DataFrame()
           
    print("--------------------------------------------")
    #print(f'Request {type_data} for year: {year}')
    
    ini_month = 1
    
    ini_month_dt= dt.datetime(year, ini_month, 1)
    print(f'Requesting data for year: {ini_month_dt.year}')
    
    for month in range(1, 13):
        offset = 0
        while True:
            json_resp = req_month_operations(api_url= url,
                                                    api_key= api_key,
                                                    year= ini_month_dt.year,
                                                    month= month,
                                                    offset = offset)
            
            df_resp = pd.json_normalize(json_resp, record_path =['data'])
            #check id df_resp is empty
            if not df_resp.empty:
                df_eia = pd.concat([df_eia, df_resp], axis=0)
                df_eia['period'] = pd.to_datetime(df_eia['period'], format='%Y-%m')
            else:
                print(f'Empty response for: {ini_month_dt.year}-{month}') 
                break    
            
            if len(df_resp) == 5000:
                offset += 5000
                continue
            else:
                break
    # if df_eia is not empty, convert period to datetime
    if not df_eia.empty:
        df_eia['period'] = pd.to_datetime(df_eia['period'], utc=True)
        #df_eia = df_eia.loc[df_eia['period'].dt.date == day_dt]
    return df_eia

for year in range(2001, 2001):
    df_month_ops = req_year_operations(api_key, year)
    
    #Save data to csv
    file_pathname = month_power_ops_path.joinpath(f'{year}_month_power_ops.csv')
    print(f"Saving monthly power operations for {year} in:")
    print(file_pathname)
    df_month_ops.to_csv(file_pathname, index=False)
    sleep(0.3)
