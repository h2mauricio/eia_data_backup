#%%
import os
import requests
import pandas as pd
import datetime as dt
from time import sleep
from pathlib import Path

#get current working directory using pathlib
#cwd = Path.cwd()
cwd = os.getcwd()
hr_demand_path = Path(cwd).joinpath('..', 'data', 'operations_day_hour', 'hourly_demand')
hr_demand_bytech_path = Path(cwd).joinpath('..', 'data', 'operations_day_hour', 'hourly_demand_bytech')

#create folders if they dont exist
hr_demand_path.mkdir(parents=True, exist_ok=True)
hr_demand_bytech_path.mkdir(parents=True, exist_ok=True)

#api_key = '097E0917746D669FC846A22990D6F9CB'
api_key = '577ljs0wPebQR1SJ6vZRmXOdF1AWpZZEvsPWQrZH' #Matthew's API key

#%%
def req_eia_hourly_data(api_url: str, api_key: str, ini_date= '2020-01-01T00', 
                        end_date = '2020-01-02T00', offset = 0, pause_time=1.0) -> dict:
    """
    Request hourly data to eia API for defined period to the EIA API V2

    Parameters
    ----------
    api_url : str
        URL of the API to request the data
    api_key : str
        API key to access the EIA API. Can be requested at https://www.eia.gov/opendata/register.php
    ini_date : string, optional
        The initial date, inclusive, when the data is requested in the format YYY-MM-DDTHH. The default is '2019-01-01T00'.
    end_date : string, optional
        The end date, inclusive, when the data is requested in the format YYY-MM-DDTHH. The default is '2019-01-02T00'.

    Returns
    -------
    dict
        a dictionary with the requested data in a JSON format
    """
    data = {}
    params = {
        "api_key": api_key,
        "frequency": "hourly", # 'hourly' for UTC or 'local-hourly'
        'data[0]':'value',
        'offset':str(offset),
        'length':'5000',
        'sort[0][direction]':'asc',
        'sort[0][column]':'period',
        'start':str(ini_date) + 'T00',
        'end':str(end_date) + 'T00'}

    try:
        (r := requests.get(api_url, params=params)).raise_for_status()
        print('Period: ', str(ini_date) + 'T00', str(end_date) + 'T23')
        data = r.json()['response']
    except Exception as e:
        print(f'Data acquisition failed due to {e}')
    #This pause is sometimes needed to avoid a 429 error from the API
    sleep(pause_time)
    return data

def req_daily_demand(api_key: str, day_dt:dt.datetime) -> pd.DataFrame:
    """
    Request net generation, demand and demand forecast hourly data for a specific year to the EIA API V2

    Parameters
    ----------
    api_key : str
        API key to access the EIA API. Can be requested at https://www.eia.gov/opendata/register.php
    day_dt : datetime
        Date to request the data.

    Returns
    -------
    dataframe
        A dataframe with the requested data
    """
    df_eia = pd.DataFrame()

    url = 'https://api.eia.gov/v2/electricity/rto/region-data/data/?'
    end_period = day_dt + dt.timedelta(days=1)
    print("--------------------------------------------")
    print('Requesting net generation, demand and demand forecast from: {} T00 to: {} T23 '.format(day_dt, end_period))
    # TODO: This method could be more efficient. EIA's API limits its data returns to the first 5,000 rows
    # responsive to the request, we can set the length parameter to 5,000 (or a different length) and then use the
    # offset parameter
    #number_days = 1
    offset = 0
    
    #for day in range(0, number_days):
    day = 0
    
    while True:
        start_date = day_dt + dt.timedelta(days=day)
        end_date = day_dt + dt.timedelta(days=day + 1)
        print(f'Requesting data for day: {start_date} to {end_date}')

        json_resp = req_eia_hourly_data(api_url= url, 
                                        api_key= api_key, 
                                        ini_date= start_date, 
                                        end_date = end_date,
                                        offset = offset)
        
        df_resp = pd.json_normalize(json_resp, record_path =['data'])
        #check id df_resp is empty
        if not df_resp.empty:
            df_eia = pd.concat([df_eia, df_resp], axis=0)
            df_eia['period'] = pd.to_datetime(df_eia['period'], format='%Y-%m-%dT%H')
        else:
            print('Empty response for day: ', day)
            break    
        
        if len(df_resp) == 5000:
            offset += 5000
            continue
        else:
            break
    # if df_eia is not empty, convert period to datetime
    if not df_eia.empty:
        #df_eia['period'] = pd.to_datetime(df_eia['period'], format='%Y-%m-%dT%H')
        df_eia['period'] = pd.to_datetime(df_eia['period'], utc=True)
        #keep only the records from the requested day
        df_eia = df_eia.loc[df_eia['period'].dt.date == day_dt]
    
    #print(f"Length of df_eia: {len(df_eia)}")
    return df_eia

def req_gen_bytech_year(api_key: str, year = 2020, list_bal_auth = ['CAL']) -> pd.DataFrame:
    """
    Request power generation by Type of Technology data for a specific year to the EIA API V2

    Parameters
    ----------
    api_key : str
        API key to access the EIA API. Can be requested at https://www.eia.gov/opendata/register.php
    year : int, optional
        The year to request the data. The default is 2020.
    list_bal_auth : list, optional
        List of balancing authorities to request the data. The default is Duke Energy: ['DUK'].

    Returns
    -------
    dataframe
        a dataframe with the requested data
    """
    df_eia = pd.DataFrame()
    number_weeks = 53
    first_date = dt.datetime.strptime((str(year) + '0101'), '%Y%m%d').date()
    end_period = first_date + dt.timedelta(days=7*number_weeks)

    bal_auth_str = ''
    for bal_auth in list_bal_auth:
        bal_auth_str = bal_auth_str + '&facets[respondent][]=' + bal_auth

    url = 'https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/?' + bal_auth_str

    print("--------------------------------------------")
    print('Requesting generation by tech from: {ini_date} T00 to: {end_date} T23'.format(ini_date=first_date,
                                                                            end_date=end_period))

    # TODO: This method could be more efficient. EIA's API limits its data returns to the first 5,000 rows
    # responsive to the request, we can set the length parameter to 5,000 (or a different length) and then use the
    # offset parameter
    for week in range(0, number_weeks):
        start_date = first_date + dt.timedelta(days=7*week)
        end_date = start_date + dt.timedelta(days=6)

        json_resp = req_eia_hourly_data(api_url= url, 
                                        api_key= api_key, 
                                        ini_date= start_date, 
                                        end_date = end_date)
        
        df_resp = pd.json_normalize(json_resp, record_path =['data'])

        if not df_resp.empty:
            df_eia = pd.concat([df_eia, df_resp], axis=0)
        else:
            print('Empty response for week: ', week)
    #filter by date, only values from the requested year

    df_eia['period'] = pd.to_datetime(df_eia['period'], format='%Y-%m-%dT%H')
    return df_eia

#%%
"""## Request Demand, Net Generation, Forecasted Demand, and Interchange Data"""
ini_day = '2025-02-01'
end_day = '2025-02-02'

ini_day_dt = dt.datetime.strptime(ini_day, '%Y-%m-%d').date()
end_day_dt = dt.datetime.strptime(end_day, '%Y-%m-%d').date()
day_dt = ini_day_dt
n_days = (end_day_dt - ini_day_dt).days

for day in range(0, n_days):
    df_eia_gen_demand = req_daily_demand(api_key= api_key, day_dt=day_dt)
    #save data to csv
    file_pathname = hr_demand_path.joinpath(f'{day_dt}_hr_demand_forec_gen_interch.csv')
    print(f"Saving hourly demand and generation info for {day_dt} in CSV")
    df_eia_gen_demand.to_csv(file_pathname, index=False)
    
    day_dt = day_dt + dt.timedelta(days=1)
    sleep(0.5)
    

#%%

def req_daily_gen_bytech(api_key: str, day_dt:dt.datetime) -> pd.DataFrame:
    """
    Request generation by technology hourly data for a specific day to the EIA API V2

    Parameters
    ----------
    api_key : str
        API key to access the EIA API. Can be requested at https://www.eia.gov/opendata/register.php
    day_dt : datetime
        Date to request the data.

    Returns
    -------
    dataframe
        A dataframe with the requested data
    """
    df_eia = pd.DataFrame()

    #url = 'https://api.eia.gov/v2/electricity/rto/region-data/data/?'
    url = 'https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/?'
    print("--------------------------------------------")
    print(f'Request generation by technology for: {day_dt}')
    # TODO: This method could be more efficient. EIA's API limits its data returns 
    #  the first 5,000 rows
    offset = 0
    day = 0
    
    while True:
        start_date = day_dt + dt.timedelta(days=day)
        end_date = day_dt + dt.timedelta(days=day + 1)
        print(f'Requesting data for day: {start_date} to {end_date}')

        json_resp = req_eia_hourly_data(api_url= url, 
                                        api_key= api_key, 
                                        ini_date= start_date, 
                                        end_date = end_date,
                                        offset = offset)
        
        df_resp = pd.json_normalize(json_resp, record_path =['data'])
        #check id df_resp is empty
        if not df_resp.empty:
            df_eia = pd.concat([df_eia, df_resp], axis=0)
            df_eia['period'] = pd.to_datetime(df_eia['period'], format='%Y-%m-%dT%H')
        else:
            print('Empty response for day: ', day)
            break    
        
        if len(df_resp) == 5000:
            offset += 5000
            continue
        else:
            break
    # if df_eia is not empty, convert period to datetime
    if not df_eia.empty:
        df_eia['period'] = pd.to_datetime(df_eia['period'], utc=True)
        df_eia = df_eia.loc[df_eia['period'].dt.date == day_dt]
    return df_eia

"""## Request Generation by Technology Data"""

ini_day = '2019-01-31'
end_day = '2022-01-01'

ini_day_dt = dt.datetime.strptime(ini_day, '%Y-%m-%d').date()
end_day_dt = dt.datetime.strptime(end_day, '%Y-%m-%d').date()

day_dt = ini_day_dt
n_days = (end_day_dt - ini_day_dt).days

for day in range(0, n_days):
    df_gen_bytech = req_daily_gen_bytech(api_key= api_key, day_dt=day_dt)
    #Save data to csv
    file_pathname = hr_demand_bytech_path.joinpath(f'{day_dt}_hr_gen_bytech.csv')
    print(f"Saving hourly demand by technology data for {day_dt} in CSV")
    df_gen_bytech.to_csv(file_pathname, index=False)
    
    day_dt = day_dt + dt.timedelta(days=1)
    sleep(0.5)
    
#print("Saving generation by technology info for {} in CSV".format(year))
#df_gen_bytech.to_csv(gen_by_tech_path.joinpath('gen_by_tech_{}.csv'.format(year)))
# %%
