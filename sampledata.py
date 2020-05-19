from sampleparser import sample
import sqlite3
import pandas as pd
import numpy as np
import datetime
import plotly.graph_objects as go
from visc import visc
import math


def sample_data(current_sample): 
    #get start and end time for sql query
    sample_start = pd.Timestamp.to_pydatetime(current_sample['Start Time'][0])
    sample_end = current_sample['End Time'][-1]
    if pd.isnull(sample_end):
        sample_end = datetime.datetime.utcnow()
    else :
        sample_end = pd.Timestamp.to_pydatetime(current_sample['End Time'][-1])

    params = [sample_start, sample_end]
    conn = sqlite3.connect('logs.db')
    #c = conn.cursor()
    df_sample_raw = pd.read_sql_query("SELECT * from COMBINED WHERE DateTime BETWEEN ? and ?", params = params, con=conn)
    df_sample_raw['DateTime'] = df_sample_raw['DateTime'].astype('datetime64[ns]')
    df_sample = pd.DataFrame()
    # print(df_sample)
    for i in range(len(current_sample['Start Time'])):
        #get time intervals
        interval_start = pd.Timestamp.to_pydatetime(current_sample['Start Time'][i])
        interval_end = current_sample['End Time'][i]

        if pd.isnull(interval_end):
            interval_end = datetime.datetime.utcnow()
        else :
            interval_end = pd.Timestamp.to_pydatetime(current_sample['End Time'][i])

        mask = (df_sample_raw['DateTime'] > interval_start) & (df_sample_raw['DateTime'] <= interval_end)
        df_sample.loc[mask, 'DateTime'] = df_sample_raw['DateTime']
        df_sample.loc[mask, 'Confining Pressure'] = current_sample['Confining Pressure'][i]
        df_sample.loc[mask, 'Upstream Pressure'] = df_sample_raw[current_sample['vessel']+'Up']
        df_sample.loc[mask, 'Downstream Pressure'] = df_sample_raw[current_sample['vessel']+'Down']
        if current_sample['Pump'][i] == 'None':
            df_sample.loc[mask, 'Rate'] = 0
        else:
            df_sample.loc[mask, 'Rate'] = df_sample_raw[current_sample['Pump'][i]+'Rate']
        #df_sample.loc[mask, 'Rate'] = df_sample_raw[current_sample['Pump'][i]+'Rate']
        df_sample.loc[mask, 'Comment'] = current_sample['Instance Comment'][i]
    #temp values for viscosity params
    a, b = visc(current_sample['fluid'], current_sample['temperature'])
    # a = 8.365010181414292e-06
    # b = 0.0732989622362088
    df_sample['dp'] = df_sample['Upstream Pressure'].astype(int) - df_sample['Downstream Pressure'].astype(int)
    df_sample['absdp'] = abs(df_sample['Upstream Pressure'].astype(int) - df_sample['Downstream Pressure'].astype(int))
    df_sample['qdp'] = df_sample['Rate']/df_sample['dp']
    df_sample['Viscosity'] = a*df_sample['Upstream Pressure'].astype(int)+b
    area = math.pi*(current_sample['diameter']/2)**2
    df_sample['Permeability'] = (df_sample['Rate']/60*df_sample['Viscosity']*current_sample['length']*1000) / (area*(df_sample['dp']/14.696))*1000000
    return df_sample

def sample_data_eor(current_sample):
    sample_start = pd.Timestamp.to_pydatetime(current_sample['Start Time'][0])
    sample_end = current_sample['End Time'][-1]
    if pd.isnull(sample_end):
        sample_end = datetime.datetime.utcnow()
    else :
        sample_end = pd.Timestamp.to_pydatetime(current_sample['End Time'][-1])

    params = [sample_start, sample_end]
    conn = sqlite3.connect('logs.db')
    df_sample_raw = pd.read_sql_query("SELECT * from COMBINED_EOR WHERE DateTime BETWEEN ? and ?", params = params, con=conn)
    df_sample_raw['DateTime'] = df_sample_raw['DateTime'].astype('datetime64[ns]')
    df_sample = pd.DataFrame()
    for i in range(len(current_sample['Start Time'])):
        #get time intervals
        interval_start = pd.Timestamp.to_pydatetime(current_sample['Start Time'][i])
        interval_end = current_sample['End Time'][i]

        if pd.isnull(interval_end):
            interval_end = datetime.datetime.utcnow()
        else :
            interval_end = pd.Timestamp.to_pydatetime(current_sample['End Time'][i])

        mask = (df_sample_raw['DateTime'] > interval_start) & (df_sample_raw['DateTime'] <= interval_end)
        df_sample.loc[mask, 'DateTime'] = df_sample_raw['DateTime']
        df_sample.loc[mask, 'Confining Pressure'] = df_sample_raw['EORPConf']
        df_sample.loc[mask, 'Upstream Pressure'] = df_sample_raw['EORUP']
        df_sample.loc[mask, 'Downstream Pressure'] = df_sample_raw['EORDOWN']
        if current_sample['Pump'][i] == 'ISCO':
            df_sample.loc[mask, 'Rate'] = df_sample_raw['ISCORate']
        else:
            df_sample.loc[mask, 'Rate'] = df_sample_raw['EORRate']
        df_sample.loc[mask, 'Comment'] = current_sample['Instance Comment'][i]
    #temp values for viscosity params
    a, b = visc(current_sample['fluid'], current_sample['temperature'])
    df_sample['dp'] = df_sample['Upstream Pressure'].astype(int) - df_sample['Downstream Pressure'].astype(int)
    df_sample['absdp'] = abs(df_sample['Upstream Pressure'].astype(int) - df_sample['Downstream Pressure'].astype(int))
    df_sample['qdp'] = df_sample['Rate']/df_sample['dp']
    df_sample['Viscosity'] = a*df_sample['Upstream Pressure'].astype(int)+b
    df_sample['Permeability'] = (df_sample['Rate']/60*df_sample['Viscosity']*current_sample['length']*1000) / (area*(df_sample['dp']/14.696))*1000000
    return df_sample

# #get sample info
# samplesheet = r"M:\Team Chaos Liquid Perm Initialization v5.xlsx"
# current_sample = sample(samplesheet, 2).sampleprop()
# df_current = sample_data(current_sample)
# fig = go.Figure([go.Scatter(x=df_current['DateTime'], y=df_current['Rate'])])
# fig.update_xaxes(rangeslider_visible=True)

# fig.show()

#print(df_temp.head(5))