import pandas as pd
import math
import datetime
import csv
import sqlite3
import time


class logLoader:
    def __init__(self, dxd, isco, vindum, vindumNMR, EOR):
        self.dxd = dxd
        self.isco = isco
        self.vindum = vindum
        self.vindumNMR = vindumNMR
        self.EOR = EOR

    def test(self):
        return("hello world")

    def dxdLoader(self):
        df_dxd = pd.read_csv(self.dxd, skiprows=7)
        df_dxd.drop(df_dxd.columns[[3, 4, 6, 7, 9, 10, 12, 13, 15, 16,
                                18, 19, 21, 22, 24, 25, 27, 28, 30, 31]], axis=1, inplace=True)
        
        df_dxd.columns = ['Date', 'Time', 'Ext3Up', 'Ext3Down', 'Ext4Up',
                            'Ext4Down', 'SS1Up', 'SS1Down', 'SS2Up', 'SS2Down', 'DeadulusDown', 'DeadulusUp']
        df_dxd['DateTime'] = pd.to_datetime(df_dxd['Date'] + " " + df_dxd['Time'])
        df_dxd['DateTime'] = df_dxd['DateTime'].dt.round('30s')  
        df_dxd = df_dxd.dropna()
        df_dxd = df_dxd.sort_values(by='DateTime')
        df_dxd.drop(df_dxd.columns[[0,1]], axis=1, inplace=True)
        conn = sqlite3.connect('logs.db')
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS DXD (Ext3Up, Ext3Down, Ext4Up, Ext4Down, SS1Up, SS1Down, SS2Up, SS2Down, DeadulusDown, DeadulusUp, DateTime)')
        df_temp = pd.read_sql_query("SELECT * from dxd", conn)
        df_dxd = pd.concat([df_dxd, df_temp])
        df_dxd['DateTime'] = pd.to_datetime(df_dxd['DateTime'] ,errors='coerce')
        df_dxd = df_dxd.sort_values(by='DateTime')
        df_dxd = df_dxd.drop_duplicates(subset='DateTime', keep="first")
        df_dxd.to_sql('DXD', conn, if_exists='replace', index = False)

    def iscoLoader(self):
        df_isco = pd.read_csv(self.isco)
        df_isco.columns = df_isco.columns.str.replace('/', '')
        df_isco['DateTime'] = df_isco['DateTime'].str.replace('=', '')
        df_isco['DateTime'] = df_isco['DateTime'].str.replace('"', '')
        df_isco[pd.to_numeric(df_isco['DateTime'], errors='coerce').notnull()]
        isco_list = ["Pressure AB", "Flow Rate AB", "DateTime"]
        df_isco = df_isco[isco_list]
        df_isco['DateTime'] = pd.to_datetime(df_isco['DateTime'], errors='coerce') 
        df_isco.columns = ['ISCOPres', 'ISCORate', 'DateTime']
        df_isco['DateTime'] = pd.to_datetime(df_isco['DateTime'])
        df_isco['DateTime'] = df_isco['DateTime'].dt.round('30s') 
        conn = sqlite3.connect('logs.db')
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS ISCO (ISCOPres INTEGER, ISCORate INTEGER, DateTime TIMESTAMP)')
        df_temp = pd.read_sql_query("SELECT * from isco", conn)
        df_isco = pd.concat([df_isco, df_temp])
        df_isco['DateTime'] = pd.to_datetime(df_isco['DateTime'] ,errors='coerce')
        df_isco = df_isco.sort_values(by='DateTime')
        df_isco = df_isco.drop_duplicates(subset='DateTime', keep="first")
        df_isco.to_sql('ISCO', conn, if_exists='replace', index = False)

    def vindumLoader(self):
        df_vin = pd.read_csv(self.vindum, index_col=False)
        mainlist = ['Date', 'Time', 'P1 Press', 'P1 Rate', 'P2 Press',
                    'P2 Rate', 'P3 Press', 'P3 Rate', 'P4 Press', 'P4 Rate']
        df_vin = df_vin[[c for c in df_vin.columns if c in mainlist]]
        df_vin = df_vin[:-1]
        df_vin['DateTime'] = pd.to_datetime(df_vin['Date'] + " " + df_vin['Time'])
        df_vin['DateTime'] = pd.to_datetime(df_vin['DateTime'])
        df_vin['DateTime'] = df_vin['DateTime'].dt.round('30s')
        df_vin = df_vin.drop(['Date', 'Time'], axis=1)
        df_vin.columns = ['P1Pres', 'P1Rate', 'P2Pres','P2Rate', 'P3Pres', 'P3Rate', 'P4Pres', 'P4Rate', 'DateTime']
        df_vin = df_vin.dropna()
        df_vin = df_vin.sort_values(by='DateTime')
        conn = sqlite3.connect('logs.db')
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS VIN (P1Pres INTEGER, P1Rate INTEGER, P2Pres INTEGER, P2Rate INTEGER, P3Pres INTEGER, P3Rate INTEGER, P4Pres INTEGER, P4Rate INTEGER, DateTime TIMESTAMP)')
        df_temp = pd.read_sql_query("SELECT * from vin", conn)
        df_vin = pd.concat([df_vin, df_temp])
        df_vin['DateTime'] = pd.to_datetime(df_vin['DateTime'] ,errors='coerce')
        df_vin = df_vin.sort_values(by='DateTime')
        df_vin = df_vin.drop_duplicates(subset='DateTime', keep="first")
        df_vin.to_sql('VIN', conn, if_exists='replace', index = False)

    def vindumnmrLoader(self):
        df_vinnmr = pd.read_csv(self.vindumNMR)
        df_vinnmr = df_vinnmr.loc[:,['Date', 'Time', 'P1 Press', 'P1 Rate']]
        df_vinnmr.columns = ['Date', 'Time', 'P1NMRPres', 'P1NMRRate']
        df_vinnmr['DateTime'] = pd.to_datetime(df_vinnmr['Date'] + " " + df_vinnmr['Time'])
        df_vinnmr['DateTime'] = pd.to_datetime(df_vinnmr['DateTime'])
        df_vinnmr['DateTime'] = df_vinnmr['DateTime'].dt.round('30s') 
        df_vinnmr = df_vinnmr.drop(['Date', 'Time'], axis=1) 
        df_vinnmr = df_vinnmr.dropna()
        df_vinnmr = df_vinnmr.sort_values(by='DateTime')
        conn = sqlite3.connect('logs.db')
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS VINNMR (P1NMRPres INTEGER, P1NMRRate INTEGER, DateTime TIMESTAMP)')
        df_temp = pd.read_sql_query("SELECT * from VINNMR", conn)
        df_vinnmr = pd.concat([df_vinnmr, df_temp])
        df_vinnmr['DateTime'] = pd.to_datetime(df_vinnmr['DateTime'] ,errors='coerce')
        df_vinnmr = df_vinnmr.sort_values(by='DateTime')
        df_vinnmr = df_vinnmr.drop_duplicates(subset='DateTime', keep="first")
        df_vinnmr.to_sql('VINNMR', conn, if_exists='replace', index = False)

    def EORLoader(self):
        df_eor = pd.read_csv(self.EOR, skiprows=46,encoding='latin1', header = None)
        df_eor = df_eor.drop(df_eor.columns[[1, 3, 4, 5, 6, 7, 8, 9, 12, 13 ,16 ,17, 19, 21, 22, 23, 24,
                                            25, 26, 27 ,28, 29, 30, 34, 36, 37, 38, 39, 40, 41, 42, 
                                            43, 44, 45]], axis=1)
        df_eor.columns = ['DateTime', 'EORPConf', 'P1_injV', 'P1_injQ', 'P2_injV', 'P2_ingQ', 'EORUP',
                            'EORDOWN', 'EORVol', 'EORRate', 'EORDP', 'EORHES']
        df_eor = df_eor.set_index('DateTime')
        df_eor.index = pd.to_datetime(df_eor.index)
        df_eor = df_eor.resample('30s').pad()
        df_eor = df_eor.reset_index()
        df_eor = df_eor.dropna()
        df_eor = df_eor.sort_values(by='DateTime')
        conn = sqlite3.connect('logs.db')
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS EOR (DateTime TIMESTAMP, EORPConf INTEGER, P1_injV INTEGER, P1_injQ INTEGER, P2_injV INTEGER, P2_ingQ INTEGER, EORUP INTEGER, EORDOWN INTEGER, EORVol INTEGER, EORRate INTEGER, EORDP INTEGER, EORHES INTEGER)')
        df_temp = pd.read_sql_query("SELECT * from EOR", conn)
        df_eor = pd.concat([df_eor, df_temp])
        df_eor['DateTime'] = pd.to_datetime(df_eor['DateTime'] ,errors='coerce')
        df_eor = df_eor.sort_values(by='DateTime')
        df_eor = df_eor.drop_duplicates(subset='DateTime', keep="first")
        df_eor.to_sql('EOR', conn, if_exists='replace', index = False)

    def combined(self):
        #merge logs
        conn = sqlite3.connect('logs.db')
        df_dxd_temp = pd.read_sql_query("SELECT * from dxd", conn)
        df_dxd_temp['DateTime'] = pd.to_datetime(df_dxd_temp['DateTime'] ,errors='coerce')
        df_vin_temp = pd.read_sql_query("SELECT * from vin", conn)
        df_vin_temp['DateTime'] = pd.to_datetime(df_vin_temp['DateTime'] ,errors='coerce')
        df_vinnmr_temp = pd.read_sql_query("SELECT * from vinnmr", conn)
        df_vinnmr_temp['DateTime'] = pd.to_datetime(df_vinnmr_temp['DateTime'] ,errors='coerce')
        df_isco_temp = pd.read_sql_query("SELECT * from isco", conn)
        df_isco_temp['DateTime'] = pd.to_datetime(df_isco_temp['DateTime'] ,errors='coerce')
        df_eor_temp = pd.read_sql_query("SELECT * from EOR", conn)
        df_eor_temp['DateTime'] = pd.to_datetime(df_eor_temp['DateTime'] ,errors='coerce')
        df_com = pd.merge_asof(df_dxd_temp, df_vin_temp, on='DateTime')
        df_com = pd.merge_asof(df_com, df_vinnmr_temp, on='DateTime')
        df_com = pd.merge_asof(df_com, df_isco_temp, on='DateTime')
        df_com.to_sql('COMBINED', conn, if_exists='replace', index = False)
        df_com_eor = pd.merge_asof(df_eor_temp, df_isco_temp, on='DateTime')
        df_com_eor.to_sql('COMBINED_EOR', conn, if_exists='replace', index = False)

dxd = r"M:\DXD Log Files\DXD_Log_4_14_119pm.csv"
isco = r"M:\DXD Log Files\ISCO_Log_4_24_835am.csv"
vindum = r"M:\DXD Log Files\VindumPumpLog (Pump1-4) 4-14 115pm.csv"
vindumNMR = r"M:\VindumPumpLog_NMR Lperm.csv"
samplesheet = r"M:\Team Chaos Liquid Perm Initialization v2_1.xlsx"
eor = r"M:\live oil 4_28_2020 sep gas"
x = logLoader(dxd, isco, vindum, vindumNMR, eor)
start = time.time()
x.dxdLoader()
print('dxd complete')
x.iscoLoader()
print('isco complete')
x.vindumLoader()
print('vindum complete')
x.vindumnmrLoader()
print('vindumnmr complete')
x.EORLoader()
print('eor complete')

x.combined()
print('combine complete')
end = time.time()
print(end - start)

# schedule.every(5).minutes.do(x.dxdLoader())
# schedule.every(5).minutes.do(x.vindumLoader())
# schedule.every(5).minutes.do(x.iscoLoader())
# schedule.every(5).minutes.do(x.vindumnmrLoader())
# schedule.every(5).minutes.do(x.EORLoader())
# schedule.every(5).minutes.do(x.combined())
# while 1:
#     schedule.run_pending()
#     time.sleep(1)