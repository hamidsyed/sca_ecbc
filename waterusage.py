import warnings
import pandas as pd
import mysql.connector as mc
from datetime import date, timedelta, datetime
import json

warnings.filterwarnings("ignore")
timenow = datetime.now()
timenowstr = timenow.strftime("%Y-%m-%d %H:%M:%S")
reducedtime = timenow - timedelta(hours=24)
reducedtimestr = reducedtime.strftime("%Y-%m-%d %H:%M:%S")
motoron = 0
motoroff = 0
connection = mc.connect(host='localhost', database='water_level', user='wtrsenusr', password='WtrSnsr@731928', auth_plugin='mysql_native_password')
sql_query1 = "SELECT WaterLevel, UNIX_TIMESTAMP(DateTime) as DateTime FROM waterlevel WHERE DateTime BETWEEN %s AND %s ORDER BY DateTime"
data = pd.read_sql(sql_query1, connection,params=(reducedtimestr,timenowstr))
connection.close()
data["WaterLevel"] = data["WaterLevel"]*(-1.9) +2560.5
data["Difference"] = data["WaterLevel"].diff()
data["MotorStatus"] = data['Difference'].apply(lambda x: 1 if x > 6 else 0)
data = data.drop(data.index[0])
# data.to_csv("testnew.csv", index=False)
i = 0
while(i < len(data)):
    if(i > 5):
        if((data['MotorStatus'].iloc[i - 6] == 0) & (data['MotorStatus'].iloc[i - 5] == 0) & (data['MotorStatus'].iloc[i - 4] == 0) & (data['MotorStatus'].iloc[i - 3] == 0) & (data['MotorStatus'].iloc[i - 2] == 1) & (data['MotorStatus'].iloc[i - 1] == 1) & (data['MotorStatus'].iloc[i] == 1)):
            motoron = motoron + 1
        if((data['MotorStatus'].iloc[i - 6] == 1) & (data['MotorStatus'].iloc[i - 5] == 1) & (data['MotorStatus'].iloc[i - 4] == 1) & (data['MotorStatus'].iloc[i - 3] == 1) & (data['MotorStatus'].iloc[i - 2] == 0) & (data['MotorStatus'].iloc[i - 1] == 0) & (data['MotorStatus'].iloc[i] == 0)):
            motoroff = motoroff + 1
    i = i+1
#motoroff = (data['MotorStatus'] - data['MotorStatus'].shift(1)).lt(0).sum()
# print(f"The motor turned OFF {motoroff} times.")
#motoron = (data['MotorStatus'] - data['MotorStatus'].shift(1)).gt(0).sum()
# print(f"The motor turned ON {motoron} times.")
dlywtrusg = (data[data['Difference'] < 2]['Difference'].sum())*(-1)
# print(f"Total Water Usage of Previous Day: {dlywtrusg} Litres")
motorruntime = (data[data['MotorStatus'] == 1]['MotorStatus'].sum())*1.083
# print(f"The sum of 'MotorStatus' when it is equal to 1 is: {motorruntime}")
motorstat = data['MotorStatus'].iloc[-1]
idxleast = data['Difference'].idxmin()
lstdiff = data.loc[idxleast, 'Difference']*(-1)
lsttime = data.loc[idxleast, 'DateTime']
jsondataformat = {
    'Last24HrUsg' : float(dlywtrusg),
    'MaxWaterUsage' : float(lstdiff),
    'TimeStamp' : int(lsttime),
    'MotorStat' : int(motorstat),
	'MotorOnCount' : int(motoron),
    'MotorOFFCount' : int(motoroff),
	'MotorRunTime' : float(motorruntime)
}
json_data = json.dumps(jsondataformat)
print(json_data)
print(motoron)
print(motoroff)
# data.to_csv("testnew.csv", index=False)
