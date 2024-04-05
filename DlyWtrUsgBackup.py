import pandas as pd
import mysql.connector as mc
from datetime import date, timedelta, datetime
import matplotlib.pyplot as plt
prevdate = date.today() - timedelta(days=1)
start_timestamp = datetime.combine(prevdate, datetime.min.time())
end_timestamp = datetime.combine(prevdate, datetime.max.time())
# print(start_timestamp)
# print(end_timestamp)
connection = mc.connect(host='localhost', database='water_level', user='wtrsenusr', password='WtrSnsr@731928', auth_plugin='mysql_native_password')
sql_query0 = "SELECT * FROM dailywaterusage WHERE Date = %s"
cursor = connection.cursor()
cursor.execute(sql_query0, (prevdate,))
result = cursor.fetchone()
connection.commit()
cursor.close()
connection.close()
if result is None:
    connection = mc.connect(host='localhost', database='water_level', user='wtrsenusr', password='WtrSnsr@731928', auth_plugin='mysql_native_password')
    sql_query1 = "SELECT WaterLevel, DateTime FROM waterlevel WHERE DateTime BETWEEN %s AND %s ORDER BY DateTime"
    data = pd.read_sql(sql_query1, connection, params=(start_timestamp, end_timestamp))
    connection.close()
    data["WaterLevel"] = data["WaterLevel"]*(-1.9) +2560.5
    data["Difference"] = data["WaterLevel"].diff()
    data["MotorStatus"] = data['Difference'].apply(lambda x: 1 if x > 6 else 0)
    # data.to_csv("testnew.csv", index=False)
    motoroff = (data['MotorStatus'] - data['MotorStatus'].shift(1)).lt(0).sum()
    # print(f"The motor turned OFF {motoroff} times.")
    motoron = (data['MotorStatus'] - data['MotorStatus'].shift(1)).gt(0).sum()
    # print(f"The motor turned ON {motoron} times.")
    dlywtrusg = (data[data['Difference'] < 2]['Difference'].sum())*(-1)
    # print(f"Total Water Usage of Previous Day: {dlywtrusg} Litres")
    motorruntime = (data[data['MotorStatus'] == 1]['MotorStatus'].sum())*1.083
    # print(f"The sum of 'MotorStatus' when it is equal to 1 is: {motorruntime}")
    connection = mc.connect(host='localhost', database='water_level', user='wtrsenusr', password='WtrSnsr@731928', auth_plugin='mysql_native_password')
    cursor = connection.cursor()
    sql_query2 = "INSERT INTO dailywaterusage(Date, WaterUsage,MotorOff,MotorOn,MotorRunTime) VALUES(%s,%s,%s,%s,%s)"
    cursor.execute(sql_query2, (prevdate, dlywtrusg,int(motoroff),int(motoron),motorruntime))
    connection.commit()
    cursor.close()
    connection.close()

