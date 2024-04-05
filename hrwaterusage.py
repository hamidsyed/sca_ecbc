import pandas as pd
import mysql.connector as mc
from datetime import date, timedelta, datetime

timenow = datetime.now()
timenowstr = timenow.strftime("%Y-%m-%d %H")
timenowstr = timenowstr + ":00:00"
reducedtime = timenow - timedelta(hours=2)
reducedtimestr = reducedtime.strftime("%Y-%m-%d %H")
reducedtimestr =reducedtimestr + ":59:00"
# print(timenowstr)
# print(reducedtimestr)

connection = mc.connect(host='localhost', database='water_level', user='wtrsenusr', password='WtrSnsr@731928', auth_plugin='mysql_native_password')
sql_query0 = "SELECT WaterLevel, DateTime FROM waterlevel WHERE DateTime BETWEEN %s AND %s ORDER BY DateTime"
data = pd.read_sql(sql_query0, connection,params=(reducedtimestr,timenowstr))
connection.close()
data["WaterLevel"] = data["WaterLevel"]*(-1.9) +2560.5
data["Difference"] = data["WaterLevel"].diff()
data = data.drop(data.index[0])
hrlywtrusg = (data[data['Difference'] < 6]['Difference'].sum())*(-1)

connection = mc.connect(host='localhost', database='water_level', user='wtrsenusr', password='WtrSnsr@731928', auth_plugin='mysql_native_password')
sql_query0 = "SELECT DateTime FROM hrwaterusage WHERE DateTime = %s"
cursor = connection.cursor()
cursor.execute(sql_query0, (timenowstr,))
result = cursor.fetchone()
connection.commit()
cursor.close()
connection.close()
if result is None:
    connection = mc.connect(host='localhost', database='water_level', user='wtrsenusr', password='WtrSnsr@731928', auth_plugin='mysql_native_password')
    sql_query1 = "INSERT INTO hrwaterusage(DateTime, WaterUsage) VALUES(%s, %s)"
    cursor = connection.cursor()
    cursor.execute(sql_query1, (timenowstr, hrlywtrusg))
    connection.commit()
    cursor.close()
    connection.close()
