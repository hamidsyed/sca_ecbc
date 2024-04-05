import pandas as pd
import mysql.connector as mc
from datetime import date, timedelta, datetime

timenow = datetime.now()
timenowstr = timenow.strftime("%Y-%m-%d %H")
timenowstr = timenowstr + ":00:00"
reducedtime = timenow - timedelta(hours=2)
reducedtimestr = reducedtime.strftime("%Y-%m-%d %H")
reducedtimestr =reducedtimestr + ":59:00"
todaydate = timenow.strftime("%Y-%m-%d")
colname = "Hr" + str(timenow.hour)
# print(timenowstr)
# print(reducedtimestr)

connection = mc.connect(host='124.123.18.67', database='water_level', user='rmuser', password='rmpassword', auth_plugin='mysql_native_password')
sql_query0 = "SELECT WaterLevel, DateTime FROM waterlevel WHERE DateTime BETWEEN %s AND %s ORDER BY DateTime"
data = pd.read_sql(sql_query0, connection,params=(reducedtimestr,timenowstr))
connection.close()
data["WaterLevel"] = data["WaterLevel"]*(-1.9) +2560.5
data["Difference"] = data["WaterLevel"].diff()
data = data.drop(data.index[0])
hrlywtrusg = (data[data['Difference'] < 6]['Difference'].sum())*(-1)

connection = mc.connect(host='124.123.18.67', database='water_level', user='rmuser', password='rmpassword', auth_plugin='mysql_native_password')
sql_query0 = "SELECT Date FROM hourlywaterusage WHERE Date = %s"
cursor = connection.cursor()
cursor.execute(sql_query0, (todaydate,))
result = cursor.fetchone()
connection.commit()
cursor.close()
connection.close()
if result is None:
    connection = mc.connect(host='124.123.18.67', database='water_level', user='rmuser', password='rmpassword', auth_plugin='mysql_native_password')
    sql_query1 = f"INSERT INTO hourlywaterusage(Date, {colname}) VALUES(%s, %s)"
    cursor = connection.cursor()
    cursor.execute(sql_query1, (todaydate, hrlywtrusg))
    connection.commit()
    cursor.close()
    connection.close()
else:
    connection = mc.connect(host='124.123.18.67', database='water_level', user='rmuser', password='rmpassword', auth_plugin='mysql_native_password')
    sql_query3 = f"UPDATE hourlywaterusage SET {colname} = %s WHERE Date = %s"
    cursor = connection.cursor()
    cursor.execute(sql_query3, (hrlywtrusg, todaydate))
    result2 = cursor.fetchone()
    connection.commit()
    cursor.close()
    connection.close()
