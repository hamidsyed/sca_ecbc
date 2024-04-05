import mysql.connector
import paho.mqtt.client as mqtt
from scipy.signal import savgol_filter
import pandas as pd
import time as tm

MQTT_Broker = "192.168.0.121"
MQTT_Port = 1883
MQTT_Topic = "rawreadings2"
Keep_Alive_Interval = 60
global_count = 0


def sav_gol_fil(data):
    w = savgol_filter(data, 81, 2)
    return w[0]

def on_message(client, userdata, message):
    tm.sleep(3)
    connection = mysql.connector.connect(host='localhost',database='water_level2',user='wtrsenusr',
                                   password='WtrSnsr@731928',auth_plugin='mysql_native_password')

    sql_data_query = "SELECT WaterLevel,DateTime from waterlevel2 ORDER BY DateTime DESC LIMIT 100"
    data = pd.read_sql(sql_data_query, connection)
    arr = data["WaterLevel"].values
    time = data["DateTime"].values
    fil_data = sav_gol_fil(arr)
    sql_insert_query = "INSERT INTO filteredwaterlevel(FilteredWaterLevel, DateTime) VALUES(%s, %s)"
    cursor = connection.cursor()
    cursor.execute(sql_insert_query, (fil_data, time.astype(str)[0]))  # Corrected this line
    connection.commit()
    cursor.close()
    connection.close()

    # print(fil_data)
    # print(time.astype(str)[0])

def on_connect(client, userdata, flags, rc): 
    print("Subscribing to Topic: ",MQTT_Topic)
    client.subscribe(MQTT_Topic)

client = mqtt.Client(clean_session=True) #create new instance
client.on_connect = on_connect
client.on_message = on_message #attach function to callback

print("Connecting to Broker Topic1")
client.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval)) #connect to broker
client.loop_forever() #stop the loop
