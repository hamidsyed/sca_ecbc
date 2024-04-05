import mysql.connector
import paho.mqtt.client as mqtt
from scipy.signal import savgol_filter
import pandas as pd

MQTT_Broker = "192.168.0.121"
MQTT_Port = 1883
MQTT_Topic = "test2"
Keep_Alive_Interval = 60
global_count = 0

def sav_gol_fil(data):
    w = savgol_filter(data, 81, 2)
    return w[-1]

def on_message(client, userdata, message):
    connection = mysql.connector.connect(host='localhost',database='water_level2',user='wtrsenusr',
                                   password='WtrSnsr@731928',auth_plugin='mysql_native_password')
    global global_count 
    global_count = global_count + 1
    #print("Message Received ")
    msg = str(message.payload.decode("utf-8"))
    level = msg.split(" ")[0]
    level = int(level)
    datetime = msg.split(" ")[1] + " " + msg.split(" ")[2]
    invaliddata = msg.split(" ")[3]
    invaliddata = int(invaliddata)
    sql_data_query = "SELECT WaterLevel from waterlevel ORDER BY DateTime DESC LIMIT 300"
    data = pd.read_sql(sql_data_query, connection)
    arr = data["WaterLevel"].values
    fil_data = sav_gol_fil(arr)
    cursor = connection.cursor()
    sql_insert_query = "INSERT INTO waterlevel(WaterLevel, FilteredWaterLevel, DateTime, InvalidData) VALUES(%s, %s, %s, %s)"
    print('Message count = %d, Inserting in DB'%(global_count))
    cursor.execute(sql_insert_query, (level, fil_data, datetime, invaliddata))
    connection.commit()
    cursor.close()
    connection.close()

def on_connect(client, userdata, flags, rc): 
    print("Subscribing to Topic: ",MQTT_Topic)
    client.subscribe(MQTT_Topic)

client = mqtt.Client(clean_session=True) #create new instance
client.on_connect = on_connect
client.on_message = on_message #attach function to callback

print("Connecting to Broker")
client.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval)) #connect to broker
client.loop_forever() #stop the loop
