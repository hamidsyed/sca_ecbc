import mysql.connector
import paho.mqtt.client as mqtt
import pandas as pd
MQTT_Broker = "192.168.0.121"
MQTT_Port = 1883
MQTT_Topic = "rawreadings2"
Keep_Alive_Interval = 60
global_count = 0
def on_message(client, userdata, message):
    connection = mysql.connector.connect(host='localhost',database='water_level2',user='wtrsenusr',
                                   password='WtrSnsr@731928',auth_plugin='mysql_native_password')
    global global_count 
    global_count = global_count + 1
    print("Message Received ")
    print('Message count = %d, Inserting in DB'%(global_count))
    msg = str(message.payload.decode("utf-8"))
    cnt = msg.count(",")
    for i in range (cnt):
        level = (msg.split(",")[i]).split(" ")[0]
        datetime =  (msg.split(",")[i]).split(" ")[1] + " " + (msg.split(",")[i]).split(" ")[2]
        sql_insert_query = "INSERT INTO rawreadings(WaterLevel, DateTime) VALUES(%s, %s)"
        cursor = connection.cursor()
        cursor.execute(sql_insert_query, (level, datetime))
        connection.commit()
        cursor.close()

    sql_data_query = "SELECT WaterLevel,DateTime from rawreadings ORDER BY DateTime DESC LIMIT 60"
    data = pd.read_sql(sql_data_query, connection)
    arr = data["WaterLevel"].values
    time = data["DateTime"].values
    sql_insert_query2 = "INSERT INTO waterlevel2(WaterLevel, DateTime) VALUES(%s, %s)"
    cursor = connection.cursor()
    cursor.execute(sql_insert_query2, (max(arr), time.astype(str)[0]))
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
