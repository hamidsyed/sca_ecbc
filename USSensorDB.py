import mysql.connector
import paho.mqtt.client as mqtt
MQTT_Broker = "192.168.0.121"
MQTT_Port = 1883
MQTT_Topic = "test"
Keep_Alive_Interval = 60
global_count = 0;
def on_message(client, userdata, message):
    connection = mysql.connector.connect(host='localhost',database='water_level',user='wtrsenusr',
                                   password='WtrSnsr@731928',auth_plugin='mysql_native_password')
    cursor = connection.cursor()
    global global_count 
    global_count = global_count + 1
    #print("Message Received ")
    msg = str(message.payload.decode("utf-8"))
    level = msg.split(" ")[0]
    level = int(level)
    datetime = msg.split(" ")[1] + " " + msg.split(" ")[2]
    invaliddata = msg.split(" ")[3]
    invaliddata = int(invaliddata)
    sql_insert_query = "INSERT INTO waterlevel(WaterLevel, DateTime, InvalidData) VALUES(%s, %s, %s)"
    print('Message count = %d, Inserting in DB'%(global_count))
    cursor.execute(sql_insert_query, (level, datetime, invaliddata))
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
