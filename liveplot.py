import paho.mqtt.client as mqtt
import matplotlib.pyplot as plt
from collections import deque
from datetime import datetime as dt
import time

MQTT_Broker = "124.123.18.67"
MQTT_Port = 1883
MQTT_Topic = "test"
Keep_Alive_Interval = 60

waterlevel = deque(maxlen=10)
waterleveltimestamp = deque (maxlen=10)

def on_connect(client, userdata, flags, rc):
    print("Subscribing to Topic: ",MQTT_Topic)
    client.subscribe(MQTT_Topic)

def on_message(client, userdata, message):
    msg = str(message.payload.decode("utf-8"))
    level = msg.split(" ")[0]
    level = int(level)
    datetime = msg.split(" ")[1] + " " + msg.split(" ")[2]
    datetime_object = dt.strptime(datetime, "%Y-%m-%d %H:%M:%S")
    waterlevel.append(level)
    waterleveltimestamp.append(datetime_object)

# def plot_live_data():
#     plt.plot(waterleveltimestamp, waterlevel)
#     plt.xlabel("Time")
#     plt.ylabel("Value")
#     plt.title("Live MQTT Data")
#     plt.grid(True)
#     plt.pause(1)  # Pause for 1 second to update the plot
#     plt.clf()
#     plt.show()  # Clear the previous plot

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval)) #connect to broker
client.loop_forever()