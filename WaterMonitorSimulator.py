import paho.mqtt.client as mqtt
import random as rand
import time
import datetime

MQTT_Broker_1 = "192.168.0.121"
MQTT_Broker_2 = "192.168.0.103"
MQTT_2_Username = "watersensor"
MQTT_2_Password = "watersensor#8527"
MQTT_Port = 1883
MQTT_Topic = "test"
client_1 = mqtt.Client("")
client_2 = mqtt.Client("")
client_2.username_pw_set(MQTT_2_Username,MQTT_2_Password)


# client.connect(MQTT_Broker_1, MQTT_Port)
max_val = 740
min_val = 307
value = 307
water_fill_flag = False
list_day_week = [0, 1, 2, 3]
list_night_week = [0, 1]
day_time_flag = True
message = ""
weekend_flag = False

while True:
    time_now = datetime.datetime.now()
    weekno = datetime.datetime.today().weekday()
    if time_now.hour >= 8 :
        day_time_flag = True
    else: 
        day_time_flag = False
    if weekno < 5:
        weekend_flag = False
    else:
        weekend_flag = True
    while water_fill_flag == False and day_time_flag == True:
        time_now = datetime.datetime.now()
        if time_now.hour >= 8 :
            day_time_flag = True
        else: 
            day_time_flag = False
        weekno = datetime.datetime.today().weekday()
        if weekno < 5:
            weekend_flag = False
            value = value + rand.choices(list_day_week, weights=(35, 30, 25, 10), k=1)[0]
        else:
            weekend_flag = True
            value = value + rand.choices(list_day_week, weights=(55, 25, 10, 10), k=1)[0]
        if value >= max_val:
            water_fill_flag = True
        message = str(value) + " " + time_now.strftime("%Y-%m-%d %H:%M:%S") + " 0"
        client_1.connect(MQTT_Broker_1,MQTT_Port)
        client_1.publish(MQTT_Topic, message)
        client_1.disconnect()
        client_2.connect(MQTT_Broker_2,MQTT_Port)
        client_2.publish(MQTT_Topic, message)
        client_2.disconnect()
        time.sleep(60)
    while water_fill_flag == True:
        time_now = datetime.datetime.now()
        if time_now.hour >= 8 :
            day_time_flag = True
        else: 
            day_time_flag = False
        value = value - rand.randint(16,18)
        if value <= min_val:
            water_fill_flag = False
        message = str(value) + " " + time_now.strftime("%Y-%m-%d %H:%M:%S") + " 0"
        client_1.connect(MQTT_Broker_1,MQTT_Port)
        client_1.publish(MQTT_Topic, message)
        client_1.disconnect()
        client_2.connect(MQTT_Broker_2,MQTT_Port)
        client_2.publish(MQTT_Topic, message)
        client_2.disconnect()
        time.sleep(60)
    while water_fill_flag == False and day_time_flag == False:
        time_now = datetime.datetime.now()
        if time_now.hour >= 8 :
            day_time_flag = True
        else: 
            day_time_flag = False
        value = value + rand.choices(list_night_week, weights=(80, 20), k=1)[0]
        time.sleep(0.5)
        if value >= max_val:
            water_fill_flag = True
        message = str(value) + " " + time_now.strftime("%Y-%m-%d %H:%M:%S") + " 0"
        client_1.connect(MQTT_Broker_1,MQTT_Port)
        client_1.publish(MQTT_Topic, message)
        client_1.disconnect()
        client_2.connect(MQTT_Broker_2,MQTT_Port)
        client_2.publish(MQTT_Topic, message)
        client_2.disconnect()
        time.sleep(60)