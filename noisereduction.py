# import numpy as np
# import pandas as pd
# import matplotlib.pyplot as plt
# from scipy.signal import savgol_filter
# import statsmodels.api as sm
# import mysql.connector

# startdate = '2024-04-03 10:00:00'
# enddate = '2024-04-05 10:00:00'

# connection = mysql.connector.connect(host='localhost', database='water_level2', user='wtrsenusr', password='WtrSnsr@731928', auth_plugin='mysql_native_password')
# sql_query = "SELECT WaterLevel, DateTime FROM rawreadings WHERE DateTime BETWEEN %s AND %s ORDER BY DateTime"
# dataframe = pd.read_sql(sql_query, connection, params=(startdate, enddate))
# connection.close()

# waterlevel = dataframe['WaterLevel']
# time = pd.to_datetime(dataframe['DateTime'])  # Convert timestamp to datetime format
# plt.plot(time, waterlevel, 'r')
# w = savgol_filter(waterlevel, 81, 2)
# plt.plot(time, w, 'b')  # high frequency noise removed
# plt.xticks(rotation=45)
# plt.show()

# # y_lowess = sm.nonparametric.lowess(waterlevel, time, frac = 0.3)  # 30 % lowess smoothing

# # plt.plot(time, waterlevel, 'r')
# # plt.plot(y_lowess[:, 0], y_lowess[:, 1], 'b')  # some noise removed
# # plt.show()


# # n = 15 # the larger n is, the smoother curve will be
# # b = [1.0 / n] * n
# # a = 1
# # yy = lfilter(b, a, waterlevel)

# # plt.plot(time, waterlevel, 'r')
# # plt.plot(time, yy, 'b')  # high frequency noise removed

# # plt.show()







import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.signal import savgol_filter
import statsmodels.api as sm
import mysql.connector

startdate = '2024-04-03 10:00:00'
enddate = '2024-04-05 14:54:00'

connection = mysql.connector.connect(host='localhost', database='water_level2', user='wtrsenusr', password='WtrSnsr@731928', auth_plugin='mysql_native_password')
sql_query = "SELECT WaterLevel, DateTime FROM rawreadings WHERE DateTime BETWEEN %s AND %s ORDER BY DateTime"
sql_query2 = "SELECT WaterLevel, DateTime FROM waterlevel WHERE DateTime BETWEEN %s AND %s ORDER BY DateTime"
dataframe = pd.read_sql(sql_query, connection, params=(startdate, enddate))
dataframe2 = pd.read_sql(sql_query2, connection, params=(startdate, enddate))
connection.close()

# Extracting waterlevel and time from the dataframe
waterlevel = dataframe['WaterLevel']
time = pd.to_datetime(dataframe['DateTime'])  # Convert timestamp to datetime format

waterlevel2 = dataframe2['WaterLevel']
time2 = pd.to_datetime(dataframe2['DateTime'])  # Convert timestamp to datetime format

# Iterate over every 60 values, sort them, take the first value, and save its corresponding time
smoothed_waterlevels = []
smoothed_times = []
for i in range(0, len(waterlevel), 60):
    chunk = waterlevel[i:i+60]  # Extracting a chunk of 60 values
    sorted_chunk = sorted(chunk)  # Sorting the chunk in ascending order
    smoothed_waterlevels.append(sorted_chunk[-1])  # Taking the first value
    smoothed_times.append(time[i])  # Saving its corresponding time

# Plotting the smoothed water levels
w = savgol_filter(smoothed_waterlevels, 81, 2)


# Plotting as subplots
fig, axs = plt.subplots(4, 1, figsize=(10, 15), sharex=True)

# Plot Orgiginal readings
axs[1].plot(time2, waterlevel2, 'r')
axs[1].set_title('Median Readings')
axs[1].set_ylabel('Water Level')

# Plot raw readings
axs[0].plot(time, waterlevel, 'r')
axs[0].set_title('Raw Readings')
axs[0].set_ylabel('Water Level')

# Plot max readings
axs[2].plot(smoothed_times, smoothed_waterlevels, 'g')
axs[2].set_title('Max Readings')
axs[2].set_ylabel('Water Level')

# Plot filtered readings
axs[3].plot(smoothed_times, w, 'b')
axs[3].set_title('Filtered Readings')
axs[3].set_ylabel('Water Level')
axs[3].set_xlabel('Time')
plt.xticks(rotation=45)

plt.tight_layout()
plt.show()

# w = savgol_filter(smoothed_waterlevels, 81, 2)
# plt.plot(smoothed_times, w, 'b')  # high frequency noise removed
# plt.xticks(rotation=45)
# plt.show()
