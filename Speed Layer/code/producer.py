# https://kafka.apache.org/quickstart#quickstart_createtopic
import pandas as pd
from kafka import KafkaProducer
from time import sleep
import requests
import airportsdata  # added 30/3
import json
import time

#airbase api endppoint
flight_schedules_url = "https://airlabs.co/api/v9/schedules?dep_iata=MIA&api_key=cc9e3386-52e2-4bc6-aece-c4118ce60e00"

# Producing as JSON
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    api_version=(0, 10, 1),
    value_serializer=lambda m: json.dumps(m).encode('ascii')
)


def transform_data(json_file):
    def round_3hr(time):
        if pd.isna(time):
            return ""
        elif int(time[11:13]) % 3 == 0:
            return time[0:13] + ":00:00"
        elif int(time[11:13]) % 3 == 1:
            return time[0:11] + str(int(time[11:13]) - 1) + ":00:00"
        elif int(time[11:13]) % 3 == 2:
            if int(time[11:13]) + 1 == 24:
                return time[0:8] + str(int(time[8:10]) + 1) + " 00:00:00"
        else:
            return time[0:11] + str(int(time[11:13]) + 1) + ":00:00"

    # added 30/3
    def get_city(icao, airports):
        if icao in airports:
            return airports[icao]['city']
        else:
            return ""

    # added 30/3
    def fetch_weather_data(cities):
        data = list()
        api_key = '2c25571e697eef3a87e8fc46aee17124'

        for city in cities:
            api_url = f'http://api.openweathermap.org/data/2.5/forecast?q={city}&APPID={api_key}'
            response = requests.get(api_url)

            if response.status_code != 200:
                continue

            city_weather_data = response.json()['list']

            for entry in city_weather_data:
                row = dict()
    
                row['city'] = city
                row['dt'] = entry['dt']
    
                main = entry['main']
                row['main_temp'] = main['temp']
                row['main_feels_like'] = main['feels_like']
                row['main_temp_min'] = main['temp_min']
                row['main_temp_max'] = main['temp_max']
                row['main_pressure'] = main['pressure']
                row['main_sea_level'] = main['sea_level']
                row['main_grnd_level'] = main['grnd_level']
                row['main_humidity'] = main['humidity']
                row['main_temp_kf'] = main['temp_kf']
    
                weather = entry['weather'][0]
                row['weather_id'] = weather['id']
                row['weather_main'] = weather['main']
                row['weather_description'] = weather['description']
                row['weather_icon'] = weather['icon']
        
                clouds = entry['clouds']
                row['clouds_all'] = clouds['all']
        
                wind = entry['wind']
                row['wind_speed'] = wind['speed']
                row['wind_deg'] = wind['deg']
                row['wind_gust'] = wind['gust']
        
                row['visibility'] = entry['visibility']
        
                row['pop'] = entry['pop']
        
                sys = entry['sys']
                row['sys_pod'] = sys['pod']
        
                row['dt_txt'] = entry['dt_txt']
        
                data.append(row)

        weather_data = pd.DataFrame(data)
        return weather_data

    df = pd.DataFrame(json_file)
    df['dep_time_utc_3hr'] = df['dep_time_utc'].apply(lambda x: round_3hr(x))
    df['arr_time_utc_3hr'] = df['arr_time_utc'].apply(lambda x: round_3hr(x))

    # added 30/3
    airports = airportsdata.load()
    df['dep_city'] = df['dep_icao'].apply(lambda x: get_city(x, airports))
    df['arr_city'] = df['arr_icao'].apply(lambda x: get_city(x, airports))

    # added 30/3
    dep_cities = df['dep_city'].tolist()
    arr_cities = df['arr_city'].tolist()
    cities = dep_cities + arr_cities
    unique_cities = list(dict.fromkeys(cities))

    # added 30/3
    weather_data = fetch_weather_data(unique_cities)

    # added 30/3
    weather_data = weather_data.rename(columns={'city': 'dep_city'})
    weather_data = weather_data.rename(columns={'dt_txt': 'dep_time_utc_3hr'})

    # added 30/3
    merged_df = pd.merge(df, weather_data, on=['dep_city', 'dep_time_utc_3hr'], how='left')
    
    # added 30/3
    last_21_cols = merged_df.iloc[:, -21:].columns
    new_col_names = ['dep_' + col for col in last_21_cols]
    merged_df = merged_df.rename(columns=dict(zip(last_21_cols, new_col_names)))

    # added 30/3
    weather_data = weather_data.rename(columns={'dep_city': 'arr_city'})
    weather_data = weather_data.rename(columns={'dep_time_utc_3hr': 'arr_time_utc_3hr'})

    # added 30/3
    merged_df = pd.merge(merged_df, weather_data, on=['arr_city', 'arr_time_utc_3hr'], how='left')

    # added 30/3
    last_21_cols = merged_df.iloc[:, -21:].columns
    new_col_names = ['arr_' + col for col in last_21_cols]
    merged_df = merged_df.rename(columns=dict(zip(last_21_cols, new_col_names)))

    print('Data transformed')

    return merged_df.to_dict('records')


use_api = False  # turned off due to prevent over usage ...  we've been warned ...

while True:

    sleep(3)  # configure the speed of the stream data

    if use_api:
        flight_schedules_json = requests.get(flight_schedules_url).json()['response']
    else:
        with open('flight_schedules.json', 'r') as f:
            flight_schedules_json = json.load(f) 

    print('Data fetched')

    flight_schedules_json = transform_data(flight_schedules_json)

    start_time = time.time()
    producer.send('data-streams', flight_schedules_json)
    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f'Time Elapsed: {elapsed_time}')
    print('Data sent to consumer')
