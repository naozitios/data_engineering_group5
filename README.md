# IS3107 Data Engineering AY22/23 | Group 5
# Batch Layer

## Setting up Airflow connection to Google BigQuery:
Follow the steps in https://cloud.google.com/composer/docs/how-to/managing/connections, under the "Creating new Airflow connections" header

Note:

	- When creating a new connection in airflow GUI, fill up Conn Id as "is3107_connection" instead of "my_gcp_connection"

## Setting up the Google Authentication with the cloud bucket:
Get the JSON credential file for authentication and set the file path in "os.environ["GOOGLE_APPLICATION_CREDENTIALS"]"

Make sure that your account has Admin role

Setting up Google Bigquery Tables\n
project_id = 'is3107-380701'
dataset_id = 'data'
table_ids = ['flight_delay', 'weather_data']

## 'flight_delay' table in Google Bigquery
Source URL: gs://gcf-sources-769425010528-us-central1/flight_delay.csv

Schema:

| Field | Type | Nullable |
| --- | --- | --- |
| aircraft_icao | STRING | NULLABLE |
| airline_iata | STRING | NULLABLE | 		
| airline_icao | STRING | NULLABLE |			
| arr_actual | STRING | NULLABLE |		
| arr_actual_ts | STRING | NULLABLE |		
| arr_actual_utc | STRING | NULLABLE			
| arr_baggage | STRING | NULLABLE |		
| arr_city | STRING | NULLABLE |		
| arr_delayed | STRING | NULLABLE |			
| arr_estimated | STRING | NULLABLE |			
| arr_estimated_ts | STRING | NULLABLE |		
| arr_estimated_utc | STRING | NULLABLE	|		
| arr_gate | STRING | NULLABLE |		
| arr_iata | STRING | NULLABLE |		
| arr_icao | STRING | NULLABLE |		
| arr_terminal | STRING | NULLABLE |		
| arr_time | STRING | NULLABLE |			
| arr_time_ts | STRING | NULLABLE |			
| arr_time_utc | STRING	| NULLABLE |			
| arr_time_utc_3hr | STRING | NULLABLE |		
| cs_airline_iata | STRING | NULLABLE |		
| cs_flight_iata | STRING | NULLABLE |		
| cs_flight_number | STRING | NULLABLE |		
| delayed | FLOAT | NULLABLE |			
| dep_actual | STRING | NULLABLE |			
| dep_actual_ts | STRING | NULLABLE |			
| dep_actual_utc | STRING | NULLABLE |			
| dep_city | STRING | NULLABLE |			
| dep_delayed | STRING | NULLABLE |			
| dep_estimated | STRING | NULLABLE |			
| dep_estimated_ts | STRING | NULLABLE |		
| dep_estimated_utc | STRING | NULLABLE |		
| dep_gate | STRING | NULLABLE |		
| dep_iata | STRING | NULLABLE |		
| dep_icao | STRING | NULLABLE |		
| dep_terminal | STRING | NULLABLE |		
| dep_time | STRING | NULLABLE |			
| dep_time_ts | STRING | NULLABLE |		
| dep_time_utc | TIMESTAMP | NULLABLE |		
| dep_time_utc_3hr | STRING | NULLABLE |	
| duration | FLOAT |NULLABLE |		
| flight_iata | STRING | NULLABLE |		
| flight_icao | STRING | NULLABLE |		
| flight_number | STRING | NULLABLE |		
| outlier | INTEGER | NULLABLE |	
| status | STRING | NULLABLE |

## 'weather_data' table in Google Bigquery

Source URL: gs://gcf-sources-769425010528-us-central1/weather.csv

Schema:

| Field | Type | Nullable |
| --- | --- | --- |
| city | STRING | NULLABLE |			
| clouds_all | STRING | NULLABLE |			
| dt | STRING | NULLABLE |			
| dt_txt | STRING | NULLABLE |			
| main_feels_like | STRING | NULLABLE |			
| main_grnd_level | STRING | NULLABLE |		
| main_humidity | STRING | NULLABLE |			
| main_pressure | STRING | NULLABLE |			
| main_sea_level | STRING | NULLABLE |			
| main_temp |  STRING | NULLABLE |		
| main_temp_kf | STRING | NULLABLE |			
|main_temp_max | STRING | NULLABLE |			
|main_temp_min | STRING | NULLABLE |			
|pop | STRING | NULLABLE |	
|sys_pod | STRING | NULLABLE |			
|visibility |  STRING | NULLABLE |			
|weather_description | STRING | NULLABLE |			
|weather_icon | STRING | NULLABLE |		
|weather_id | STRING | NULLABLE |		
|weather_main | STRING | NULLABLE |			
|wind_deg | STRING | NULLABLE |			
|wind_gust | STRING | NULLABLE |			
|wind_speed | STRING | NULLABLE |

## Setting up the Google Cloud Bucket

URL for export model: 'gs://deploy_models/model'

URL for flight_delay to be inserted: 'gs://gcf-sources-769425010528-us-central1/flight_delay.csv'

URL for weather_data to be inserted: 'gs://gcf-sources-769425010528-us-central1/weather.csv'

## Setting up API Keys to extract data

Flight_Delay: https://airlabs.co/signin

Weather_Data: https://openweathermap.org/home/sign_in

# Speed Layer

## Setting Up Kafka

Site references:<br>
https://kafka.apache.org/quickstart#quickstart_createtopic <br>
https://medium.com/the-research-nest/creating-a-real-time-data-stream-using-apache-kafka-in-python-132e0e5630d3 <br>

### Setting up streaming data 
1. Download Kafka as per first site reference, pip install necessary dependencies
2. Go to location where Kafka is installed
3. Run zookeeper in terminal: ```bin/zookeeper-server-start.sh config/zookeeper.properties```
4. Run Kafka in a new terminal: ```bin/kafka-server-start.sh config/server.properties```
5. If a you want to create a new topic run this in a new terminal: <br>
```bin/kafka-topics.sh --create --topic TOPIC_NAME --bootstrap-server localhost:9092```
6. Run ```producer.py``` to start collecting data and sending it to the kafka server <br>
7. Run ```consumer.py``` to start retrieving data from kafka server

## How our Speed Layer works
• Run producer.py and consumer.py simultaneously to generate, process and make predictions on our stream data <br>
• ```producer.py``` file works by generating our data and sending them to the relevant Kafka topic <br>
• ```consumer.py``` reads this data and performs predictions on it with our latest pre-trained logistic regression model <br>
• This data is then sent to Tableau for visualisation purposes using a MongoDB connector <br>

# Tableau Dashboard
## Some Notes
• Link to dashboard: [https://prod-apsoutheast-a.online.tableau.com/#/site/is3107group5/workbooks/232318/views](https://prod-apsoutheast-a.online.tableau.com/#/site/is3107group5/workbooks/232318/views) <br>
• Desktop copy (.twbx file) requires access to Google BigQuery account (see above) <br>
• Desktop copy (.twbx file) requires user to setup a MongoDB connector within Tableau in order for our stream data to be visualised <br>
• For ease of use, we have supplied a ```sample_data.xlsx``` to be read by Tableau to replicate the same output generated and tested by our Speed Layer <br>
• Ensure this Excel file's path is set correctly within Tableau's data sources <br>
