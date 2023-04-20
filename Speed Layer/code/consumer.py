import pandas as pd
import numpy as np
import tensorflow as tf
import json
from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
from config import *

mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['IS3107_Project']
mongo_coll = mongo_db['stream_data']


def process_payload(data: list, columns: list) -> pd.DataFrame:
    """
    Process the value of each message in the Kafka stream
    :param data: Kafka message.value
    :param columns: Columns we require from the data to engineer our features
    :return: Dataframe containing our raw features for prediction
    """
    df = pd.DataFrame(data)[columns]

    return df


def prepare_features(df: pd.DataFrame) -> tuple[dict[str, tf.Tensor], pd.DataFrame]:
    """
    Function to:
        - Compute required features
        - clean the raw features
    :param df: Raw dataframe containing columns required to compute final features for input
    :return: Dictionary of final features (key -> feature name, value -> feature tensor)
    """
    new_df, res = df.copy(deep=True), dict()
    dummy_string = 'null'
    for f in FEATURE_NAMES:
        if f == 'airline_iata':
            f_ser = new_df[f]
            f_ser.fillna(dummy_string, inplace=True)
            new_df[f].fillna(dummy_string, inplace=True)
            res[f] = tf.convert_to_tensor(f_ser.to_numpy())
        elif f == 'arr_desc':
            f_ser = new_df['arr_weather_description']
            f_ser.fillna(dummy_string, inplace=True)
            new_df['arr_weather_description'].fillna(dummy_string, inplace=True)
            res[f] = tf.convert_to_tensor(f_ser.to_numpy())
        elif f == 'arr_iata':
            f_ser = new_df[f]
            f_ser.fillna(dummy_string, inplace=True)
            new_df[f].fillna(dummy_string, inplace=True)
            res[f] = tf.convert_to_tensor(f_ser.to_numpy())
        elif f == 'dep_desc':
            f_ser = new_df['dep_weather_description']
            f_ser.fillna(dummy_string, inplace=True)
            new_df['dep_weather_description'].fillna('few clouds', inplace=True)
            res[f] = tf.convert_to_tensor(f_ser.to_numpy())
        elif f == 'dep_iata':
            f_ser = new_df[f]
            f_ser.fillna(dummy_string, inplace=True)
            new_df[f].fillna(dummy_string, inplace=True)
            res[f] = tf.convert_to_tensor(f_ser.to_numpy())
        elif f == 'dep_time_float':  # Hour of dep_time_utc * 60 + Minute of dep_time_utc
            f_ser = new_df['dep_time_utc']
            f_ser.fillna(0, inplace=True)
            new_df['dep_time_utc'].fillna(0, inplace=True)
            f_arr = f_ser.to_numpy()
            f_arr = [float(t[11:13]) * 60 + float(t[14:16]) for t in f_arr]
            res[f] = tf.cast(f_arr, tf.double)
        elif f == 'duration':
            f_ser = new_df[f]
            f_ser.fillna(0, inplace=True)
            new_df[f].fillna(0, inplace=True)
            res[f] = tf.cast(f_ser.to_numpy(), tf.double)

    print('Features prepared')

    return res, new_df


def fetch_prediction_model(path: str) -> tf.saved_model.load:
    """
    Load our latest logistic regression model for predicting new values
    :param path: Location of where model information is stored
    :return: Pre-trained model for making predictions
    """
    loaded = tf.saved_model.load(path)
    inferred = loaded.signatures['serving_default']

    # print(f'Tensorflow model fetched: {inferred}')

    return inferred


def make_predictions(
        model: tf.saved_model.load,
        features: dict[str, tf.Tensor]
) -> np.array:
    """
    Make predictions using the latest model and features
    :param model: Latest logistic regression model
    :param features: Cleaned features
    :return: Prediction for each flight on whether they'll be delayed
    """

    predictions = model(
        airline_iata=features['airline_iata'],
        arr_desc=features['arr_desc'],
        arr_iata=features['arr_iata'],
        dep_desc=features['dep_desc'],
        dep_iata=features['dep_iata'],
        dep_time_float=features['dep_time_float'],
        duration=features['duration']
    )['target_probs'].numpy()

    predictions = predictions.transpose()[0]

    print('Predictions made')

    return predictions


def merge_results(df: pd.DataFrame, predictions: np.array) -> pd.DataFrame:
    """
    Join our latest predictions with our original features
    :param df: Dataframe containing features used for prediction
    :param predictions: Output of our ML model
    :return: A merged dataframe of the above 2 inputs
    """
    new_df = pd.concat([df, pd.Series(predictions)], axis=1)

    new_df.rename(columns={0: 'delay_prob'}, inplace=True)

    print('Results merged')

    return new_df


def export_data_to_mongo(data: pd.DataFrame):
    """
    Push data to our MongoDB collection
    :param data: Dataframe to be uploaded
    """
    json_data = data.to_dict(orient='records')
    for doc in json_data:
        mongo_coll.insert_one(doc)


# create Kafka Consumer object
consumer = KafkaConsumer(
    'data-streams',
    bootstrap_servers=['localhost:9092'],
    api_version=(0, 10, 1),
    value_deserializer=lambda m: json.loads(m.decode('ascii'))
)

print('========== Ready to begin processing data in the Kafka stream ========== ')
for message in consumer:

    # get the value of the message in our Kafka stream
    payload = message.value
    # generate the raw features for prediction
    raw_features = process_payload(payload, COLS)
    # clean our features to ensure they're ready to be passed into the model
    cleaned_features, cleaned_df = prepare_features(raw_features)
    # load our latest pre-trained saved_model with TensorFlow
    latest_model = fetch_prediction_model(MODEL_PATH)
    # make predictions for the data from the message in the Kafka stream
    new_predictions = make_predictions(latest_model, cleaned_features)
    # combine our results with our original df
    merged_results = merge_results(cleaned_df, new_predictions)
    # merged_results.to_excel('sample_output.xlsx')
    # send data to our MondoDB Atlas database for real-time Tableau data visualisations
    export_data_to_mongo(merged_results)

    print('=============== Round done ================')
    print('======== Moving on to next message ========')
