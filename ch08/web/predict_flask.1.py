import sys, os, re
from flask import Flask, render_template, request
from pymongo import MongoClient
from bson import json_util

# Configuration details
import config

# Helpers for search and prediction APIs
import predict_utils

# Set up Flask, Mongo and Elasticsearch
app = Flask(__name__)

client = MongoClient()

from pyelasticsearch import ElasticSearch
elastic = ElasticSearch(config.ELASTIC_URL)

import json

# Date/time stuff
import iso8601
import datetime

# Setup Kafka
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0,10))
PREDICTION_TOPIC = 'flight_delay_classification_request'

import uuid

# Make our API a post, so a search engine wouldn't hit it
@app.route("/flights/delays/predict/classify_realtime", methods=['POST'])
def classify_flight_delays_realtime():
  """POST API for classifying flight delays"""
  
  # Define the form fields to process
  api_field_type_map = \
    {
      "DepDelay": float,
      "Carrier": str,
      "FlightDate": str,
      "Dest": str,
      "FlightNum": str,
      "Origin": str
    }

  # Fetch the values for each field from the form object
  api_form_values = {}
  for api_field_name, api_field_type in api_field_type_map.items():
    api_form_values[api_field_name] = request.form.get(api_field_name, type=api_field_type)
  
  # Set the direct values, which excludes Date
  prediction_features = {}
  for key, value in api_form_values.items():
    prediction_features[key] = value
  
  print api_form_values['Origin']
  print api_form_values['Dest']
  
  # Set the derived values
  prediction_features['Distance'] = predict_utils.get_flight_distance(
    client, api_form_values['Origin'],
    api_form_values['Dest']
  )
  print prediction_features['Distance']
  # # Turn the date into DayOfYear, DayOfMonth, DayOfWeek
  # date_features_dict = predict_utils.get_regression_date_args(
  #   api_form_values['FlightDate']
  # )
  # for api_field_name, api_field_value in date_features_dict.items():
  #   prediction_features[api_field_name] = api_field_value
  
  # # Add a timestamp
  # prediction_features['Timestamp'] = predict_utils.get_current_timestamp()
  
  # # Create a unique ID for this message
  # unique_id = str(uuid.uuid4())
  # prediction_features['UUID'] = unique_id
  
  # message_bytes = json.dumps(prediction_features).encode()
  # producer.send(PREDICTION_TOPIC, message_bytes)

  # response = {"status": "OK", "id": unique_id}
  # return json_util.dumps(response)
  return "done"

# Make our API a post, so a search engine wouldn't hit it
@app.route("/flights/delays/predict/regress", methods=['POST'])
def regress_flight_delays():
  
  api_field_type_map = \
    {
      "DepDelay": int,
      "Carrier": str,
      "FlightDate": str,
      "Dest": str,
      "FlightNum": str,
      "Origin": str
    }
  
  api_form_values = {}
  for api_field_name, api_field_type in api_field_type_map.items():
    api_form_values[api_field_name] = request.form.get(api_field_name, type=api_field_type)
  
  # Set the direct values
  prediction_features = {}
  prediction_features['Origin'] = api_form_values['Origin']
  prediction_features['Dest'] = api_form_values['Dest']
  prediction_features['FlightNum'] = api_form_values['FlightNum']
  
  # Set the derived values
  prediction_features['Distance'] = predict_utils.get_flight_distance(client, api_form_values['Origin'], api_form_values['Dest'])
  
  # Turn the date into DayOfYear, DayOfMonth, DayOfWeek
  date_features_dict = predict_utils.get_regression_date_args(api_form_values['FlightDate'])
  for api_field_name, api_field_value in date_features_dict.items():
    prediction_features[api_field_name] = api_field_value
  
  # Vectorize the features
  feature_vectors = vectorizer.transform([prediction_features])
  
  # Make the prediction!
  result = regressor.predict(feature_vectors)[0]
  
  # Return a JSON object
  result_obj = {"Delay": result}
  return json.dumps(result_obj)

if __name__ == "__main__":
  app.run(debug=True)
