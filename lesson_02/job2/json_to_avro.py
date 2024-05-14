from flask import Flask, request
import os
import json
import fastavro

app = Flask(__name__)

token = os.environ.get('AUTH_TOKEN')

def convert_json_to_avro():
    global avro_schema
    content = request.get_json()
    stg_dir = content.get('stg_dir')
    raw_dir = content.get('raw_dir')

    os.makedirs(stg_dir, exist_ok=True)
    clean_folder(stg_dir)

    json_files = [file for file in os.listdir(raw_dir) if file.endswith('.json')]

    for json_file in json_files:
        json_path = os.path.join(raw_dir, json_file)
        avro_path = os.path.join(stg_dir, json_file.replace('.json', '.avro'))

        with open(json_path, 'r') as json_file:
            json_data = json.load(json_file)

        with open('avro_schema.json', 'r') as f:
            avro_schema = json.load(f)

        with open(avro_path, 'wb') as avro_file:
            fastavro.writer(avro_file, avro_schema, json_data)


def clean_folder(stg_dir):
    """
    Clean folder contents
    """

    if os.path.exists(stg_dir):
        for filename in os.listdir(stg_dir):
            file_path = os.path.join(stg_dir, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    clean_folder(file_path)
                    os.rmdir(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')
