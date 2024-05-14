from flask import Flask, jsonify
from json_to_avro import convert_json_to_avro

app = Flask(__name__)

@app.route('/', methods=['POST'])
def handle_request():
    convert_json_to_avro()
    return jsonify({"message": "Converting completed successfully"}), 200

if __name__ == '__main__':
    app.run(debug=True, port=8082)
