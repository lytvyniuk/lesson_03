from flask import Flask, request, jsonify
from sales_api import clean_and_fetch_sales

app = Flask(__name__)

@app.route('/', methods=['POST'])
def handle_request():
    clean_and_fetch_sales(request.get_json())
    return jsonify({"message": "Data processing completed successfully"}), 200

if __name__ == '__main__':
    app.run(debug=True, port=8081)
