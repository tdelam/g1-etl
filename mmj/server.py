from flask import Flask, request
import etl_import

app = Flask(__name__)

@app.route('/import/extract', methods=['POST'])
def extract():
    if request.method == 'POST':
        organization_id = request.form["organization_id"]
        dispensary_id = request.form["dispensary_id"]
        return etl_import.extract(dispensary_id, organization_id)

@app.route('/healthcheck', methods=['GET'])
def healthcheck():
	if request.method == 'GET':
		return '{ "success": true, "status": 200 }'