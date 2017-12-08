from flask import Flask, request
import etl_import

app = Flask(__name__)

@app.route('/import/extract', methods=['POST'])
def extract():
    if request.method == 'POST':
        organizationId = request.form["organizationId"]
        return etl_import.extract(organizationId)
