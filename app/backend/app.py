import time
import uuid
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=True)

AUTH_HEADER = "Basic SVJ0UG9vRkEzS2lOZlY3bWJ5YVRFb0pucGVaQzBaaWg6REZQS0E2VVB2VEVrU0pldg=="
API_KEY = "IRtPooFA3KiNfV7mbyaTEoJnpeZC0Zih"

OAUTH_URL = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"
FUEL_LOCATION_URL = "https://api.onegov.nsw.gov.au/FuelPriceCheck/v1/fuel/prices/location"

_token_cache = {"access_token": None, "expires_at": 0.0}


def _get_access_token():
    now = time.time()
    if _token_cache["access_token"] and now < _token_cache["expires_at"]:
        return _token_cache["access_token"]

    response = requests.get(
        OAUTH_URL,
        params={"grant_type": "client_credentials"},
        headers={"Authorization": AUTH_HEADER},
        timeout=15,
    )
    response.raise_for_status()
    data = response.json()

    token = data["access_token"]
    expires_in = int(data.get("expires_in", 11 * 3600))
    _token_cache["access_token"] = token
    _token_cache["expires_at"] = now + max(expires_in - 60, 300)
    return token


def _fuel_api_headers():
    return {
        "Authorization": f"Bearer {_get_access_token()}",
        "apikey": API_KEY,
        "Content-Type": "application/json",
        "transactionID": str(uuid.uuid4()),
        "requestTimeStamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
    }


def _fetch_fuel_prices(namedlocation):
    response = requests.post(
        FUEL_LOCATION_URL,
        json={"fueltype": "E10", "namedlocation": namedlocation},
        headers=_fuel_api_headers(),
        timeout=15,
    )

    if response.status_code == 401:
        _token_cache["access_token"] = None
        _token_cache["expires_at"] = 0.0
        response = requests.post(
            FUEL_LOCATION_URL,
            json={"fueltype": "E10", "namedlocation": namedlocation},
            headers=_fuel_api_headers(),
            timeout=15,
        )

    return response


@app.route('/api/fuel', methods=['POST', 'OPTIONS'])
@cross_origin()
def get_fuel_data():
    if request.method == 'OPTIONS':
        response = make_response()
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
        response.headers.add('Access-Control-Allow-Methods', 'POST,OPTIONS')
        return response

    data = request.json or {}
    suburb = data.get('suburb', '').upper().strip()
    postcode = data.get('postcode', '').strip()

    if not suburb and not postcode:
        return jsonify({"price": "N/A", "prediction": "N/A"}), 400

    namedlocation = postcode or suburb

    try:
        response = _fetch_fuel_prices(namedlocation)

        print(f"--- Calling API for Suburb: {suburb} (location: {namedlocation}) ---")
        print(f"Status Received: {response.status_code}")

        if response.status_code != 200:
            print(f"Error Body: {response.text}")
            return jsonify({"price": f"Err {response.status_code}", "prediction": "Err"})

        prices_list = response.json().get('prices', [])

        if prices_list:
            raw_prices = [float(station['price']) for station in prices_list if 'price' in station]

            if raw_prices:
                avg_current_price = sum(raw_prices) / len(raw_prices)
                predicted_price = avg_current_price + 1.5

                return jsonify({
                    "price": f"{avg_current_price:.1f}¢",
                    "prediction": f"{predicted_price:.1f}¢"
                })

        return jsonify({"price": "N/A", "prediction": "N/A"})

    except Exception as e:
        print(f"Exception triggered: {str(e)}")
        return jsonify({"price": "Offline", "prediction": "Offline"}), 500


if __name__ == '__main__':
    app.run(port=5000, debug=True)
