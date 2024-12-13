
from flask import Flask, request, jsonify
import pandas as pd
import redis
import hashlib
import json
from typing import List, Dict, Any

app = Flask(__name__)

# Load dataset
data = pd.read_csv('uc_results_gf.csv', parse_dates=['TRANSACTION DATE'])
data['TRANSACTION DATE'] = pd.to_datetime(data['TRANSACTION DATE'], format='%d/%m/%y')

# Configure Redis cache
cache = redis.StrictRedis(host='localhost', port=6379, db=0)

def generate_cache_key(params):
    """
    Generate a unique cache key with an emissions-specific prefix.
    """
    # Generate a hash of the parameters
    hash_key = hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()

    # Prefix the hash with 'emissions:' to easily identify and filter cache entries
    return f"emissions:{hash_key}"

def check_date_overlap(start1, end1, start2, end2):
    """
    Check if two date ranges overlap.
    """
    return not (end1 < start2 or start1 > end2)


def check_facility_overlap(facilities1, facilities2):
    """
    Check if two lists of facilities have any common elements.
    """
    return bool(set(facilities1) & set(facilities2))


def find_overlapping_cached_results(start_date, end_date, facilities):
    """
    Find and aggregate cached results that overlap with the current request.
    """
    overlapping_results = []

    # Use pattern matching to find only emissions-related cache keys
    all_cache_keys = cache.keys('emissions:*')

    for key in all_cache_keys:
        try:
            key_str = key.decode('utf-8')  # Convert bytes to string
            cached_data_str = cache.get(key).decode('utf-8')
            cached_data = json.loads(cached_data_str)

            cached_start = pd.to_datetime(cached_data['start_date'])
            cached_end = pd.to_datetime(cached_data['end_date'])
            cached_facilities = cached_data['facilities']

            # Check for date and facility overlap
            date_overlap = check_date_overlap(start_date, end_date, cached_start, cached_end)
            facility_overlap = check_facility_overlap(facilities, cached_facilities)

            if date_overlap and facility_overlap:
                overlapping_results.extend(cached_data['results'])

        except Exception as e:
            print(f"Error processing cache key {key_str}: {e}")
            continue

    return overlapping_results


@app.route('/api/emissions', methods=['GET'])
def get_emissions():
    """
    Retrieve aggregated CO2 emissions data for specified business facilities.

    This endpoint provides an intelligent caching mechanism to efficiently retrieve
    emissions data, reducing database load by serving cached or partially cached results.

    Request Payload:
    ---------------
    A JSON object containing the following required fields:
    {
        "startDate" (str): Start date for emissions data retrieval in ISO 8601 format (YYYY-MM-DD),
        "endDate" (str): End date for emissions data retrieval in ISO 8601 format (YYYY-MM-DD),
        "businessFacility" (list): List of business facility names to query
    }

    Request Payload Example:
    -----------------------
    {
        "startDate": "2023-01-01",
        "endDate": "2023-06-30",
        "businessFacility": ["GreenEat Changi", "GreenEat Orchard"]
    }

    Response:
    ---------
    Returns a JSON array of emissions data for the specified facilities:
    [
        {
            "businessFacility": str,
            "totalEmissions": float
        },
        ...
    ]
    """
    # Parse request data
    req_data = request.get_json()
    start_date = pd.to_datetime(req_data.get('startDate'))
    end_date = pd.to_datetime(req_data.get('endDate'))
    facilities = req_data.get('businessFacility', [])

    # Generate cache key for exact match
    exact_cache_key = generate_cache_key({
        'startDate': start_date.strftime('%Y-%m-%d'),
        'endDate': end_date.strftime('%Y-%m-%d'),
        'businessFacility': sorted(facilities),
    })

    # Check for exact match in cache
    cached_result = cache.get(exact_cache_key)
    # if cached_result:
    #     return jsonify(json.loads(cached_result.decode('utf-8')))
    if cached_result:
        cached_data = json.loads(cached_result.decode('utf-8'))
        return jsonify(cached_data.get('results', []))

    # Find overlapping cached results
    overlapping_results = find_overlapping_cached_results(start_date, end_date, facilities)

    # Aggregate overlapping results if found
    if overlapping_results:
        aggregated_results = (
            pd.DataFrame(overlapping_results)
            .groupby('businessFacility', as_index=False)['totalEmissions']
            .sum()
            .to_dict(orient='records')
        )
    else:
        # Filter dataset based on current request
        filtered_data = data[
            (data['TRANSACTION DATE'] >= start_date) &
            (data['TRANSACTION DATE'] <= end_date) &
            (data['Business Facility'].isin(facilities))
            ]

        # Aggregate emissions
        aggregated_results = filtered_data.groupby('Business Facility')['CO2_ITEM'].sum().reset_index()
        aggregated_results.columns = ['businessFacility', 'totalEmissions']
        aggregated_results = aggregated_results.to_dict(orient='records')

    # Cache the new result
    cache_value = {
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'facilities': sorted(facilities),
        'results': aggregated_results
    }
    cache.set(exact_cache_key, json.dumps(cache_value))

    return jsonify(aggregated_results)


if __name__ == '__main__':
    app.run(debug=True)