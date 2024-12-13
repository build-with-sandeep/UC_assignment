## Emissions Data API
## Overview
This Flask application provides an API endpoint for retrieving and caching CO2 emissions data for business facilities. The application uses intelligent caching to reduce database load and improve response times.
Prerequisites

Python 3.8+
Redis server
Pandas
Flask

## Setup Instructions
1. Clone the Repository
```bash
git clone https://github.com/build-with-sandeep/UC_assignment.git
```
## 2. Create a Virtual Environment
```bash
python3 -m venv venv
```
Activate virtual environment 

`source venv/bin/activate`

 On Windows, use

`venv\Scripts\activate`

## 3. Install Dependencies
```bash
pip install -r requirements.txt
```

## 4. Configure Redis

Ensure Redis is installed and running on your local machine.
Default configuration uses localhost:6379.
Modify redis.StrictRedis() in the code if your Redis setup differs

## 5. Run the Application
```bash
python main.py
```
# API Endpoint
/api/emissions (GET)

Request Payload
```json
{
    "startDate": "2023-01-01",
    "endDate": "2023-06-30",
    "businessFacility": ["GreenEat Changi", "GreenEat Orchard"]
}
```
Response
```json
[
    {
        "businessFacility": "GreenEat Changi",
        "totalEmissions": 1234.56
    }
]
```
