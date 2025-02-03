import requests

url = "http://127.0.0.1:8000/query/"
data = {"question": "How many banks have assets above $1B?"}
response = requests.post(url, json=data)

print(response.status_code, response.json())
