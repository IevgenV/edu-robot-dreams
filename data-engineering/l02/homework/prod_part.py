#%%
import requests
import json

url = "https://robot-dreams-de-api.herokuapp.com/auth"
headers = {"content-type": "application/json"}
data = {"username": "***", "password": "***"}

r = requests.post(url, data=json.dumps(data), headers=headers)
resp = r.json()
print(resp)


# %%

token = "JWT " + resp["access_token"]

url = "https://robot-dreams-de-api.herokuapp.com/out_of_stock"
data = {"date": "2021-01-03"}
headers = {"content-type": "application/json", "Authorization": token}

r = requests.get(url, data=json.dumps(data), headers=headers)
print(r.json())
# %%
