#%%
import requests
res = requests.get('https://scotch.io')
print(res)

# %%
py_list = ['Bob', 'Anne', 'Joe']
py_tuple = ('Bob', 'Anne', 'Joe')
py_set = {'Bob', 'Anne', 'Joe'}

# %%
print(py_list[0])
print(py_tuple[0])
print(py_set)

# %%
py_list.append('John')
print(py_list)

# %%
py_list_ext = ['Mike', 'Samuel']
py_list.extend(py_list_ext)
print(py_list)

# %%
friends = {'Bob', 'Ann', 'Joe'}
abroad = {'Bob', 'Joe'}

# %%
local = friends.difference(abroad)
print(local)

# %%
python = {'Bob', 'Jen', 'Rolf', 'Charlie'}
scala = {'Bob', 'Jen', 'Adam', 'Anne'}

both = python.intersection(scala)
only = python.symmetric_difference(scala)
only_python = only.intersection(python)
only_scala = only.intersection(scala)
print(both)
print(only)
print(only_python)
print(only_scala)


# %%
ls = [i for i in range(1, 11)]
print(ls)

# %%
print(ls[2:5:2])
# reverse order:
print(ls[::-1])

# %%
emp_salary = {'Bob':      5000,
              'Anne':     4000,
              'Kristine': 4000,
              'Eugene':   4200}

# %%
emps = [ {"name": k, "salary": v} for k, v in emp_salary.items() ]
emps

# %%
delim = "----"
print(delim)
for emp in emps:
    print(emp["salary"])
    print(delim)

# %%
import json
with open("./data/dd.json", 'w') as json_file:
    json.dump(emps, json_file)

# %%
with open("./data/dd.json", 'r') as json_file:
    new_emps = json.load(json_file)
new_emps

# %%
import requests
from requests import HTTPError
import yaml

def load_cfg(fp, application='app'):
    return yaml.load(fp)[application]

with open("./configs/cfg.yaml") as yaml_file:
    cfg = load_cfg(yaml_file)

print(type(cfg))
print(cfg)

for base in cfg['bases']:
    try:
        res = requests.get(cfg['api_rates_url'], params={"base": base}, timeout=1)
        res.raise_for_status()
    except HTTPError as e:
        print(f"Exception with {base}")
        continue

    rates = res.json()
    with open(f'./data/currency-{base}.json', 'w') as json_file:
        json.dump(rates, json_file)

print('\n----\n', 'done')

# %%