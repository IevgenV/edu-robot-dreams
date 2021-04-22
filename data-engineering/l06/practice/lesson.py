import os
import json
from fastavro import writer, reader, parse_schema 


def write_avro():
    schema = {
        "namespace": "sample.avro",
        "type": "record",
        "name": "Cars",
        "fields": [
            {"name": "model", "type": "string"},
            {"name": "make", "type": ["string", "null"]},
            {"name": "year", "type": ["int", "null"]}
        ]
    }

    records = [
        {"model": "ABC", "make": "Audi", "year": 2010},
        {"model": "ABC", "make": "Audi", "year": 2010},
        {"model": "ABC", "make": "Audi", "year": 2010},
        {"model": "ABC", "make": "Audi", "year": 2010},
        {"model": "ABC", "make": "Audi", "year": 2010},
        {"model": "EDF", "make": "BEWM"},
        {"model": "EDF", "make": "BEWM"},
        {"model": "EDF", "make": "BEWM"},
        {"model": "EDF", "make": "BEWM"},
        {"model": "EDF", "make": "BEWM"},
        {"model": "XCV", "year": 2010},
        {"model": "XCV", "year": 2010},
        {"model": "XCV", "year": 2010},
        {"model": "XCV", "year": 2010},
        {"model": "XCV", "year": 2010}
    ]

    os.makedirs(os.path.join('.', 'data'), exist_ok=True)
    with open(os.path.join('.', 'data', 'cars.avro'), 'wb') as avro_file:
        writer(avro_file, parse_schema(schema), records)
    with open(os.path.join('.', 'data', 'cars.json'), 'w') as json_file:
        json.dump(records, json_file)


def read_avro():
    with open(os.path.join('.', 'data', 'cars.avro'), 'rb') as avro_file:
        for record in reader(avro_file):
            print(record)


def main():
    write_avro()
    read_avro()


if __name__ == '__main__':
    main()