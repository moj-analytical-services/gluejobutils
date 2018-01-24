import json

def read_json(filename) :
    with open(filename) as json_data:
        data = json.load(json_data)
    return data

def write_json(data, filename) :
    with open(filename, 'w') as outfile:
        json.dump(data, outfile, indent=4, separators=(',', ': '))