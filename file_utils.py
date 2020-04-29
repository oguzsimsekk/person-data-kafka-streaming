import json


def get_file_content(mock_data_path):
    try:
        with open(mock_data_path) as json_file:
            data = json.load(json_file)
            return data
    except Exception as ex:
        print('Unable to read json file')
        print(str(ex))
