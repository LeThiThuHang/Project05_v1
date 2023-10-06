import json
import os

# Define your schema
bq_schema = [
    {"name": "id", "type" : "INTEGER"},
    {"name": "sku", "type" : "STRING"},
    {"name": "price", "type" : "INTEGER"},
    {"name": "list_price", "type" : "INTEGER"},
    {"name": "original_price", "type" : "INTEGER"},
    {"name": "rating_average", "type" : "FLOAT"},
    {"name": "country_origin", "type" : "STRING"},
    {"name": "categories", "type" : "RECORD", "fields": [
        {"name":"id", "type": "INTEGER"},
        {"name": "name" , "type": "STRING"},
        {"name":"is_leaf", "type":"BOOLEAN"}
    ], "mode" : "REPEATED"},
    {"name": "current_seller", "type" : "RECORD", "fields": [
        {"name":"store_id","type":"INTEGER"},
        {"name":"name", "type": "STRING"}
    ],"mode":"REPEATED"},
    {"name": "all_time_quantity_sold", "type" : "INTEGER"}
]

# Read the original JSON file

# define a function to filter a dictionary based on a schema
def filter_dict(input_dict, schema):
    filtered_dict = {}
    for field in schema:
        field_name = field["name"]
        if field_name in input_dict:
            filtered_dict[field_name] = input_dict[field_name]
    # get origin
    if "specifications" in input_dict:
        for spec in input_dict["specifications"]:
            for attr in spec["attributes"]:
                if attr['code'] == 'origin' and attr['value'] is not None:
                    filtered_dict['origin'] = attr['value']

    return filtered_dict

#define the output_file_path
def output_file_name(input_file_path):
    output_file_path_root_folder = 'jsonl_files/'
    # extract the input_file names, then skip the name of the 'split_files/'
    output_file_name = os.path.splitext(input_file_path)[0][12:]
    output_file_path = output_file_path_root_folder + output_file_name + '.jsonl'
    return output_file_path

def transform(input_file_path,schema):
    filtered_data = []

    #read the json input file
    with open(input_file_path, 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)

    #filter the data based on the schema
    for item in data:
        filtered_data.append(filter_dict(item,schema))

    output_file_path = output_file_name(input_file_path)

    with open(output_file_path, 'w', encoding='utf-8') as jsonl_file:
        for item in filtered_data:
            # Convert each dictionary to a JSON string and write it as a line
            jsonl_file.write(json.dumps(item,ensure_ascii=False) + '\n')
        print(f"JSONL file {output_file_path} created successfully.")


#read the whole list of files in the split_files into the variables name list
    file_list = ['split_files/'+f for f in os.listdir('split_files')]
#print(file_list)

#read the output file
for input_file_path in file_list:
    transform(input_file_path,bq_schema)