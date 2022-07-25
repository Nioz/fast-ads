import webbrowser, os
import json
import boto3
from botocore.exceptions import ClientError
import logging
import io
from io import BytesIO
import sys
from pprint import pprint
import csv

file_name = ""

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    if event:
        print("Event : ", event)
        file_obj = event["Records"][0]
        filename = str(file_obj['s3']['object']['key']) #get name of file uploaded
        print("Filename: ", filename)
        file_name = filename
        main(file_name)

def main(file_name):
    
    s3 = boto3.client("s3")
    bucket = 'kpnbtextract'
    if file_name == "":
        file_name = 'images/Page04.jpg'
    fileObj = s3.get_object(Bucket = "kpnbtextract", Key = file_name)

    table_csv = get_table_csv_results(bucket, file_name)
    index1 = file_name.index("/")
    index2 = file_name.index(".")
    file_name = file_name[index1+1:index2]
    output_file = 'results/' + file_name + '_Output.csv'
    output_json = 'results/' + file_name + '_Output.json'

    # replace content
    #with open(output_file, "wt") as fout:
    #    fout.write(table_csv)

    #upload file
    uploadByteStream = bytes((table_csv).encode('UTF-8'))
    s3.put_object(Bucket=bucket, Key=output_file, Body=uploadByteStream)
    
    #convert csv to json and upload
    json_data = []
    
    csv_reader = csv.DictReader(table_csv)
    for csv_row in csv_reader:
        json_data.append(csv_row)
      
    final_json = json.dumps(json_data)   

    s3.put_object(Bucket=bucket, Key=output_json, Body=final_json)

    # show the results
    print('CSV OUTPUT FILE: ', output_file)

def get_rows_columns_map(table_result, blocks_map):
    rows = {}
    for relationship in table_result['Relationships']:
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                cell = blocks_map[child_id]
                if cell['BlockType'] == 'CELL':
                    row_index = cell['RowIndex']
                    col_index = cell['ColumnIndex']
                    if row_index not in rows:
                        # create new row
                        rows[row_index] = {}
                        
                    # get the text value
                    rows[row_index][col_index] = get_text(cell, blocks_map)
    return rows

def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] =='SELECTED':
                            text +=  'X '    
    return text

def get_table_csv_results(bucket, file_name):

    #Get the document from S3
    s3_connection = boto3.resource('s3')

    s3_object = s3_connection.Object(bucket, file_name)
    s3_response = s3_object.get()
    stream = io.BytesIO(s3_response['Body'].read())

    img_test = s3_object.get()['Body'].read()
    bytes_test = bytearray(img_test)
    print('Image loaded', file_name)

    #with open(s3_object, 'rb') as file:
    #    img_test = file.read()
    #    bytes_test = bytearray(img_test)
    #    print('Image loaded', file_name)

    # process using image bytes
    # get the results
    client = boto3.client('textract')

    #Analyze blocks to make csv
    response = client.analyze_document(Document={'Bytes': bytes_test}, FeatureTypes=['TABLES'])


    #Print out all blocks
    #response = client.detect_document_text(
    #    Document={'S3Object': {'Bucket': bucket, 'Name': file_name}})


    # Get the text blocks
    blocks=response['Blocks']
    pprint(blocks)

    blocks_map = {}
    table_blocks = []
    for block in blocks:
        blocks_map[block['Id']] = block
        if block['BlockType'] == "TABLE":
            table_blocks.append(block)

    if len(table_blocks) <= 0:
        return "<b> NO Table FOUND </b>"

    csv = ''
    for index, table in enumerate(table_blocks):
        csv += generate_table_csv(table, blocks_map, index +1)
        csv += '\n\n'

    return csv

def generate_table_csv(table_result, blocks_map, table_index):
    rows = get_rows_columns_map(table_result, blocks_map)

    table_id = 'Table_' + str(table_index)
    
    # get cells.
    csv = 'Table: {0}\n\n'.format(table_id)

    for row_index, cols in rows.items():
        
        for col_index, text in cols.items():
            csv += '{}'.format(text) + ","
        csv += '\n'
        
    csv += '\n\n\n'
    return csv

def upload_file(file_name, bucket, object_name):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

if __name__ == "__main__":
    main(file_name)