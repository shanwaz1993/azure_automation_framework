from dotenv import load_dotenv
import os
import json
import csv
import argparse
import polars as pl
import logging

"""Imports used in other Modules of PDP."""
import random
import numpy as np
import pytest
from time import sleep
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from databricks import sql
from collections import namedtuple

logging.getLogger("databricks").setLevel(logging.ERROR)
logging.getLogger("azure").setLevel(logging.ERROR)

load_dotenv()


def set_env_variables(env, file_name='env_config.json'):
    try:
        file_path = os.path.join("..\\", "", file_name)
        env_vars = read_json_file(file_path)
        print(env_vars)
    except FileNotFoundError as exception:
        logging.critical("No env_config file found in path %s error  %s", file_name, exception)
        raise FileNotFoundError(f"{exception}") from exception

    try:
        logging.info("loading env vars for env  %s", env)
        for k, v in (env_vars.get(env)).items():
            os.environ[k] = v
    except ValueError as exception:
        logging.critical(f"Error while setting Environment variables")
        raise ValueError(f"{exception}") from exception

    return get_azure_details()


def get_vault_key(key_vault_name, secret_name):
    # keyVaultName = 'un-dev-186-prp-kv'
    kv_uri = f"https://{key_vault_name}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=kv_uri, credential=credential)
    secret_value = client.get_secret(secret_name)
    return secret_value


def get_azure_details():
    return {"AZURE_SUBSCRIPTION_ID": os.getenv("AZURE_SUBSCRIPTION_ID"),
            "RESOURCE_GROUP_NAME": os.getenv("RESOURCE_GROUP_NAME"),
            "AZURE_TENANT_ID": os.getenv("AZURE_TENANT_ID"),
            "AZURE_CLIENT_ID": os.getenv("AZURE_CLIENT_ID"),
            "AZURE_CLIENT_SECRET": os.getenv("AZURE_CLIENT_SECRET"),
            "DATABRICKS_SERVER_HOSTNAME": os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            "DATABRICKS_HTTP_PATH": os.getenv("DATABRICKS_HTTP_PATH"),
            "DATABRICKS_TOKEN": os.getenv("DATABRICKS_TOKEN")}


def read_json_file(file_path):
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)
    return data


def get_file_path(base_folder, folder, file_name):
    return os.path.join(base_folder, folder, file_name)


def read_config(base_folder="..\\", folder="config", config_file=None, key=None, config=None):
    if config is None:
        parser = argparse.ArgumentParser(description='Read JSON config file')
        parser.add_argument('--config_file',
                            dest='config_file',
                            required=False,
                            default=None,
                            help='Name of the config file')

        parser.add_argument('--file_name',
                            dest='file_name',
                            required=False,
                            default=None,
                            help='Name of the data file need to generate')

        # args = parser.parse_args()
        args, unknown_args = parser.parse_known_args()

        if config_file is not None:
            config_path = get_file_path(base_folder, folder, config_file)
        elif args.config_file is not None:
            config_path = get_file_path(base_folder, folder, args.config_file)
        else:
            logging.info("No config fie Passed")
            raise Exception("No config Files Passed")

        config = read_json_file(config_path)
        if key is not None:
            config = config.get(key)

        if args.file_name is not None:
            config["file"] = args.file_name

        return config


def get_env_vars():
    azure_details = get_azure_details()
    return azure_details


def print_kpi(df):
    print("\tKPI column Names ", df.columns)
    # print(df.head(30))
    return 0


def print_shape(df, df_name=''):
    row_count, column_count = df.shape
    print("\n\trow count ", row_count)
    print("\tcolumn count ", column_count)
    print("\tcolumn Names ", df.columns)
    print(f"\tSample data for {df_name}")
    # print(df.head(5))
    return 0


def read_csv(base_folder, folder, source_file, has_header=True):
    # file_path = os.path.join(base_folder, folder, source_file)
    file_path = get_file_path(base_folder, folder, source_file)
    data = pl.read_csv(source=file_path, has_header=has_header)
    return data


def write_csv(df, base_folder, folder, output_file):
    file_path = get_file_path(base_folder, folder, output_file)
    df.write_csv(file=file_path, has_header=True)
    return 0


def delete_file(base_folder, folder, file_name):
    file_path = get_file_path(base_folder, folder, file_name)
    if os.path.exists(file_path):
        os.remove(file_path)
    return 0


def convert_csv_to_utf8(base_folder, folder, input_file, output_file):
    # Read the CSV file with the default encoding
    with open(get_file_path(base_folder, folder, input_file), 'r', encoding='utf-8-sig') as file:
        reader = csv.reader(file)
        data = list(reader)

    # Write the CSV file with UTF-8 encoding
    with open(get_file_path(base_folder, folder, output_file), 'w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)

    # Delete existing file
    delete_file(base_folder=base_folder,
                folder=folder,
                file_name=input_file)


def html_base_content():
    base_html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <style>
table, th, td {
  border: 1px solid black;
}
</style>
    <meta charset="UTF-8">
    <title>Title</title>
</head>
<body>
<h1>PDP Automation Report</h1>
<p>This is the PDP Automation report generated after the execution of PDP Automation Framework,
    Please find the report below</p>
add_segment
</body>
</html>"""
    return base_html_content


def get_html_segment(tag):
    if tag == "data_generator":
        new_segment = """<h2>Data Generator </h2>
<p>Data generator Details</p>
<table style="width:100%">
  <tr>
    <th>Details</th>
    <th>Value</th>
  </tr>
  <tr>
    <td>File Name</td>
    <td>{file_name}</td>
  </tr>
  <tr>
    <td>Generated Count</td>
    <td>{output_count}</td>
  </tr>
  <tr>
    <td>Column Count</td>
    <td>{columns_count}</td>
  </tr>
  <tr>
    <td>Column Names</td>
    <td>{columns}</td>
  </tr>
  <tr>
    <td>Status</td>
    <td>{Status}</td>
  </tr>
</table>
"""
    elif tag == "upload_to_azure":
        new_segment = """<h2>Upload File to Azure </h2>
<p>Azure Upload file details</p>
<table style="width:100%">
  <tr>
    <th>Details</th>
    <th>Value</th>
  </tr>
  <tr>
    <td>Source File </td>
    <td>{source_file}</td>
  </tr>
  <tr>
    <td>Azure Path</td>
    <td>{azure_file}</td>
  </tr>
  <tr>
    <td>Status</td>
    <td>{Status}</td>
  </tr>
</table>
"""
    elif tag == "trigger_adf_pipeline":
        new_segment = """<h2>Datafactory Pipeline </h2>
<p>Triggering datafactory pipeline details</p>
<table style="width:100%">
  <tr>
    <th>Details</th>
    <th>Value</th>
  </tr>
  <tr>
    <td>ADF Pipeline Name</td>
    <td>{pipeline_name}</td>
  </tr>
  <tr>
    <td>ADF Run Id</td>
    <td>{run_id}</td>
  </tr>
  <tr>
    <td>Status</td>
    <td>{Status}</td>
  </tr>
</table>
"""
    elif tag == "validations":
        new_segment = """<h2>{layer_name}</h2>
<p>{layer_name} Validation details</p>
<table style='width:100%'>
  <tr>
    <th>Details</th>
    <th>Value</th>
  </tr>
  <tr>
    <td>Layer Name</td>
    <td>{layer_name}</td>
  </tr>
  <tr>
    <td>Count</td>
    <td>{count}</td>
  </tr>
  <tr>
    <td>duplicate</td>
    <td>{duplicate}</td>
  </tr>
  <tr>
    <td>data</td>
    <td>{data}</td>
  </tr>
  <tr>
    <td>date format</td>
    <td>{date_format}</td>
  </tr>
</table>"""
    else:
        raise Exception("Tag is not correctly passed")

    return new_segment


def write_to_html(file_path,content):
    with open(file_path, "w") as f:
        f.write(content)
    return 0


def add_segment_content(tag, value_dict, base_content=None):
    if base_content is None:
        base_html_content = html_base_content()
    else:
        base_html_content = base_content

    new_segment = get_html_segment(tag=tag)

    new_segment = new_segment.format(**value_dict) + "\nadd_segment"
    new_base_html_content = base_html_content.replace("add_segment", new_segment)
    return new_base_html_content
