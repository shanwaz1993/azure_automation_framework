# PDP Automation Framework

This PDP Automation Framework is design to cover end to end PDP automation process.
that involves Modules
1. Data Generator: - Generate Mock up data and calculating KPI, 
2. Upload to Azure: Upload generated Data to azure.
3. Trigger ADF Pipeline- Trigger the ADF pipeline in azure
4. Validations - Perform validation on Databricks bronze, silver and gold layer tables.
5. test suits - Test pdp automation flow using pytest framework

Please check the conflunce page for more information on the same:
https://confluence.global.tesco.org/display/CTP/PDP+Automation+Framework+%7C+WIP

## Installation

To run this script, make sure you have Python installed on your system. Additionally, install the required dependencies by running the following command:

Installation command

```bash
 python.exe -m pip install --upgrade pip --trusted-host pypi.org --trusted-host pypi.org --trusted-host files.pythonhosted.org --trusted-host pypi.python.org
```
 Then need to install multiple packages 
```bash
 pip install <your-package-name> --trusted-host pypi.org --trusted-host pypi.org --trusted-host files.pythonhosted.org --trusted-host pypi.python.org
```
In above command you need to replace <your-package-name> with the package names below need to be installed.
1. Polars
2. Pandas
3. azure-storage-blob
4. azure-identity
5. azure-mgmt-datafactory
6. databricks-sql-connector
7. pytest-html 
8. pytest
9. pytest-xdis
10. python-dotenv

# Clone Repository
```bash
# to  clone repository 
mkdir <your-folder>
cd <your-folder>
git init
git clone <https://<repository-link>> 
```

Repository Link : https://github.dev.global.tesco.org/INE12405137/pdp_automation_framework/tree/feature_automation_framework

# Modules Information

## Data Generation Script

This Python script is designed to generate large volumes of data based on a given configuration. It utilizes the `polars` library for efficient data manipulation and provides various options for generating and transforming data. The generated data is then written to a CSV file for further use.

Script: data_generator.py

## Input

The script reads a JSON configuration file that specifies the data generation settings. The configuration file should include the following options:

- `dimension_data`: An array of objects specifying the dimensions to generate. Each object should have the following properties:
  - `name`: The name of the dimension (corresponding to a CSV file with the same name).
  - `columns`: An array of column names to include from the dimension CSV file.

- `date_dimension_columns`: An array of objects specifying the date dimensions to generate. Each object should have the following properties:
  - `column`: The name of the column to populate with random timestamps.
  - `start`: The start date for the random timestamp range.
  - `end`: The end date for the random timestamp range.

- `int_dimension_columns`: An array of objects specifying the int dimensions to generate. Each object should have the following properties:
  - `column`: The name of the column to populate with random int.
  - `start`: The start int for the random int range.
  - `end`: The end int for the random int range.

- `random_date_columns`: An array of objects specifying columns to be populated with random timestamp values. Each object should have the following properties:
  - `column`: The name of the column to populate with random timestamps.
  - `start`: The start date for the random timestamp range.
  - `end`: The end date for the random timestamp range.

- `random_int_columns`: An array of objects specifying the int  to generate. Each object should have the following properties:
  - `column`: The name of the column to populate with random int.
  - `start`: The start int for the random int range.
  - `end`: The end int for the random int range.

- `random_str_columns`: An array of objects specifying the str  to generate. Each object should have the following properties:
  - `column`: The name of the column to populate with random str.
  - `start`: The start str for the random str range.
  - `end`: The end str for the random str range.

- `null_columns`: An array of objects specifying the name to generate. Each object should have the following properties:
  - `column`: The name of the column to populate with null.

- `output_schema`: An array of column names to include in the final output CSV file. Only the specified columns will be retained.

- `output_file`: The name of the output CSV file to be created.

- `load_date_columns`: An array of objects specifying columns to be populated with load date values. Each object should have the following properties:
  - `column`: The name of the column to populate with load dates.
  - `load_date`: (Optional) A specific load date value to assign to the column. If not provided, the current timestamp will be used.

- `output_count`: The desired number of records to generate. This value is used when generating random date columns to ensure an adequate number of timestamps.
- `output_format` : csv or txt
- `file`: output file name
- `drop_duplicates` : drop duplicates columns.
- `output_schema`: List of columns required in output.
- `utf8_encoding`: is output file is required in utf8 format.
- `output_folder`: output folder name
- `transformation`: the Transformation for dataframe required. it is specific to columns:
- `kpi_calculation`: the KPI calculation transformation for dataframe required. it is for gold layer:


## Output

The script generates the desired data based on the provided configuration and output, the File names will the same as name provide in file in config or given as input parameter.

## Configurations

The script performs the following data generation configurations:

1. **Join Dimension DataFrames**: It joins multiple DataFrames from dimension data using the specified join method.

2. **Read Dimension DataFrames**: It reads dimension data from CSV files and creates a joined DataFrame.

3. **Add Date Dimension Columns**: It adds date-related columns to the DataFrame based on the specified date range.

4. **Add Integer Dimension Columns**: It adds integer-related columns to the DataFrame based on the specified range and step.

5. **Add Random Date Columns**: It adds random date values to the DataFrame within the specified date range.

6. **Add Random Integer Columns**: It adds random integer values to the DataFrame within the specified range and step.

7. **Add Random String Columns**: It adds random string values to the DataFrame based on the specified samples.

8. **Add Load Date Columns**: It populates columns with load date values or the current timestamp.

9. **Transformation**: It performs custom transformations on the DataFrame.

10. **Drop Duplicates**: It removes duplicate rows from the DataFrame based on specified columns.

11. **Output Schema**: It selects specific columns to include in the final output CSV file.

12. **Output Count**: It samples a specific number of records from the DataFrame for the output CSV file.

13. **Output File**: It writes the generated DataFrame to a CSV file.

14. **UTF-8 Encoding**: It converts the output CSV file to UTF-8 encoding.

15. **KPI Calculation**: It performs additional key performance indicator calculations.

The script generates data based on the provided configuration, writes it to a CSV file, and displays the base HTML content with details of the data generation process.


# Upload To Azure

This Python script is designed to upload a file to Azure Blob Storage using the Azure-identity. It provides functionality to connect to an Azure Blob Storage container, read a local file in chunks, and upload it to the specified blob path.

## Input

The script reads a JSON configuration file that specifies the Azure Blob Storage upload settings. The configuration file should include the following options:
- `file`: Input and output File name
- `source_folder`: source folder name
- `container_name`: The name of the Azure Blob Storage container to upload the file to.
- `storage_account_name`: The name of the Azure Storage account.
- `chunk_size_mb`: The size of each chunk in megabytes for uploading the file in chunks.
- `blob_folder_raw`: The path of the folder inside the Azure Blob Storage container where the file will be uploaded.
- `blob_folder_encrypted`: encrypted folder name
- `blob_folder_process`: process folder name
- `blob_folder_error`: error folder name


## Output

The script uploads the specified file to the Azure Blob Storage container based on the provided configuration. It performs the following steps:

1. Connects to Azure Blob Storage using the DefaultAzureCredential.

2. Creates the BlobServiceClient object using the account URL and default credential.

3. Defines the chunk size for uploading the file in smaller segments.

4. Gets the blob client using the container name and blob file path.

5. Uploads the local file to the specified blob path in chunks.

6. Waits for the file to be uploaded and then sleeps 30 sec.

7. Check file in encrypt folder 

8. check file in process folder and error folder

9. Returns a status of status=0 if the file uploaded in process folder , else 1.

## Configurations

The script reads the configuration from the JSON file and provides options to customize the upload process. The JSON configuration file should have the necessary parameters as mentioned in the "upload_to_azure" section.


## Conclusion

The Azure Blob Storage Uploader script simplifies the process of uploading files to Azure Blob Storage, allowing for efficient handling of large files and enabling easy integration with Azure-based applications.


# Azure Data Factory Pipeline Trigger

This Python script is designed to trigger one or more pipelines in Azure Data Factory (ADF). It uses the Azure Management SDK to connect to an ADF instance and trigger the specified pipelines. The script monitors the status of the pipeline run and updates the user with the run status.



# Input
The script reads a JSON configuration file that specifies the ADF pipeline trigger settings. The configuration file should include the following options:

factory_name: The name of the Azure Data Factory instance.

run_pipelines: An array of objects specifying the pipelines to trigger. Each object should have the following properties:

- `pipeline_name`: The name of the pipeline to trigger.
- `parameters`: (Optional) Any parameters required for the pipeline run.
- `check_status_freq`: (Optional) The frequency in minutes to check the status of the pipeline run. Default is 1 minute.
- `run_id_key`: (Optional) A key name to store the pipeline run ID for future reference.


## Output

The script triggers the specified pipelines in Azure Data Factory based on the provided configuration. It performs the following steps:

- `Connects`: to the Azure Data Factory Management client using the DefaultAzureCredential.

- `Triggers`: each specified pipeline run with the given parameters.

- `Monitors` the status of each pipeline run until it completes or fails.

- `Updates` the user with the pipeline run status and raises an exception if the pipeline run fails.

  
# Data Validation Script

This Python script is designed to perform data validation on Azure Databricks tables. It connects to the Azure Databricks cluster using the provided credentials and performs various data validations on the specified tables. The script supports count validation, duplicate validation, data validation, and date format validation.

## Input

The script reads a JSON configuration file that specifies the data validation settings. The configuration file should include the following options:
- `file`: source file from local for fata validation.
- `kpi_file` : kpi file for kpi validation.
- `input_count`: input file count.
- `bronze_silver_run_id`: run id for pipeline.
- `gold_run_id`: gold run id
- `validation_flow`: Validation flow bronze,silver,gold.

- `bronze` (Optional): An array of objects specifying the data validation settings for the bronze layer. Each object should have the following properties:
  - `type`: An array of validation types to perform (e.g., count, duplicate, data, date_format).
  - `db_table`: The name of the database table to validate.
  - `where`: (Optional) The WHERE clause to apply during validation.
  - `source_path`: (Optional) The path to the source data file. If not provided, the script looks for the file in the specified folder.
  - `folder`: (Optional) The folder where the source data file is located.
  - `select_cols`: (Optional) An array of column names to select from the source data file.
  - `sort_column`: (Optional) The column to use for sorting data during validation.
  - `header`: (Optional) A boolean indicating whether the source data file has a header row.

- `silver` (Optional): An array of objects specifying the data validation settings for the silver layer. The structure of each object is similar to the `bronze_validation` settings.
  - `type`: An array of validation types to perform (e.g., count, duplicate, data, date_format).
  - `db_table`: The name of the database table to validate.
  - `where`: (Optional) The WHERE clause to apply during validation.
  - `source_path`: (Optional) The path to the source data file. If not provided, the script looks for the file in the specified folder.
  - `folder`: (Optional) The folder where the source data file is located.
  - `select_cols`: (Optional) An array of column names to select from the source data file.
  - `sort_column`: (Optional) The column to use for sorting data during validation.
  - `header`: (Optional) A boolean indicating whether the source data file has a header row.

- `gold` (Optional): An array of objects specifying the data validation settings for the gold layer. The structure of each object is similar to the `bronze_validation` settings.
  - `type`: An array of validation types to perform (e.g., count, duplicate, data, date_format).
  - `db_table`: The name of the database table to validate.
  - `where`: (Optional) The WHERE clause to apply during validation.
  - `source_path`: (Optional) The path to the source data file. If not provided, the script looks for the file in the specified folder.
  - `folder`: (Optional) The folder where the source data file is located.
  - `select_cols`: (Optional) An array of column names to select from the source data file.
  - `sort_column`: (Optional) The column to use for sorting data during validation.
  - `header`: (Optional) A boolean indicating whether the source data file has a header row.


## Output

The script performs the specified data validations on the specified tables in Azure Databricks. It performs the following validations:

- **Count Validation**: Compares the count of records in the table with the provided `input_count`.

- **Duplicate Validation**: Checks for duplicate records in the table.

- **Data Validation**: Compares the source data in a CSV file with the data in the specified database table. It checks for data mismatches, sorts the data if required, and verifies if the column selection matches.

- **Date Format Validation**: Validates the date format of specific columns in the table.

The script generates an HTML report that provides details about the validation status for each table and validation type. It displays the count of records, duplicate status, data match status, and date format validation status.

## Configuration

The script reads the configuration from the JSON file and provides options to customize the data validation process. The JSON configuration file should have the necessary parameters as mentioned in the "Input" section.

## Usage

To use the script, follow these steps:

1. Create a JSON configuration file with the required settings (e.g., `config.json`).

2. Run the script.

3. The script will read the configuration file and perform the specified data validations on the specified tables.

4. Check the generated HTML report for the validation status of each table and validation type if html_report is true.


## Conclusion

The Data Validation Script for Azure Databricks streamlines the process of data validation on tables, ensuring data integrity and accuracy in data processing pipelines.

## Sample Output

The script generates an HTML report with details about each validation type and table. Below is a sample output:

Validation Report: Check the sample report

# How To Trigger

To trigger this whole Pipeline we have pdp_flow configuration in our config file. 
That follow the order of execution Example:

```json
"pdp_flow": ["data_generator","upload_to_azure","trigger_adf_pipeline","validations"]
```

Then have to Trigger the pdp_framework.py script with configuration file as input.
Example :

```bash
python pdp_framework.py --config_name="PRP_ExtraHoursAPI_ExtraHours_config.json" --config_name="PRP_ExtraHoursAPI_ExtraHours_{date_time}.json"
```

