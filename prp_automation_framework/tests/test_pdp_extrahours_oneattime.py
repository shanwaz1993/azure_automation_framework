from pdp_framework import PdpAutomation
from utils.utils import *


file_prefix = "PRP_ExtraHoursAPI_ExtraHours"


# Tested
@pytest.mark.pipeline
# @pytest.mark.regression
def test_pdp_flow_all():
    logging.info("""\n\t---------------Test Description---------------
    \n\tValidate ExtraHours PDP Flow, data_generator,upload_to_azure,trigger_adf_pipeline,validations\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)
    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["data_generator", "upload_to_azure",
                          "trigger_adf_pipeline", "validations"]
    config["validations"]["validation_flow"] = ["bronze", "silver", "gold"]
    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


# Tested
@pytest.mark.pipeline
def test_pdp_trigger_bronze_silver_pipeline_success():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours ADF Pipeline Trigger Successfully with correct parameters\n""")
    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["data_generator", "upload_to_azure",
                          "trigger_adf_pipeline", "validations"]

    config["trigger_adf_pipeline"]["run_pipelines"] = [{"run_id_key": "bronze_silver_run_id",
                                                        "pipeline_name": "pl_master_orchestrator_r1dm",
                                                        "parameters": {"DataSource": "ExtraHours",
                                                                       "PlatformName": "PRP",
                                                                       "SourceName": "ExtraHoursAPI",
                                                                       "DatasetName": "ExtraHours"
                                                                       },
                                                        "check_status_freq": 1}]

    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": "MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}'",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


# Tested
@pytest.mark.pipeline
#@pytest.mark.regression
def test_pdp_trigger_bronze_silver_pipeline_failed_filecheck():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours ADF Pipeline Trigger with no file in process folder\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["trigger_adf_pipeline", "validations"]  # Make sure no file exist in processed folder

    config["trigger_adf_pipeline"]["run_pipelines"] = [{"run_id_key": "bronze_silver_run_id",
                                                        "pipeline_name": "pl_master_orchestrator_r1dm",
                                                        "parameters": {"DataSource": "ExtraHours",
                                                                       "PlatformName": "PRP",
                                                                       "SourceName": "ExtraHoursAPI",
                                                                       "DatasetName": "ExtraHours"
                                                                       },
                                                        "check_status_freq": 1}]

    config["validations"]["input_count"] = 100
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": "MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}'",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 1


# Tested
@pytest.mark.pipeline
#@pytest.mark.regression
def test_pdp_trigger_bronze_silver_pipeline_failed_parameters():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours ADF Pipeline Trigger with incorrect parameters\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["trigger_adf_pipeline"]

    config["validations"]["input_count"] = 100
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": "MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}'",
                                        "type": [{"name": "count", "query": None}]}]

    config["trigger_adf_pipeline"]["run_pipelines"] = [{"run_id_key": "bronze_silver_run_id",
                                                        "pipeline_name": "pl_master_orchestrator_r1dm",
                                                        "parameters": {"DataSource": "ExtraHours",
                                                                       "PlatformName": "P",
                                                                       "SourceName": "ExtraHoursAPI",
                                                                       "DatasetName": "ExtraHours"
                                                                       },
                                                        "check_status_freq": 1}]  # Wrong PlatformName name

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 1


# Tested
@pytest.mark.pipeline
def test_pdp_trigger_gold_pipeline_success():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours ADF Pipeline Trigger with incorrect parameters\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["trigger_adf_pipeline"]

    config["trigger_adf_pipeline"]["run_pipelines"] = [{"run_id_key": "gold_run_id",
                                                        "pipeline_name": "pl_master_gold_with_period",
                                                        "parameters": {"goldDriverName": "GoldDriver",
                                                                       "platformName": "PRP",
                                                                       "sourceName": "ExtraHoursAPI",
                                                                       "datasetName": "ExtraHours",
                                                                       "StartDate": "2020-01-01",
                                                                       "EndDate": "2020-01-30", "KPIType": "ExtraHours",
                                                                       "pipelineRunId": "no need to pass"
                                                                       },
                                                        "check_status_freq": 1}]  # Wrong PlatformName name

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


# Tested
@pytest.mark.pipeline
# @pytest.mark.regression
def test_pdp_trigger_gold_pipeline_failed_parameters():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours ADF Pipeline Trigger with incorrect parameters\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["trigger_adf_pipeline"]

    config["trigger_adf_pipeline"]["run_pipelines"] = [{"run_id_key": "gold_run_id",
                                                        "pipeline_name": "pl_master_gold_with_period",
                                                        "parameters": {"goldDriverName": "GoldDriver",
                                                                       "platformName": "P",
                                                                       "sourceName": "ExtraHoursAPI",
                                                                       "datasetName": "ExtraHours",
                                                                       "StartDate": "2020-01-01",
                                                                       "EndDate": "2020-01-30", "KPIType": "ExtraHours",
                                                                       "pipelineRunId": "no need to pass"
                                                                       },
                                                        "check_status_freq": 1}]  # Wrong PlatformName name

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 1


# Tested
@pytest.mark.pipeline
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_duplicate_rows():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours if Bronze table have Duplicate Rows\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = "PRP_ExtraHoursAPI_ExtraHours_20230804-010.csv"  # File Having Duplicate rows
    config["pdp_flow"] = ["upload_to_azure",
                          "trigger_adf_pipeline", "validations"]

    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["input_count"] = 200

    config["trigger_adf_pipeline"]["run_pipelines"] = [{"run_id_key": "bronze_silver_run_id",
                                                        "pipeline_name": "pl_master_orchestrator_r1dm",
                                                        "parameters": {"DataSource": "ExtraHours",
                                                                       "PlatformName": "PRP",
                                                                       "SourceName": "ExtraHoursAPI",
                                                                       "DatasetName": "ExtraHours"
                                                                       },
                                                        "check_status_freq": 1}]

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": "MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}'",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


# Tested
@pytest.mark.pipeline
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_primary_key_invalid():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Silver table if invalid primary key uploaded\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["data_generator", "upload_to_azure",
                          "trigger_adf_pipeline", "validations"]

    config["data_generator"]["null_columns"] = [{"column": "extraHourId"}]
    config["data_generator"]["random_int_columns"] = None

    config["trigger_adf_pipeline"]["run_pipelines"] = [{"run_id_key": "bronze_silver_run_id",
                                                        "pipeline_name": "pl_master_orchestrator_r1dm",
                                                        "parameters": {"DataSource": "ExtraHours",
                                                                       "PlatformName": "PRP",
                                                                       "SourceName": "ExtraHoursAPI",
                                                                       "DatasetName": "ExtraHours"
                                                                       },
                                                        "check_status_freq": 1}]

    config["validations"]["validation_flow"] = ["bronze", "silver"]

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}' 
                                                    and extraHourId is null""",
                                        "type": [{"name": "count", "query": None}]}]

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}'
                                                    and extrahour_Id is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 1


# Tested
@pytest.mark.pipeline
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_column_invalid_value():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Silver table if invalid column other than primary key uploaded\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["data_generator", "upload_to_azure",
                          "trigger_adf_pipeline", "validations"]

    config["data_generator"]["random_str_columns"] = [{"column": "status",
                                                       "sample": ["create", "publish", "claim",
                                                                  "assign", "delete"]}]  # Invalid values

    config["trigger_adf_pipeline"]["run_pipelines"] = [{"run_id_key": "bronze_silver_run_id",
                                                        "pipeline_name": "pl_master_orchestrator_r1dm",
                                                        "parameters": {"DataSource": "ExtraHours",
                                                                       "PlatformName": "PRP",
                                                                       "SourceName": "ExtraHoursAPI",
                                                                       "DatasetName": "ExtraHours"
                                                                       },
                                                        "check_status_freq": 1}]

    config["validations"]["validation_flow"] = ["bronze", "silver"]

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}'
                                                    and status in ("create", "publish", "claim",
                                                                  "assign", "delete")""",
                                        "type": [{"name": "count", "query": None}]}]

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}'
                                                    and status in ("create", "publish", "claim",
                                                                  "assign", "delete")""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 1


# Tested
@pytest.mark.pipeline
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_column_invalid_date_format():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Silver table if invalid date format uploaded\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["data_generator", "upload_to_azure",
                          "trigger_adf_pipeline", "validations"]

    config["data_generator"]["random_date_columns"] = [{"column": "startDateTime",
                                                        "start": "2020-01-01",
                                                        "end": "2020-01-30",
                                                        "input_format": "%Y-%m-%d",
                                                        "output_format": "%Y-%m-%d %H:%M:%S.%f",
                                                        "step": "19h"}]  # Invalid date format

    config["trigger_adf_pipeline"]["run_pipelines"] = [{"run_id_key": "bronze_silver_run_id",
                                                        "pipeline_name": "pl_master_orchestrator_r1dm",
                                                        "parameters": {"DataSource": "ExtraHours",
                                                                       "PlatformName": "PRP",
                                                                       "SourceName": "ExtraHoursAPI",
                                                                       "DatasetName": "ExtraHours"
                                                                       },
                                                        "check_status_freq": 1}]

    config["validations"]["validation_flow"] = ["bronze", "silver"]

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}'
                                                    and startDateTime is not null""",
                                        "type": [{"name": "count", "query": None}]}]

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}'
                                                    and start_date_time is not null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 1


# No Tested
@pytest.mark.pipeline
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_invalid_skill_id():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Silver table if invalid skill_id uploaded\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["data_generator", "upload_to_azure",
                          "trigger_adf_pipeline", "validations"]

    config["data_generator"]["dimension_data"] = [{"name": "location", "columns": ["locationUUID"],
                                                   "renamed_map": {"locationUUID": "locationUuid"},
                                                   "records": 50},
                                                  {"name": "colleague", "columns": ["colleagueUUID"],
                                                   "renamed_map": {"colleagueUUID": "updatedBy"},
                                                   "records": 50}]

    config["data_generator"]["null_columns"] = [{"column": "departmentId"}]

    config["trigger_adf_pipeline"]["run_pipelines"] = [{"run_id_key": "bronze_silver_run_id",
                                                        "pipeline_name": "pl_master_orchestrator_r1dm",
                                                        "parameters": {"DataSource": "ExtraHours",
                                                                       "PlatformName": "PRP",
                                                                       "SourceName": "ExtraHoursAPI",
                                                                       "DatasetName": "ExtraHours"
                                                                       },
                                                        "check_status_freq": 1}]

    config["validations"]["validation_flow"] = ["bronze", "silver"]

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}' 
                                                    and departmentId is null""",
                                        "type": [{"name": "count", "query": None}]}]

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}'
                                                    and department_Id is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 1
