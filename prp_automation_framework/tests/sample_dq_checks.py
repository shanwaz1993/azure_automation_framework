from pdp_framework import PdpAutomation
from utils.utils import *

file_prefix = "PRP_ExtraHoursAPI_ExtraHours"

existing_bronze_silver_run_id = '8823b715-3fde-11ee-97a8-bcf4d42f600c'  # Define if running withrunid mark
existing_data_file = 'PRP_ExtraHoursAPI_ExtraHours_20230821-477.csv'  # Define if running withrunid mark


# Tested
@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_date_format():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours if Bronze table have correct date Format as "%Y-%m-%dT%H:%M:%S.%f\n""")
    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file
    config["pdp_flow"] = ["validations"]

    config["validations"]["validation_flow"] = ["bronze"]

    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id
    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": "True=True",
                                        "type": [{"name": "date_format",
                                                  "date_columns": ["startDateTime",
                                                                   "endDateTime",
                                                                   "updatedTimestamp",
                                                                   "createdTimestamp"
                                                                   "publishedTimestamp"],
                                                  "format": "%Y-%m-%dT%H:%M:%S.%f"}]}]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


# Tested
@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_extra_cols():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours if Bronze table have Extra columns like MD_CREATED_DTM,MD_PIPELINE_RUN_ID,MD_BATCH_ID\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file
    config["pdp_flow"] = ["validations"]

    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """True=False""",
                                        "type": [{"name": "cols_exist",
                                                  "columns": ["MD_CREATED_DTM", "MD_CHANGED_DTM", "MD_CREATED_BY",
                                                              "MD_MODIFIED_BY",
                                                              "MD_SOURCE_NAME", "MD_PLATFORM_NAME", "MD_BATCH_ID",
                                                              "MD_KEY_HASH",
                                                              "MD_ROW_HASH", "MD_PIPELINE_RUN_ID", "MD_PIPELINE_NAME",
                                                              "MD_DATASET_NAME",
                                                              "MD_LOAD_DT"]}]}]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_integrity_assigned_to():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours bronze table integrity check for assignedTo\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """assignedTo is not null
                                                  and assignedTo is not in (select distinct colleagueUUID  
                                                  from delta_silver.colleague)""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_consistency_assigned_to():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours bronze table integrity check for assignedTo\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """assignedTo is null
                                                    and (status in ('claimed', 'assigned','deleted')
                                                  )""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_validity_assigned_to():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours bronze table integrity check for assignedTo\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """length(assignedTo)!=36""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


# Tested
@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_integrity_department_id():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours bronze table integrity check for departmentId\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """departmentId is not in (select distinct skillCode 
                                                                              from delta_silver.skills)""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_department_id_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Silver table null check for departmentId\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """departmentId is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_validity_department_id():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours bronze table integrity check for assignedTo\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """length(departmentId)!=7""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_end_date_time_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Bronze table null check for endDateTime\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """endDateTime is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_validity_end_date_time():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Bronze table null check for endDateTime\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """timestamp(endDateTime) < timestamp( startDateTime)""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_extra_hour_id_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Bronze table null check for extraHourId\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """extraHourId is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_validity_extra_hour_id():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Bronze table null check for extraHourId\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """length(extraHourId)!=7 
                                        or cast(extraHourId as double) is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


# Tested
@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_integrity_location_uuid():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours bronze table integrity check for location_uuid\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """locationUuid is not in (select distinct locationUuid  
                                                  from delta_silver.location)""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_location_uuid_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Bronze table null check for location_uuid\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """locationUuid is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_validity_location_uuid():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Bronze table null check for location_uuid\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """length(locationUuid)!=36""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_consistency_published_timestamp():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours bronze table integrity check for published_timestamp\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """publishedTimestamp is null
                                                    and (status in ('claimed', 'assigned','published','deleted')
                                                  )""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_start_date_time_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Bronze table null check for startDateTime\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """startDateTime is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_status_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours bronze table integrity check for status\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """status is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_integrity_status():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours bronze table integrity check for status\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """status is not in ('created','published','claimed',
                                                  'assigned','deleted')""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_status_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours bronze table integrity check for status\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """status is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_updated_timestamp_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Silver table null check for updatedTimestamp\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """updatedTimestamp is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


# ----------------------------Bronze DQ checkDone-------------------------

# ---------------Silver DQ check STARTS----------------------

# Tested
@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_date_format():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours if Silver table have correct date Format as "%Y-%m-%dT%H:%M:%S.%f\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file
    config["pdp_flow"] = ["validations"]

    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": "True=True",
                                        "type": [{"name": "date_format",
                                                  "date_columns": ["start_date_time",
                                                                   "end_date_time",
                                                                   "updated_timestamp"
                                                                   "published_timestamp"],
                                                  "format": "%Y-%m-%dT%H:%M:%S.%f"}]}]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


# Tested
@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_extra_cols():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours if Silver table have Extra columns like MD_CREATED_DTM,MD_PIPELINE_RUN_ID,MD_BATCH_ID\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file
    config["pdp_flow"] = ["validations"]

    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": "True=False",
                                        "type": [{"name": "cols_exist",
                                                  "columns": ["MD_CREATED_DTM", "MD_CHANGED_DTM", "MD_CREATED_BY",
                                                              "MD_MODIFIED_BY",
                                                              "MD_SOURCE_NAME", "MD_PLATFORM_NAME", "MD_BATCH_ID",
                                                              "MD_KEY_HASH",
                                                              "MD_ROW_HASH", "MD_PIPELINE_RUN_ID", "MD_PIPELINE_NAME",
                                                              "MD_DATASET_NAME",
                                                              "MD_LOAD_DT"]}]}]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_integrity_assigned_to():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Silver table integrity check for location_uuid\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """assigned_to is not null
                                                  and assigned_to is not in (select distinct colleagueUUID  
                                                  from delta_silver.colleague)""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_validity_assigned_to():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Silver table integrity check for assigned_to\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """length(assigned_to)!=36""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_consistency_assigned_to():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours silver table integrity check for assignedTo\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """assignedTo is null
                                                    and (status in ('claimed', 'assigned','deleted')
                                                  )""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_integrity_department_id():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours silver table integrity check for department_id\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """department_id is not in (select distinct skillCode 
                                                                              from delta_silver.skills)""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_department_id_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Silver table null check for department_id\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """department_id is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_validity_department_id():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours silver table integrity check for department_id\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """length(department_id)!=7""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_end_date_time_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Bronze table null check for start_date_time\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """start_date_time is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_validity_end_date_time():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Silver table null check for end_date_time\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """timestamp(endDateTime) < timestamp( startDateTime)""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_extra_hour_id_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Bronze table null check for extrahour_id\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """extrahour_id is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_validity_extra_hour_id():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours silver table null check for extraHourId\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """length(extrahour_id)!=7 
                                        or cast(extrahour_id as double) is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_integrity_store_id():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Silver table integrity check for store_id/location_uuid\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """store_id is not in (select distinct locationUuid  
                                                  from delta_silver.location)""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_store_id_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours silver table null check for store_id/location_uuid\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """store_id is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_validity_store_id():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Bronze table null check for store_id/location_uuid\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """length(store_id)!=36""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_consistency_published_timestamp():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours silver table integrity check for published_timestamp\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """published_timestamp is null
                                                    and status in ('claimed', 'assigned','published','deleted')""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_start_date_time_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Bronze table null check for start_date_time\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """start_date_time is null
                                                  """,
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_integrity_status():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Silver table integrity check for location_uuid\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """status is not in ('created','published','claimed',
                                                  'assigned','deleted')""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_status_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Silver table integrity check for location_uuid\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """status is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_updated_timestamp_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Silver table null check for updated_timestamp\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """updated_timestamp is null""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


# ---------------Silver DQ check Done----------------------

# ---------------Gold DQ check START----------------------


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_integrity_skills_id():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table integrity check for skills_id/department_id\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """Skills_ID is not in (select distinct skillCode 
                                                                              from delta_silver.skills)""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_skills_id_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table null check for skills_id/department_id\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """Skills_ID is null""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_validity_skills_id():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table integrity check for skills_id/department_id\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """length(Skills_ID)!=7""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_integrity_department_name():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table integrity check for department_name\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """department_name is not in (select distinct skillName 
                                                                              from delta_silver.skills)""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_department_name_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table null check for department_name\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """department_name is null""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_validity_department_name():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table integrity check for department_name\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """length(department_name)>5""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_extra_hours_date_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table null check for extra_hours_date\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """Extra_Hours_date is null""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_validity_extra_hours_date():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table null check for extra_hours_date\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """date(Extra_Hours_date) is null""",
                                      "type": [{"name": "count", "query": None}]}
                                     ]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_integrity_store_id():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table integrity check for store_id/location_uuid\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """store_id is not in (select distinct locationUuid  
                                                  from delta_silver.location)""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_store_id_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table null check for store_id/location_uuid\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """store_id is null""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_validity_store_id():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table null check for store_id/location_uuid\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """length(store_id)!=36""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_integrity_store_name():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table integrity check for store_name/LocationInternalname\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """store_name is not in (select 
                                       distinct LocationInternalname  
                                                  from delta_silver.location)""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_store_name_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table null check for store_name/LocationInternalname\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """store_name is null""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_uniqueness_store_name():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table uniqueness check for store_name/LocationInternalname\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """cast(store_name as string) is null""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_integrity_store_number():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table integrity check for store_number/StoreNumber\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """store_number is not in (select 
                                       distinct StoreNumber  
                                                  from delta_silver.location)""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_store_number_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table null check for store_number/StoreNumber\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """store_number is null""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_validity_store_number():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table Validity check for store_number/StoreNumber\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """length(store_number)!=36""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0
