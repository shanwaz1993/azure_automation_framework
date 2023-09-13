from pdp_framework import PdpAutomation
from utils.utils import *

file_prefix = "PRP_ExtraHoursAPI_ExtraHours"

existing_bronze_silver_run_id = '8823b715-3fde-11ee-97a8-bcf4d42f600c'  # Define if running withrunid mark
existing_data_file = 'PRP_ExtraHoursAPI_ExtraHours_20230821-477.csv'             # Define if running withrunid mark


# Tested
@pytest.mark.datagen
def test_pdp_data_gen():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Generate data\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)
    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["data_generator"]
    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


# Tested
@pytest.mark.filehandler
def test_pdp_upload_processed():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Upload data to azure blob with correct file\n""")
    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["data_generator", "upload_to_azure"]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


# Tested
@pytest.mark.filehandler
def test_pdp_upload_name_error():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Upload data to azure blob with incorrect file name\n""")
    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)
    # wrong File name
    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}s_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                             dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                             random_number=random_number)

    config["pdp_flow"] = ["data_generator", "upload_to_azure"]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 1


# Tested
@pytest.mark.filehandler
def test_pdp_upload_extn_error():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Upload data to azure blob with incorrect file extension\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)
    # Wrong File Extension
    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.txt".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)

    config["pdp_flow"] = ["data_generator", "upload_to_azure"]
    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 1


# Tested
@pytest.mark.filehandler
def test_pdp_upload_header_error():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Upload data to azure blob with incorrect file Header/Columns\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["data_generator", "upload_to_azure"]

    dimension_data = [{"name": "location", "columns": ["locationUUID", "StoreNumber", "LocationInternalname"],
                       "renamed_map": {"locationUUID": "location_Uuid", "StoreNumber": "Store_Number",
                      "LocationInternalname": "Store_Name"}, "records": 50},
                      {"name": "skills", "columns": ["skillCode", "skillName"],
                       "renamed_map": {"skillCode": "department_Id", "skillName": "Department_Name"}, "records": 50},
                      {"name": "colleague", "columns": ["colleagueUUID"], "renamed_map": {"colleagueUUID": "updatedBy"},
                       "records": 50}
                      ]   # Renamed columns with wrong name

    output_schema = ["extraHourId", "status", "location_Uuid", "department_Id", "assignedTo",
                     "startDateTime", "endDateTime", "updatedTimestamp",
                     "updatedBy", "createdTimestamp", "publishedTimestamp"]
    kpi_calculation = None
    config["data_generator"]["dimension_data"] = dimension_data
    config["data_generator"]["output_schema"] = output_schema
    config["data_generator"]["kpi_calculation"] = kpi_calculation
    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 1


# Tested
@pytest.mark.filehandler
def test_pdp_upload_content_error():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Upload data to azure blob with incorrect file Content\n""")
    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)
    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["data_generator", "upload_to_azure"]

    config["data_generator"]["dimension_data"] = [{"name": "location", "columns": ["locationUUID", "StoreNumber",
                                                                                   "LocationInternalname"],
                                                   "renamed_map": {"locationUUID": "location_Uuid",
                                                                   "StoreNumber": "Store_Number",
                                                                   "LocationInternalname": "Store_Name"},
                                                   "records": 50},
                                                  {"name": "skills", "columns": ["skillCode", "skillName"],
                                                   "renamed_map": {"skillCode": "department_Id",
                                                                   "skillName": "Department_Name"},
                                                   "records": 50},
                                                  {"name": "colleague", "columns": ["colleagueUUID"],
                                                   "renamed_map": {"colleagueUUID": "updatedBy"},
                                                   "records": 50}
                                                  ]

    config["data_generator"]["output_schema"] = ["extraHourId", "status", "location_Uuid", "department_Id",
                                                 "assignedTo", "startDateTime", "endDateTime", "updatedTimestamp",
                                                 "updatedBy", "createdTimestamp"]   # Removed one column from file

    config["data_generator"]["kpi_calculation"] = None
    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 1


# Tested
@pytest.mark.filehandler
def test_pdp_upload_protected_error():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Upload data to azure blob with incorrect file Header/Columns\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    random_number = random.randint(100, 999)
    config["file"] = "PRP_ExtraHoursAPI_ExtraHours_20230811-500.csv"
    config["pdp_flow"] = ["upload_to_azure"]

    dimension_data = [
        {"name": "location", "columns": ["locationUUID"], "renamed_map": {"locationUUID": "location_Uuid"},
         "records": 50},
        {"name": "skills", "columns": ["skillCode"], "renamed_map": {"skillCode": "department_Id"}, "records": 50},
        {"name": "colleague", "columns": ["colleague_uuid"], "renamed_map": {"colleague_uuid": "updatedBy"},
         "records": 50}]  # Renamed columns with wrong name

    output_schema = ["extraHourId", "status", "location_Uuid", "department_Id", "assignedTo",
                     "startDateTime", "endDateTime", "updatedTimestamp",
                     "updatedBy", "createdTimestamp", "publishedTimestamp"]

    config["data_generator"]["dimension_data"] = dimension_data
    config["data_generator"]["output_schema"] = output_schema
    config["data_generator"]["kpi_calculation"] = None
    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 1


# Tested
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_count():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours if Bronze table count is same as input_count\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file
    config["pdp_flow"] = ["validations"]

    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["input_count"] = 100
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id
    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": "MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}'",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


# Tested
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_duplicate():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours if Bronze table have duplicates on primary keys : extraHourId,updatedTimestamp\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file
    config["pdp_flow"] = ["validations"]

    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id
    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": "MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}'",
                                        "type": [{"name": "duplicate", "cols": ["extraHourId", "updatedTimestamp"]}]}]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


# Tested
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_data():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours if Bronze table Data validation with input file\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file
    config["pdp_flow"] = ["validations"]

    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["input_count"] = 100
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id
    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}'""",
                                        "type": [{"name": "data",
                                                  "query": None,
                                                  "file": "{file}", "source_path": None,
                                                  "folder": "output_data", "header": True,
                                                  "select_cols": ["extraHourId", "status", "locationUuid",
                                                                  "departmentId",
                                                                  "assignedTo", "startDateTime",
                                                                  "endDateTime", "updatedTimestamp",
                                                                  "updatedBy", "createdTimestamp",
                                                                  "publishedTimestamp"],
                                                  "sort_column":["extraHourId", "updatedTimestamp"]}]}]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


# Tested
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_all_validations():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Bronze table all validation from config\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file
    config["pdp_flow"] = ["validations"]

    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["input_count"] = 100
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


# Tested
@pytest.mark.validation
@pytest.mark.bronze
def test_pdp_bronze_table_check_scd2():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours if Bronze table have history data\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file
    config["pdp_flow"] = ["validations"]

    config["validations"]["validation_flow"] = ["bronze"]
    config["validations"]["input_count"] = 0
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id
    config["validations"]["bronze"] = [{"db_table": "delta_bronze.prp_extrahoursapi_extrahours",
                                        "where": """MD_PIPELINE_RUN_ID != '{bronze_silver_run_id}'""",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 1


# Tested
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_count():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours if Silver table count is same as input_count\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file
    config["pdp_flow"] = ["validations"]

    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["input_count"] = 100
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id
    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": "MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}'",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


# Tested
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_duplicate():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours if Silver table have Duplicates on Primary Keys extrahour_id,updated_timestamp\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]

    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id
    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": "MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}'",
                                        "type": [{"name": "duplicate",
                                                  "cols": ["extrahour_id", "updated_timestamp"]}]}]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


# Tested
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_data():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Silver table data validation with input file\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file
    config["pdp_flow"] = ["validations"]
    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["input_count"] = 0
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id
    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": """MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}'""",
                                        "type": [{"name": "count",
                                                  "query": """select count(*) as count 
                                                  from delta_silver.extrahours slvr 
                                                  full outer join (select startDateTime,locationUuid,departmentId 
                                                  from delta_bronze.prp_extrahoursapi_extrahours 
                                                  where MD_PIPELINE_RUN_ID = '8823b715-3fde-11ee-97a8-bcf4d42f600c') brnz
                                                  on timestamp(slvr.start_Date_Time)=timestamp(brnz.startDateTime)
                                                  and slvr.store_id=brnz.locationUuid 
                                                  and slvr.department_Id= brnz.departmentId 
                                                  where MD_PIPELINE_RUN_ID = '8823b715-3fde-11ee-97a8-bcf4d42f600c' 
                                                  and brnz.departmentId is null or slvr.department_Id is null"""
                                                  }]}]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


# Tested
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_all_validations():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Silver table all validation from config\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file
    config["pdp_flow"] = ["validations"]

    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["input_count"] = 100
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


# Tested
@pytest.mark.validation
@pytest.mark.silver
def test_pdp_silver_table_check_scd2():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours if Silver table have history data\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file
    config["pdp_flow"] = ["validations"]

    config["validations"]["validation_flow"] = ["silver"]
    config["validations"]["input_count"] = 0
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id
    config["validations"]["silver"] = [{"db_table": "delta_silver.extrahours",
                                        "where": "MD_PIPELINE_RUN_ID != '{bronze_silver_run_id}'",
                                        "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 1


# Not Tested
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_data():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Gold table KPI validation\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]

    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": "True=True",
                                      "type": [{"name": "data",
                                                "query": """select * from delta_gold.extrahours gold 
                                                left join 
                                                (select DISTINCT date(startDateTime) as extra_hours_date,locationUuid,
                                                  departmentId 
                                                  from delta_bronze.prp_extrahoursapi_extrahours 
                                                  where MD_PIPELINE_RUN_ID = '{bronze_silver_run_id}') brnz 
                                                on  gold.extra_hours_date=brnz.extra_hours_date 
                                                and gold.Location_UUID=brnz.locationUuid 
                                                and gold.Skills_ID= brnz.departmentId 
                                                where brnz.extra_hours_date is not null""",
                                                "file": "kpi_{file}", "source_path": None,
                                                "folder": "output_data", "header": True,
                                                "select_cols": ["Location_UUID",
                                                                "Store_Number",
                                                                "Store_Name",
                                                                "Skills_ID",
                                                                "Department_Name",
                                                                "Extra_Hours_date",
                                                                "Published_Shifts",
                                                                "Claimed_Shifts",
                                                                "Unclaimed_shifts",
                                                                "Published_unclaimed_manually_assigned_shifts",
                                                                "Unpublished_manually_assigned_shifts"],
                                                "sort_column": ["Location_UUID", "Skills_ID", "Extra_Hours_date"]}]}]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0
