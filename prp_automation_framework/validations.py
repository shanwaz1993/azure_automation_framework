from utils.utils import *
from polars.testing import assert_frame_equal


def run_sql_adb(query):
    try:
        azure_details_dict = get_env_vars()
        adb_server_host = azure_details_dict.get("DATABRICKS_SERVER_HOSTNAME")
        adb_http_path = azure_details_dict.get("DATABRICKS_HTTP_PATH")
        adb_token = azure_details_dict.get("DATABRICKS_TOKEN")
    except Exception as e:
        logging.error(f"\tEnvironment credentials are missing - {e}", exc_info=True)
        raise Exception(f"\tEnvironment credentials are missing - {e}")

    try:
        with sql.connect(server_hostname=adb_server_host,
                         http_path=adb_http_path,
                         access_token=adb_token
                         ) as connection:

            with connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()

    except Exception as e:
        logging.error(f"\tError while running Query- {e}", exc_info=True)
        raise Exception(f"\tError while running Query- {e}")


def get_df(result):
    data = [row.asDict() for row in result]
    return pl.DataFrame(data)


def get_select_query(q_type, db_table, cols=None, where=None, limit=None):
    query_details = {
        "table": db_table,
        "cols": ', '.join(cols) if cols is not None else ' *',
        "where": where if where is not None else "1=1",
        "limit": limit if limit is not None else 20
    }

    if q_type.lower() == "select":
        query = """SELECT {cols}  FROM {table} WHERE {where} limit {limit}""".format(**query_details)
    elif q_type.lower() == "count":
        query = """SELECT COUNT(*) as count  FROM {table} WHERE {where} limit {limit}""".format(**query_details)
    elif q_type.lower() == "distinct_count":
        query = """SELECT COUNT(*) as count FROM (SELECT distinct {cols}  FROM {table} 
        WHERE {where}) limit {limit}""".format(**query_details)
    else:
        logging.error("""Incorrect values passed, Please varify table: {table}, q_type: {q_type}, cols:{cols}, 
         where: {where}, limit: {limit} """.format(**query_details))
        raise Exception("""Incorrect values passed, Please varify table: {table}, q_type: {q_type}, cols:{cols}, 
         where: {where}, limit: {limit} """.format(**query_details))

    logging.info(f"\tQuery Generated is: {query}")
    return query


def count_validation(input_count, query=None, db_table=None,  where=None):
    logging.info(f"\tInput Query is {query}")
    if query is None:
        query = get_select_query(q_type="count", db_table=db_table, where=where, limit=1)
    else:
        logging.info(f"\tRunning for Input Query {query}")

    result = run_sql_adb(query)
    adb_count = result[0].asDict()
    logging.info(f"\tTable data Count: {adb_count['count']}")
    logging.info(f"\tInput Data Count: {input_count}")
    if adb_count["count"] == input_count:
        logging.info("\tCount Matched")
        status = True
    else:
        logging.info("\tCount Not Matched")
        logging.info("\tGetting sample data")
        query = query.replace("SELECT COUNT(*) as count", "SELECT *")
        result = run_sql_adb(query)
        if len(result) > 0:
            adb_sample_data = result[0].asDict()
            logging.info(f"\tTable sample data: {adb_sample_data}")
        status = False
    return status


def duplicate_validation(db_table, cols=None, where=None):
    query_full_count = get_select_query(q_type="count",
                                        db_table=db_table,
                                        where=where)
    query_distinct_count = get_select_query(q_type="distinct_count",
                                            db_table=db_table,
                                            where=where, cols=cols)

    full_count = run_sql_adb(query_full_count)
    distinct_count = run_sql_adb(query_distinct_count)
    adb_full_count = full_count[0].asDict()
    adb_distinct_count = distinct_count[0].asDict()

    logging.info(f"\tTable {db_table} full data Count: {adb_full_count['count']}")
    logging.info(f"\tTable {db_table} distinct data Count: {adb_distinct_count['count']}")

    if adb_full_count['count'] == adb_distinct_count['count']:
        logging.info("\tNO Duplicates Found")
        status = True
    else:
        logging.info("\tDuplicates Found")
        status = False
    return status


def data_validation(db_table, query=None, file_path=None, select_cols=None, sort_column=None, where=None, has_header=True):
    # Read source Dataframe
    source_data = pl.read_csv(source=file_path, has_header=has_header)

    logging.info(f"\tInput Query is: {query}")
    # Read target Dataframe
    if query is None:
        query = get_select_query(q_type="select", db_table=db_table,
                                 where=where, limit=source_data.height * 2)
    else:
        logging.info(f"\tRunning for Input Query {query}")

    adb_data = run_sql_adb(query)
    adb_data_df = get_df(adb_data)

    # Creating temp file for data validation
    write_csv(adb_data_df, base_folder=os.path.dirname(__file__),
              folder="output_data",
              output_file="non_utf8-" + "adb_data_df.csv")

    convert_csv_to_utf8(base_folder=os.path.dirname(__file__),
                        folder="output_data",
                        input_file="non_utf8-" + "adb_data_df.csv",
                        output_file="adb_data_df.csv")

    adf_file_path = os.path.join(os.path.dirname(__file__), "output_data", "adb_data_df.csv")
    adb_data_df = pl.read_csv(source=adf_file_path, has_header=True)

    # Select required columns
    if select_cols is not None:
        source_data = source_data.select(select_cols)
        adb_data_df = adb_data_df.select(select_cols)

    # Perform sorting
    if sort_column is not None:
        source_data = source_data.sort(sort_column, descending=True)
        adb_data_df = adb_data_df.sort(sort_column, descending=True)
    try:
        # Compare dataframe
        if pl.testing.assert_frame_equal(source_data, adb_data_df) is None:
            logging.info("\n\tData Matched")
            status = True
        else:
            logging.info("\n\tData Mismatched")
            status = False
    except Exception as e:
        logging.error(f"\tError while comparing data {e}", exc_info=True)
        status = False

    # Deleting temp file
    delete_file(base_folder=os.path.dirname(__file__),
                folder="output_data",
                file_name="adb_data_df.csv")

    return status


def date_format_validation(db_table, cols, where, date_format="%Y-%m-%dT%H:%M:%S.%f"):
    # Read target Dataframe
    query = get_select_query(q_type="select", db_table=db_table,
                             where=where,
                             cols=cols,
                             limit=1)

    adb_data = run_sql_adb(query)
    data = adb_data[0].asDict()
    logging.info(f"date value  {data}")
    for key, value in data.items():
        try:
            if "datetime.datetime" in f"{data}":
                # value = str(value).replace(" ", "T")[:19] + ".000"
                value = str(value).replace(" ", "T")[:19] + ".000"
            elif "T" in f"{value}":
                value = str(value)
            else:
                value = "Error"

            logging.info(f"\tdate column {key} value is {value}")
            # input_format = datetime.strptime(value[:19], date_format[:17]).strftime(date_format)
            input_format = datetime.strptime(value, date_format).strftime(date_format)[:-3]
            logging.info(f"\tAfter formatting date value {input_format}")
            if value == input_format:
                logging.info(f"\tDate Formate matched for column {key}")
                status = True
            else:
                logging.info(f"\tDate Formate does not matched for column {key}")
                status = False
        except ValueError as e:
            logging.error(f"\tError while converting date format for column {key} value {value} format {date_format}",
                          exc_info=True)
            status = False
    return status


def cols_exist_validation(db_table, cols, where):
    # Read target Dataframe
    query = get_select_query(q_type="select", db_table=db_table,
                             where=where,
                             cols=cols,
                             limit=1)
    try:
        run_sql_adb(query)
        logging.info(f"Cols Exist")
        status = True
    except Exception as e:
        logging.error(f"\tColumn not found {e}", exc_info=True)
        status = False

    return status


def get_val_status(status):
    if status is True:
        return "PASS"
    elif status is False:
        return "FAILED"
    else:
        return "ERROR"


def run_validations(layer, config):
    details_dict = {"count": "Not Executed",
                    "duplicate": "Not Executed",
                    "data": "Not Executed",
                    "date_format": "Not Executed",
                    "Invalid": "No Invalid Found",
                    "layer_name": layer.replace("_", " ").title()}

    val = config.get(layer, None)
    comb_status = True
    for elem in val:
        for val_typ in elem["type"]:
            if "count" == val_typ["name"]:
                status = count_validation(config["input_count"],
                                          query=str(val_typ["query"]).format(**config) if val_typ["query"] is not None
                                          else None,
                                          db_table=elem["db_table"],
                                          where=str(elem.get("where")).format(**config))
                details_dict["count"] = get_val_status(status)
                comb_status = comb_status and status
            elif "duplicate" == val_typ["name"]:
                status = duplicate_validation(db_table=elem["db_table"],
                                              cols=val_typ["cols"],
                                              where=str(elem.get("where")).format(**config))
                details_dict["duplicate"] = get_val_status(status)
                comb_status = comb_status and status
            elif "data" == val_typ["name"]:
                if config["file"] is None:
                    logging.error(f"--file_name is neither passed in parameter nor in config json")
                    raise Exception(f"--file_name is neither passed in parameter nor in config json")

                if val_typ["source_path"] is None:
                    file_path = os.path.join(os.path.dirname(__file__), val_typ["folder"],
                                             val_typ["file"].format(**config))
                else:
                    file_path = val_typ["source_path"]

                status = data_validation(db_table=elem["db_table"],
                                         query=str(val_typ["query"]).format(**config) if val_typ["query"] is not None
                                         else None,
                                         file_path=file_path,
                                         where=str(elem.get("where")).format(**config),
                                         select_cols=val_typ["select_cols"],
                                         sort_column=val_typ["sort_column"],
                                         has_header=val_typ["header"]
                                         )
                details_dict["data"] = get_val_status(status)
                comb_status = comb_status and status

            elif "date_format" == val_typ["name"]:
                status = date_format_validation(db_table=elem["db_table"],
                                                cols=val_typ["date_columns"],
                                                where=str(elem.get("where")).format(**config),
                                                date_format=val_typ["format"])
                details_dict["date_format"] = get_val_status(status)
                comb_status = comb_status and status
            elif "cols_exist" == val_typ["name"]:
                status = cols_exist_validation(db_table=elem["db_table"],
                                               cols=val_typ["columns"],
                                               where=str(elem.get("where")).format(**config))
                details_dict["date_format"] = get_val_status(status)
                comb_status = comb_status and status
            else:
                details_dict["Invalid_Type"] = "Invalid validation Type"
                logging.info(f"\t{val_typ['name']} does not match with any validation")
                raise Exception(f"\t{val_typ['name']} does not match with any validation")

    base_html_content = add_segment_content(tag="validations",
                                            value_dict=details_dict,
                                            base_content=config.get("base_html_content", None))
    return comb_status, base_html_content


def validations(config=None):
    if config is None:
        config = read_config(base_folder=os.path.dirname(__file__),
                             key="validations")
    comb_status = True
    for layer in config.get("validation_flow", None):
        logging.info(f"\tRunning Validation for  {layer}")
        status, base_html_content = run_validations(layer=layer, config=config)
        comb_status = comb_status and status
        logging.info(f"\tCompleted Validation for {layer}")
        config["base_html_content"] = base_html_content

    if comb_status:
        status = 0
    else:
        status = 1

    return status, {"base_html_content": base_html_content}


if __name__ == "__main__":
    logging.basicConfig(filename='logs/validation.log',
                        filemode='w', encoding='utf-8', level=logging.INFO)
    logging.info("-------------------------Validations START----------------------------------")
    start_time = datetime.now()
    validations()
    print(f"validation Time taken {datetime.now()-start_time}")
    logging.info("-------------------------Validations END----------------------------------")
