from transformations import *
from utils.utils import *


def join_dimension_dataframe(dimension_dataframe, how=None):
    df = pl.DataFrame()
    for dimension in dimension_dataframe.keys():
        value = dimension_dataframe[dimension]
        if df.is_empty():
            df = value
        else:
            df = df.join(value, how=how)
    return df


def read_dimension_dataframe(dimension_list, folder):
    dimension_dataframe = {}
    for dimension in dimension_list:
        file_path = os.path.join(os.path.dirname(__file__), folder, f"{dimension['name']}.csv")
        data = pl.read_csv(source=file_path,
                           columns=dimension["columns"],
                           has_header=True).select(dimension["columns"])
        if dimension["renamed_map"] is not None:
            data = data.rename(dimension["renamed_map"])
        if dimension.get("records", None) is not None:
            dimension_dataframe[dimension['name']] = data.sample(n=dimension["records"],
                                                                 shuffle=True)
        else:
            dimension_dataframe[dimension['name']] = data
    df = join_dimension_dataframe(dimension_dataframe, how="cross")
    return df


def add_date_dimension_columns(df, column_names):
    for elem in column_names:
        date_range = pl.date_range(start=datetime.strptime(elem["start"], elem["input_format"]),
                                   end=datetime.strptime(elem["end"], elem["input_format"]),
                                   interval=elem["step"],
                                   eager=True)
        date_df = pl.DataFrame({elem["column"]: date_range.dt.strftime(elem["output_format"])})
        df = df.join(date_df, how="cross")
    return df


def add_int_dimension_columns(df, column_names):
    for elem in column_names:
        int_range = np.unique(np.random.randint(elem["start"], elem["end"],
                                                size=int((elem["end"]-elem["start"])/elem["step"])))
        int_df = pl.DataFrame({elem["column"]: int_range})
        df = df.join(int_df, how="cross")
    return df


def add_random_date_columns(df, column_names):
    for elem in column_names:
        date_values = pl.date_range(start=datetime.strptime(elem["start"], elem["input_format"]),
                                    end=datetime.strptime(elem["end"], elem["input_format"]),
                                    interval=elem["step"],
                                    eager=True).sample(n=df.height,
                                                       with_replacement=True, shuffle=True)
        df = df.with_columns(date_values.dt.strftime(elem["output_format"]).alias(elem["column"]))
    return df


def add_random_int_columns(df, column_names):
    for elem in column_names:
        int_values = pl.arange(start=elem["start"],
                               end=elem["end"]+1,
                               step=elem["step"],
                               eager=True).sample(n=df.height, with_replacement=True, shuffle=True)
        df = df.with_columns(int_values.alias(elem["column"]))
    return df


def add_null_columns(df, column_names):
    for elem in column_names:
        df = df.with_columns(pl.lit(None).cast(pl.Int64).alias(elem["column"]))
    return df


def add_random_str_columns(df, column_names):
    for elem in column_names:
        str_values = pl.Series(elem["sample"]).sample(n=df.height,
                                                      with_replacement=True,
                                                      shuffle=True)
        df = df.with_columns(str_values.alias(elem["column"]))
    return df


def add_load_date_columns(df, column_names):
    for elem in column_names:
        if elem["load_date"] is None:
            df = df.with_columns((pl.lit(datetime.now().strftime(elem["format"])).alias(elem["column"])))
        else:
            df = df.with_columns((pl.lit(elem["load_date"])).alias(elem["column"]))
    return df


def transformation(df, f, **kwargs):
    df = f(df, **kwargs)
    return df


def data_generator(config=None):
    # Read Argument
    if config is None:
        config = read_config(base_folder=os.path.dirname(__file__),
                             key="data_generator")

    if config["file"] is None:
        logging.error(f"--file_name is neither passed in parameter nor in config json", exc_info=True)
        raise Exception(f"--file_name is neither passed in parameter nor in config json")

    # Creating Empty Dataframe
    df = pl.DataFrame()

    # Generating data with dimensions required
    if config.get("dimension_data", None) is not None:
        df = read_dimension_dataframe(config.get("dimension_data", None),
                                      config.get("dimension_data_folder", None))

    # Generating data with add_date_dimension_columns
    if config.get("date_dimension_columns", None) is not None:
        df = add_date_dimension_columns(df, config.get("date_dimension_columns", None))

    # Generating data with add_date_dimension_columns
    if config.get("int_dimension_columns", None) is not None:
        df = add_int_dimension_columns(df, config.get("int_dimension_columns", None))

    # Generating data with random_date_columns
    if config.get("random_date_columns", None) is not None:
        df = add_random_date_columns(df, config.get("random_date_columns", None))

    # Generating data with random_date_columns
    if config.get("load_date_columns", None) is not None:
        df = add_load_date_columns(df, config.get("load_date_columns", None))

    # Generating data with aggregated_int_columns
    if config.get("random_int_columns", None) is not None:
        df = add_random_int_columns(df, config.get("random_int_columns", None))

    # Generating data with aggregated_int_columns
    if config.get("random_str_columns", None) is not None:
        df = add_random_str_columns(df, config.get("random_str_columns", None))

    # Generating data with aggregated_int_columns
    if config.get("null_columns", None) is not None:
        df = add_null_columns(df, config.get("null_columns", None))

    if config.get("transformation", None) is not None:
        df = transformation(df, eval(config.get("transformation", None)))

    if config.get("drop_duplicates", None) is not None:
        df = df.unique(subset=config["drop_duplicates"]["column"], keep='first')

    if config.get("output_count", None) is not None:
        df = df.sample(n=config.get("output_count", None),
                       shuffle=True,
                       with_replacement=True)

    if config.get("kpi_calculation", None) is not None:
        transformation(df, eval(config.get("kpi_calculation", None)),
                       kpi_file="kpi_"+config.get("file", None))

    if config.get("output_schema", None) is not None:
        df = df.select(config.get("output_schema", None))

    if config.get("file", None) is not None:
        write_csv(df, base_folder=os.path.dirname(__file__),
                  folder=config.get("output_folder", None),
                  output_file="non_utf8-"+config.get("file", None))

    if config.get("utf8_encoding", None) is not None:
        convert_csv_to_utf8(base_folder=os.path.dirname(__file__),
                            folder=config.get("output_folder", None),
                            input_file="non_utf8-"+config.get("file", None),
                            output_file=config.get("file", None))
        logging.info(f"\tData Saved in File : {config.get('file', None)}")

    if config.get("print_final_df_sample", None):
        print_shape(df, "final_df")

    details_dict = {"file_name": config.get("file", None),
                    "output_count": config.get("output_count", None),
                    "columns_count": len(df.columns),
                    "columns": df.columns,
                    "Status": "SUCCESS"}

    base_html_content = add_segment_content(tag="data_generator",
                                            value_dict=details_dict,
                                            base_content=config.get("base_html_content", None))

    x_comm = {"input_count": df.height, "base_html_content": base_html_content}

    return 0, x_comm


if __name__ == "__main__":
    logging.basicConfig(filename='logs/data_generator.log',
                        filemode='w', encoding='utf-8', level=logging.INFO)
    logging.info("-------------------------Data Generator START----------------------------------")
    start_time = datetime.now()
    logging.info(f"mock data start_time {start_time}")
    data_generator()
    time_taken = datetime.now()-start_time
    logging.info(f"mock data Time taken {time_taken}")
    logging.info("-------------------------Data Generator END----------------------------------")
