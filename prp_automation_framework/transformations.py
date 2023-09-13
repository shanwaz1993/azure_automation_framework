from utils.utils import *


def extrahours_serve_data(df):
    df = df.with_columns((pl.col("Published_Shifts")//2).alias("Claimed_Shifts"))
    df = df.with_columns(((pl.col("Published_Shifts")-pl.col("Claimed_Shifts")) // 2).alias("Unclaimed_shifts"))
    df = df.with_columns((pl.col("Published_Shifts") - pl.col("Claimed_Shifts") - pl.col("Unclaimed_shifts"))
                         .alias("Published_unclaimed_manually_assigned_shifts"))
    return df


def extrahours_api(df, **kwargs):
    df = df.with_columns((pl.col("startDateTime").str.strptime(pl.Datetime) + pl.duration(seconds=3600*5))
                         .dt.strftime("%Y-%m-%dT%H:%M:%S.000")
                         .alias("endDateTime"))
    df = df.with_columns((pl.col("startDateTime").str.strptime(pl.Datetime) + pl.duration(days=-2, seconds=3600*5))
                         .dt.strftime("%Y-%m-%dT%H:%M:%S.000")
                         .alias("createdTimestamp"))
    df = df.with_columns(pl.when(pl.col("status") == "created").then(pl.col("createdTimestamp"))
                         .when(pl.col("status") == "pending")
                         .then((pl.col("createdTimestamp").str.strptime(pl.Datetime) + pl.duration(seconds=120))
                               .dt.strftime("%Y-%m-%dT%H:%M:%S.000"))
                         .when(pl.col("status") == "deleted")
                         .then((pl.col("createdTimestamp").str.strptime(pl.Datetime) + pl.duration(seconds=60))
                               .dt.strftime("%Y-%m-%dT%H:%M:%S.000"))
                         .when(pl.col("status") == "published")
                         .then((pl.col("createdTimestamp").str.strptime(pl.Datetime) + pl.duration(seconds=1))
                               .dt.strftime("%Y-%m-%dT%H:%M:%S.000"))
                         .when(pl.col("status") == "claimed")
                         .then((pl.col("createdTimestamp").str.strptime(pl.Datetime) + pl.duration(seconds=3600*2))
                               .dt.strftime("%Y-%m-%dT%H:%M:%S.000"))
                         .when(pl.col("status") == "assigned")
                         .then((pl.col("createdTimestamp").str.strptime(pl.Datetime) + pl.duration(seconds=3600*3))
                               .dt.strftime("%Y-%m-%dT%H:%M:%S.000"))
                         .otherwise(pl.col("createdTimestamp"))
                         .alias("updatedTimestamp"))
    df = df.with_columns(pl.when(pl.col("status").is_in(["published", "claimed"]))
                         .then(pl.col("updatedTimestamp")).otherwise(pl.lit(None))
                         .alias("publishedTimestamp"))

    df = df.with_columns(pl.when(pl.col("status").eq("assigned"))
                          .then(pl.col("updatedTimestamp").apply(lambda p: random.choice([p, None])))
                          .otherwise(pl.col("publishedTimestamp"))
                          .alias("publishedTimestamp"))

    df = df.with_columns(pl.when(pl.col("status").is_in(["assigned", "claimed"]))
                         .then(pl.col("updatedBy")).otherwise(pl.lit(None))
                         .alias("assignedTo"))
    return df


def extrahours_kpi_calculation(df, **kwargs):
    df = df.with_columns((pl.col("startDateTime").str.strptime(pl.Datetime))
                         .dt.strftime("%Y-%m-%d")
                         .alias("Extra_Hours_date"))

    df = df.with_columns(pl.when((pl.col("status").ne("created"))
                                 .and_(pl.col("publishedTimestamp").is_not_null()))
                         .then(pl.lit(1)).otherwise(pl.lit(0))
                         .alias("Published_shifts_flag"))\
        .with_columns(pl.when((pl.col("status").eq("claimed"))
                              .and_(pl.col("publishedTimestamp").is_not_null()))
                      .then(pl.lit(1)).otherwise(pl.lit(0))
                      .alias("Claimed_shifts_flag"))\
        .with_columns(pl.when((pl.col("status").eq("published"))
                              .and_(pl.col("publishedTimestamp").is_not_null())
                              .and_(pl.col("assignedTo").is_null()))
                      .then(pl.lit(1)).otherwise(pl.lit(0))
                      .alias("Unclaimed_shifts_flag")) \
        .with_columns(pl.when((pl.col("status").eq("assigned"))
                              .and_(pl.col("publishedTimestamp").is_not_null())
                              .and_(pl.col("assignedTo").is_not_null()))
                      .then(pl.lit(1)).otherwise(pl.lit(0))
                      .alias("Published_unclaimed_manually_assigned_shifts_flag"))\
        .with_columns(pl.when((pl.col("status").eq("assigned"))
                              .and_(pl.col("publishedTimestamp").is_null())
                              .and_(pl.col("assignedTo").is_not_null()))
                      .then(pl.lit(1)).otherwise(pl.lit(0))
                      .alias("Unpublished_manually_assigned_shifts_flag"))\
        .groupby(pl.col("locationUuid"), pl.col("Store_Number"), pl.col("Store_Name"), pl.col("departmentId"), pl.col("Department_Name"), pl.col("Extra_Hours_date"))\
        .agg([pl.col("Published_shifts_flag").sum()
             .alias("Published_Shifts"),
             pl.col("Claimed_shifts_flag").sum()
             .alias("Claimed_Shifts"),
             pl.col("Unclaimed_shifts_flag").sum()
             .alias("Unclaimed_shifts"),
             pl.col("Published_unclaimed_manually_assigned_shifts_flag").sum()
             .alias("Published_unclaimed_manually_assigned_shifts"),
             pl.col("Unpublished_manually_assigned_shifts_flag").sum()
             .alias("Unpublished_manually_assigned_shifts")])
    # print_kpi(df)
    df = df.rename({"locationUuid": "Location_UUID", "departmentId": "Skills_ID"})
    write_csv(df,
              base_folder=os.path.dirname(__file__),
              folder="output_data",
              output_file=kwargs["kpi_file"])
    return df
