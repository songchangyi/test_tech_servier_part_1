"""ETL Functions"""

import re
import tempfile
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col, lit, to_date


def read_file(spark: SparkSession, file_path: str):
    """Load file using spark"""
    try:
        file_type = file_path.split(".")[-1]
        if file_type == "csv":
            df = read_csv(spark, file_path)
        elif file_type == "json":
            df = read_json(spark, file_path)
        else:
            raise TypeError(f"Unsupported file type : {file_type}")
        return df
    except IOError as err:
        raise IOError(f"Import file failed : {err}") from err


def read_csv(spark: SparkSession, file_path: str):
    """Load a CSV file with header"""
    return spark.read.csv(file_path, header=True)


def read_json(spark: SparkSession, file_path: str):
    """Load a json file"""
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    match = re.search(r"\},\n\]", content)
    if match:  # the file is corrupted when matched
        fixed_content = re.sub(r"\},\n\]", "}\n]", content)  # remove the last comma
        with tempfile.NamedTemporaryFile(
            delete=False, mode="w", suffix=".json"
        ) as temp_file:
            temp_file.write(fixed_content)
        df = spark.read.json(temp_file.name, multiLine="true")
    else:
        df = spark.read.json(file_path, multiLine="true")
    return df


def data_cleaning(df: pyspark.sql.dataframe.DataFrame):
    """Perform data cleaning"""
    if "scientific_title" in df.columns:
        df = df.withColumnRenamed("scientific_title", "title")
        df = df.withColumn("source", lit("clinical trials"))
    else:
        df = df.withColumn("source", lit("pubmed"))

    if "title" in df.columns:
        df = df.withColumn("title_low", lower(col("title")))

    df = df.withColumn("date", to_date("date"))
    df = df.fillna("")
    return df.select("id", "title", "date", "journal", "source", "title_low")


def write_to_json(df: pyspark.sql.dataframe.DataFrame, output_path: str):
    """Write the given dataframe to json"""
    df.coalesce(1).write.mode("overwrite").format("json").save(output_path)
