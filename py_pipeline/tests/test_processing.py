import os
import tempfile
import pytest

from pyspark.sql import SparkSession

from py_pipeline.etl import processing


# -----------------------------------------------------------------------------
# Fixtures and Helpers
# -----------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("ETL Functions Tests").getOrCreate()
    yield spark
    spark.stop()


# -----------------------------------------------------------------------------
# Tests for File Reading Functions
# -----------------------------------------------------------------------------


def test_read_csv(spark):
    csv_content = "id,name\n1,Alice\n2,Bob"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
        tmp.write(csv_content)
        tmp_path = tmp.name

    try:
        df = processing.read_csv(spark, tmp_path)
        data = df.collect()

        assert len(data) == 2
        assert data[0]["id"] == "1"
    finally:
        os.unlink(tmp_path)


def test_read_json_valid(spark):
    json_content = """
    [
        {"id": "1", "value": "A"},
        {"id": "2", "value": "B"}
    ]
    """
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        tmp.write(json_content)
        tmp_path = tmp.name

    try:
        df = processing.read_json(spark, tmp_path)
        data = df.collect()
        assert len(data) == 2

        ids = {row["id"] for row in data}
        assert "1" in ids and "2" in ids
    finally:
        os.unlink(tmp_path)


def test_read_file_unsupported(spark):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as tmp:
        tmp.write("Just some text")
        tmp_path = tmp.name

    try:
        with pytest.raises(TypeError):
            processing.read_file(spark, tmp_path)
    finally:
        os.unlink(tmp_path)


# -----------------------------------------------------------------------------
# Tests for Data Cleaning Function
# -----------------------------------------------------------------------------


def test_data_cleaning_with_scientific_title(spark):
    # Create a DataFrame with a 'scientific_title' column.
    data = [("1", "Paper A", "2020-01-01", "Journal A")]
    columns = ["id", "scientific_title", "date", "journal"]
    df = spark.createDataFrame(data, columns)

    df_cleaned = processing.data_cleaning(df)
    result = df_cleaned.collect()[0]

    assert "scientific_title" not in df_cleaned.columns
    assert result["title"] == "Paper A"
    assert result["source"] == "clinical trials"
    assert result["title_low"] == "paper a"
    assert result["date"].strftime("%Y-%m-%d") == "2020-01-01"


def test_data_cleaning_without_scientific_title(spark):
    # Create a DataFrame without a 'scientific_title' column.
    data = [("1", "Paper B", "2020-01-01", "Journal B")]
    columns = ["id", "title", "date", "journal"]
    df = spark.createDataFrame(data, columns)

    df_cleaned = processing.data_cleaning(df)
    result = df_cleaned.collect()[0]

    assert result["source"] == "pubmed"
    assert result["title_low"] == "paper b"
