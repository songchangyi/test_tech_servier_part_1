"""Main"""

import os
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col
from loguru import logger
from dotenv import load_dotenv
from processing import data_cleaning, read_file, write_to_json # pylint: disable=import-error


load_dotenv()

FILE_REF_LIST = [
    os.getenv("FILE_REF_PUBMED_CSV"),
    os.getenv("FILE_REF_PUBMED_JSON"),
    os.getenv("FILE_REF_CLINICAL"),
]


def main():
    """ETL process"""
    spark = SparkSession.builder.appName("technical_test_servier").getOrCreate()

    # Step 1 : load drugs file
    file_path = os.path.join(os.getenv("DATA_DIR"), os.getenv("FILE_CORE_DRUGS"))
    logger.info(f"Start loading drugs file from : {file_path}")
    df_drugs = read_file(spark, file_path)
    df_drugs = df_drugs.withColumn("drug_low", lower(col("drug")))
    df_drugs = df_drugs.fillna("")
    logger.info("Loading completed")

    # Step 2 : load ref file & process
    dfs = []
    for file in FILE_REF_LIST:
        temp_file_path = os.path.join(os.getenv("DATA_DIR"), file)

        logger.info(f"Start loading reference file from : {temp_file_path}")
        temp_df = read_file(spark, temp_file_path)
        temp_df = data_cleaning(temp_df)
        logger.info("Loading completed")

        dfs.append(temp_df)

    # Step 3 : join
    df_ref = reduce(lambda df1, df2: df1.unionByName(df2), dfs)
    df_res = df_ref.join(
        df_drugs, df_ref.title_low.contains(df_drugs.drug_low), how="left"
    )

    # Step 4 : format result
    columns_to_drop = ["title_low", "drug_low"]
    df_res = df_res.drop(*columns_to_drop)
    df_res = df_res.fillna("")
    df_res.show()

    # Step 5 : Save results
    output_path = os.path.join(
        os.getenv("OUTPUT_PATH"), os.getenv("FILE_OUTPUT_RESULTS")
    )
    logger.info(f"Saving results to '{output_path}'")
    write_to_json(df_res, output_path)
    logger.info("ETL job finished !")


if __name__ == "__main__":
    main()
