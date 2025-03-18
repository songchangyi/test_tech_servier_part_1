# Servier Technical Test

## Part I : Data Pipeline

### Objective
Given medical data about drugs and journals, we would like to mine more valuable information
by processing them properly.

In this project, we focus on analyzing the drugs appear in titles of pubmed or clinical trials 
published by journals.

The objective is to build an efficient data pipeline to process and modeling the defined data 
in order to generate a joint vision which allows us to better understanding their relations.

### Description of the scripts
1. `main.py` (**answer for Task 3**) : Python file contains a data pipeline in Pyspark which transform 
   different input data (in .csv and .json) to an output table in .json format. 
- Install dependencies : `poetry install`
- Run pipeline : `poetry run python py_pipeline/etl/main.py`
2. Other commands
- Black : `poetry run black .`
- Pylint : `poetry run pylint py_pipeline/etl/`
- Ruff : `poetry run ruff format .`
- Run tests : `poetry run pytest py_pipeline/tests/`

### Output data view
Output json file are stored under ```output``` repository.

- drug_reference_pyspark.json/part-00000-XXX.json
```
{"id":"1","title":"A 44-year-old man with erythema of the face diphenhydramine, neck, and chest, weakness, and palpitations","journal":"Journal of emergency nursing","source":"pubmed","atccode":"A04AD","drug":"DIPHENHYDRAMINE"}
{"id":"2","title":"An evaluation of benadryl, pyribenzamine, and other so-called diphenhydramine antihistaminic drugs in the treatment of allergy.","journal":"Journal of emergency nursing","source":"pubmed","atccode":"A04AD","drug":"DIPHENHYDRAMINE"}
```

### Algorithms explication

#### Data pipeline using Pyspark
The following pipeline is used :
1. Import data from relevant files. Leverage Pyspark and ast to perform the data ingestion 
   from different csv or json files in the target repository.
2. Data cleaning to ensure the quality of data.
    - The column 'date' contains dates in different formats which need to be uniformed.
    - There are missing values (NaN).
    - Drug names and titles need to be parsed in lowercase for string matching.
3. Join reference tables (pubmed or clinical trials) with drugs table by substring matching 
   between titles and drug names.
4. Data transformation and post processing to generate the required results.
5. Save final results into a .json file as demanded.

#### Further improvements (answer for question 6)
As Apache Spark is designed for large-scale data processing, it can handle a large volume of data. 
However, it is still important to configure the cluster and jobs according to business needs.
