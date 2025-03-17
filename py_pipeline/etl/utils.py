"""Helper functions"""

from pyspark.sql.functions import to_date, coalesce


def to_date_(col, formats=("dd/MM/yyyy", "yyyy-MM-dd", "d MMMM yyyy")):
    """function to parse string to date"""
    return coalesce(*[to_date(col, f) for f in formats])
