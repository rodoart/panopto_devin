"""Módulo common con las funciones binary_auc, binary_gini."""

import pyspark.sql.functions as F
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import array_to_vector
from pyspark.sql import DataFrame


def binary_auc(df: DataFrame, score_col: str, target_col: str) -> float:
    """Función que realiza la operación "binary_auc"."""
    df_auc = df.select(
        F.col(target_col).cast("double").alias("label"),
        array_to_vector(F.array(F.lit(0.0), F.col(score_col).cast("double"))).alias("rawPrediction"),
    )
    evaluator = BinaryClassificationEvaluator(
        rawPredictionCol="rawPrediction",
        labelCol="label",
    )
    return float(evaluator.evaluate(df_auc))


def binary_gini(df: DataFrame, score_col: str, target_col: str) -> float:
    """Función que realiza la operación "binary_gini"."""
    return 2.0 * binary_auc(df, score_col, target_col) - 1.0
