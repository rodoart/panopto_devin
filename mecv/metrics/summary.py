from typing import Any, Dict, List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


class VariableSummaryBuilder:
    @staticmethod
    def build(
        df: DataFrame,
        variable: str,
        var_type: str,
        data_type: str,
        model_id: str,
        information_date: str,
        execution_id: str,
    ) -> List[Dict[str, Any]]:
        rows = []
        base = {
            "execution_id": execution_id,
            "variable": variable,
            "var_type": var_type,
            "data_type": data_type,
            "model_id": model_id,
            "information_date": information_date,
        }

        total = df.count()
        non_null = df.filter(F.col(variable).isNotNull()).count()
        nulls = total - non_null

        rows.append({**base, "statistic": "count_total", "statistic_value": float(total), "statistic_value_str": str(total)})
        rows.append({**base, "statistic": "count_non_null", "statistic_value": float(non_null), "statistic_value_str": str(non_null)})
        rows.append({**base, "statistic": "count_null", "statistic_value": float(nulls), "statistic_value_str": str(nulls)})

        if data_type == "numeric" and non_null > 0:
            agg_row = df.agg(
                F.min(F.col(variable)).alias("min"),
                F.max(F.col(variable)).alias("max"),
                F.mean(F.col(variable)).alias("mean"),
                F.stddev_samp(F.col(variable)).alias("std"),
            ).collect()[0]
            for stat in ("min", "max", "mean", "std"):
                val = agg_row[stat]
                rows.append({**base, "statistic": stat, "statistic_value": float(val if val is not None else 0.0), "statistic_value_str": str(val)})

            for q in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
                val = df.approxQuantile(variable, [q], 0.01)[0]
                rows.append({
                    **base,
                    "statistic": f"p{int(q * 100)}",
                    "statistic_value": float(val),
                    "statistic_value_str": str(val),
                })
        elif data_type == "categorical":
            distinct = df.filter(F.col(variable).isNotNull()).select(F.col(variable)).distinct().count()
            rows.append({**base, "statistic": "distinct_count", "statistic_value": float(distinct), "statistic_value_str": str(distinct)})

            top = df.groupBy(F.col(variable).alias("category")).count().orderBy(F.desc("count")).limit(1).collect()
            if top:
                rows.append({**base, "statistic": "top_category", "statistic_value": None, "statistic_value_str": str(top[0]["category"])})
                rows.append({**base, "statistic": "top_category_count", "statistic_value": float(top[0]["count"]), "statistic_value_str": str(top[0]["count"])})

        return rows
