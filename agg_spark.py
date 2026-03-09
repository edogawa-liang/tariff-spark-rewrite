from pyspark.sql import functions as F
from pyspark.sql import Window
import time


class ElectricityAggregator:

    def __init__(self, meter_df, tariff_df=None):

        print("Initializing ElectricityAggregator...")

        self.df = meter_df.withColumn(
            "TIDPUNKT",
            F.to_timestamp("TIDPUNKT")
        )

        self.tariff_df = tariff_df

        print("DataFrame loaded")

    ############################################
    # tariff merge
    ############################################

    def _merge_tariff(self):

        if self.tariff_df is None:
            return

        print("Merging tariff data...")

        tariff = (
            self.tariff_df
            .withColumnRenamed("GS1-nr.", "aID")
            .withColumnRenamed("Startdatum", "tariff_start")
            .withColumnRenamed("Produktnamn", "tariff_plan")
            .withColumn("tariff_start", F.to_timestamp("tariff_start"))
        )

        self.df = self.df.join(tariff, "aID", "left")

        self.df = self.df.withColumn(
            "has_tariff",
            F.when(F.col("tariff_start").isNotNull(), 1).otherwise(0)
        )

        self.df = self.df.withColumn(
            "tariff_active",
            F.when(
                (F.col("has_tariff") == 1)
                & (F.col("TIDPUNKT") >= F.col("tariff_start")),
                1
            ).otherwise(0)
        )

    ############################################
    # usage group
    ############################################

    def _create_usage_group(self):

        print("Creating usage groups...")

        usage = (
            self.df
            .groupBy("aID")
            .agg(F.sum("FORBRUKNING_KWH").alias("total_usage"))
        )

        q = usage.approxQuantile("total_usage", self.user_group_col, 0.01)

        if len(q) == 2:
            q1, q2 = q
        else:
            q1 = q[0]
            q2 = q[0]

        usage = usage.withColumn(
            "usage_group",
            F.when(F.col("total_usage") <= q1, "low")
            .when(F.col("total_usage") <= q2, "medium")
            .otherwise("high")
        )

        self.df = self.df.join(
            usage.select("aID", "usage_group"),
            "aID",
            "left"
        )

    ############################################
    # price period
    ############################################

    def _add_price_period(self, df):

        return df.withColumn(
            "price",
            F.when(
                (F.dayofweek("TIDPUNKT").between(2, 6))
                & (F.hour("TIDPUNKT").between(7, 19)),
                "high"
            ).otherwise("low")
        )

    def _apply_price_mode(self, df):

        if not self.use_price:
            return df.withColumn("price", F.lit("all"))

        df_hl = self._add_price_period(df)
        df_all = df.withColumn("price", F.lit("all"))

        return df_all.unionByName(df_hl)

    ############################################
    # compute aggregation
    ############################################

    def _compute_one_agg(self, df, group_cols, method):

        if method == "sum":

            return df.groupBy(group_cols).agg(
                F.sum("FORBRUKNING_KWH").alias("sum_consumption")
            )

        if method == "mean":

            return df.groupBy(group_cols).agg(
                F.avg("FORBRUKNING_KWH").alias("mean_consumption")
            )

        if method == "variance":

            return df.groupBy(group_cols).agg(
                F.variance("FORBRUKNING_KWH").alias("variance_consumption")
            )

        if method == "median":

            return df.groupBy(group_cols).agg(
                F.expr("percentile_approx(FORBRUKNING_KWH,0.5)")
                .alias("median_consumption")
            )

        if method == "max":

            return df.groupBy(group_cols).agg(
                F.max("FORBRUKNING_KWH").alias("max_consumption")
            )

        if method.startswith("q"):

            q = float(method[1:]) / 100

            return df.groupBy(group_cols).agg(
                F.expr(f"percentile_approx(FORBRUKNING_KWH,{q})")
                .alias(f"{method}_consumption")
            )

        if method == "top3_mean":

            w = Window.partitionBy(group_cols).orderBy(
                F.col("FORBRUKNING_KWH").desc()
            )

            df_rank = df.withColumn(
                "rank",
                F.row_number().over(w)
            )

            return (
                df_rank
                .filter(F.col("rank") <= 3)
                .groupBy(group_cols)
                .agg(
                    F.avg("FORBRUKNING_KWH")
                    .alias("top3_mean_consumption")
                )
            )

        raise ValueError(method)

    ############################################
    # aggregation
    ############################################

    def _aggregate(self, df):

        print("Aggregating per household...")

        if self.freq == "hour":

            df = df.withColumn("period", F.hour("TIDPUNKT"))

        elif self.freq == "week_part":

            df = df.withColumn(
                "period",
                F.when(
                    F.dayofweek("TIDPUNKT").between(2, 6),
                    "weekday"
                ).otherwise("weekend")
            )

        elif self.freq == "weekday":

            df = df.withColumn(
                "period",
                F.dayofweek("TIDPUNKT")
            )

        else:

            df = df.withColumn(
                "period",
                F.date_trunc(self.freq, "TIDPUNKT")
            )

        df = self._apply_price_mode(df)

        group_cols = [
            "aID",
            "period",
            "price",
            "tariff_active"
        ]

        result = None

        for m in self.agg_methods:

            part = self._compute_one_agg(df, group_cols, m)

            if result is None:
                result = part
            else:
                result = result.join(part, group_cols, "left")

        result = result.withColumnRenamed("period", "TIDPUNKT")

        result = self._attach_tariff_info(result)
        result = self._attach_usage_group(result)

        return result

    ############################################
    # attach info
    ############################################

    def _attach_tariff_info(self, result):

        tariff_info = (
            self.df
            .select("aID", "tariff_start", "tariff_plan")
            .dropDuplicates(["aID"])
        )

        return result.join(tariff_info, "aID", "left")

    def _attach_usage_group(self, result):

        if "usage_group" not in self.df.columns:
            return result

        usage = (
            self.df
            .select("aID", "usage_group")
            .dropDuplicates(["aID"])
        )

        return result.join(usage, "aID", "left")

    ############################################
    # pipeline
    ############################################

    def run(
        self,
        freq="month",
        agg_method="top3_mean",
        use_price=True,
        add_user_group_col=None,
        table_name=None,
        mode="overwrite"
    ):

        start = time.time()

        print("Electricity aggregation start")

        self.freq = freq
        self.use_price = use_price
        self.user_group_col = add_user_group_col

        if isinstance(agg_method, str):
            self.agg_methods = [agg_method]
        else:
            self.agg_methods = agg_method

        if self.tariff_df is not None:
            self._merge_tariff()

        if add_user_group_col is not None:
            self._create_usage_group()

        result = self._aggregate(self.df)

        ############################################
        # save
        ############################################

        if table_name is not None:

            print(f"Saving dataset as table: {table_name}")

            (
                result
                .write
                .format("delta")
                .mode(mode)
                .saveAsTable(table_name)
            )

        print("Total runtime:", time.time() - start)

        return result