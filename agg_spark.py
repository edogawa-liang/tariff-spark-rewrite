from pyspark.sql import functions as F
from pyspark.sql import Window
import time


class ElectricityAggregator:

    ############################################
    # init
    ############################################

    def __init__(self, meter_df, tariff_df=None):

        print("Initializing ElectricityAggregator...")

        self.df = (
            meter_df
            .withColumn("TIDPUNKT", F.to_timestamp("TIDPUNKT"))
            .withColumn("aID", F.col("aID").cast("string"))
        )

        self.tariff_df = tariff_df
        self.holiday_dates = [
            "2023-01-01","2023-01-06","2023-04-07","2023-04-10",
            "2023-12-24","2023-12-25","2023-12-26","2023-12-31",

            "2024-01-01","2024-01-06","2024-03-29","2024-04-01",
            "2024-12-24","2024-12-25","2024-12-26","2024-12-31",

            "2025-01-01","2025-01-06","2025-04-18","2025-04-21",
            "2025-12-24","2025-12-25","2025-12-26","2025-12-31",

            "2026-01-01","2026-01-06","2026-04-03","2026-04-06"
            ]

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
            .withColumn("aID", F.col("aID").cast("string"))
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
            .agg(
                F.sum("FORBRUKNING_KWH")
                .alias("total_consumption")
            )
        )

        q = usage.approxQuantile(
            "total_consumption",
            self.user_group_col,
            0.01
        )

        q1, q2 = q[0], q[1]

        usage = usage.withColumn(
            "usage_group",
            F.when(F.col("total_consumption") <= q1, "low")
            .when(F.col("total_consumption") <= q2, "medium")
            .otherwise("high")
        )

        self.df = self.df.join(
            usage.select(
                "aID",
                "total_consumption",
                "usage_group"
            ),
            "aID",
            "left"
        )

    ############################################
    # price period
    ############################################

    def _add_price_period(self, df):

        df = df.withColumn(
            "date",
            F.to_date("TIDPUNKT")
        )

        # holiday flag
        df = df.withColumn(
            "is_holiday",
            F.col("date").isin(self.holiday_dates)
        )

        # winter months
        winter = F.month("TIDPUNKT").isin([11, 12, 1, 2, 3])

        # weekday (Mon-Fri)
        weekday = F.dayofweek("TIDPUNKT").between(2, 6)

        # peak hours
        peak_hour = F.hour("TIDPUNKT").between(7, 19)

        return df.withColumn(
            "price",
            F.when(
                winter &
                weekday &
                peak_hour &
                (~F.col("is_holiday")),
                "high"
            ).otherwise("low")
        )

    ############################################
    # price mode
    ############################################

    def _apply_price_mode(self, df):

        if not self.use_price:

            return df.withColumn("price", F.lit("all"))

        df_hl = self._add_price_period(df)

        df_all = df.withColumn("price", F.lit("all"))

        return df_all.unionByName(df_hl)

    ############################################
    # aggregation helper
    ############################################

    def _compute_one_agg(self, df, group_cols, method):

        if method == "sum":

            return df.groupBy(group_cols).agg(
                F.sum("FORBRUKNING_KWH")
                .alias("sum_consumption")
            )

        if method == "mean":

            return df.groupBy(group_cols).agg(
                F.avg("FORBRUKNING_KWH")
                .alias("mean_consumption")
            )

        if method == "variance":

            return df.groupBy(group_cols).agg(
                F.variance("FORBRUKNING_KWH")
                .alias("variance_consumption")
            )

        if method == "median":

            return df.groupBy(group_cols).agg(
                F.expr(
                    "percentile_approx(FORBRUKNING_KWH,0.5)"
                ).alias("median_consumption")
            )

        if method == "max":

            return df.groupBy(group_cols).agg(
                F.max("FORBRUKNING_KWH")
                .alias("max_consumption")
            )

        if method.startswith("q"):

            q = float(method[1:]) / 100

            return df.groupBy(group_cols).agg(
                F.expr(
                    f"percentile_approx(FORBRUKNING_KWH,{q})"
                ).alias(f"{method}_consumption")
            )

        ############################################
        # top3 mean peak
        ############################################

        if method == "top3_mean":

            df = df.withColumn(
                "date",
                F.to_date("TIDPUNKT")
            )

            ############################################
            # step1: daily peak
            ############################################

            w_daily = Window.partitionBy(
                group_cols + ["date"]
            ).orderBy(
                F.col("FORBRUKNING_KWH").desc()
            )

            daily_peak = (
                df
                .withColumn(
                    "rank_daily",
                    F.row_number().over(w_daily)
                )
                .filter(F.col("rank_daily") == 1)
            )

            ############################################
            # step2: rank top3 days
            ############################################

            w = Window.partitionBy(
                group_cols
            ).orderBy(
                F.col("FORBRUKNING_KWH").desc()
            )

            ranked = daily_peak.withColumn(
                "rank",
                F.row_number().over(w)
            )

            top3 = ranked.filter(
                F.col("rank") <= 3
            )

            ############################################
            # step3: pivot to peak1/2/3
            ############################################

            pivot = (
                top3
                .groupBy(group_cols)
                .pivot("rank", [1,2,3])
                .agg(
                    F.first("TIDPUNKT").alias("time"),
                    F.first("FORBRUKNING_KWH").alias("consumption")
                )
            )

            ############################################
            # rename columns
            ############################################

            pivot = (
                pivot
                .withColumnRenamed("1_time","peak1_time")
                .withColumnRenamed("1_consumption","peak1_consumption")
                .withColumnRenamed("2_time","peak2_time")
                .withColumnRenamed("2_consumption","peak2_consumption")
                .withColumnRenamed("3_time","peak3_time")
                .withColumnRenamed("3_consumption","peak3_consumption")
            )

            ############################################
            # compute mean
            ############################################

            pivot = pivot.withColumn(
                "top3_mean_consumption",
                (
                    F.col("peak1_consumption") +
                    F.col("peak2_consumption") +
                    F.col("peak3_consumption")
                ) / 3
            )

            return pivot

        raise ValueError(method)

    ############################################
    # aggregation
    ############################################

    def _aggregate(self, df):

        print("Aggregating per household...")

        if self.freq == "hour":

            df = df.withColumn(
                "period",
                F.hour("TIDPUNKT")
            )

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

                result = result.join(
                    part,
                    group_cols,
                    "left"
                )

        result = result.withColumnRenamed(
            "period",
            "TIDPUNKT"
        )

        result = self._attach_tariff_info(result)

        result = self._attach_usage_group(result)

        return result

    ############################################
    # attach info
    ############################################

    def _attach_tariff_info(self, result):

        tariff_info = (
            self.df
            .select(
                "aID",
                "tariff_start",
                "tariff_plan"
            )
            .dropDuplicates(["aID"])
        )

        tariff_info = tariff_info.withColumn(
            "tariff_plan",
            F.regexp_extract(
                "tariff_plan",
                r'(\\d+\\s*kW\\s*(?:Villa|Normal))',
                1
            )
        )

        return result.join(
            tariff_info,
            "aID",
            "left"
        )

    def _attach_usage_group(self, result):

        if "usage_group" not in self.df.columns:
            return result

        usage_info = (
            self.df
            .select(
                "aID",
                "total_consumption",
                "usage_group"
            )
            .dropDuplicates(["aID"])
        )

        return result.join(
            usage_info,
            "aID",
            "left"
        )

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