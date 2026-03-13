from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick


def plot_consumption(
    df,
    group_by,
    value_col,
    agg="mean",
    splits=None,
    kind="line",
    show_legend=True,
    figsize=(10,6),
    dpi=120
):

    group_cols = ["TIDPUNKT"]

    if splits:
        group_cols += splits

    agg_map = {
        "mean": F.mean,
        "sum": F.sum,
        "max": F.max,
        "variance": F.variance
    }

    g = (
        df
        .groupBy(group_cols)
        .agg(agg_map[agg](value_col).alias(value_col))
        .toPandas()
    )

    if splits:
        g = g.pivot_table(
            index="TIDPUNKT",
            columns=splits,
            values=value_col
        )

    else:
        g = g.set_index("TIDPUNKT")

    if "price" in (splits or []):
        g = g.drop(columns="all", errors="ignore")

    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)

    g.plot(kind=kind, marker="o", ax=ax)

    title_map = {
        "month": "Average Monthly Peak Consumption",
        "hour": "Average Hourly Consumption Profile",
        "weekday": "Average Consumption by Day of Week",
    }

    xlabel_map = {
        "month": "Month",
        "hour": "Hour of Day",
        "weekday": "Day of Week",
    }

    ylabel_map = {
        "mean_consumption": "Average Electricity Consumption (kWh)",
        "top3_mean_consumption": "Average Peak Consumption (Top 3 Hours, kWh)",
        "variance_consumption": "Variance of Electricity Consumption",
    }

    ax.set_title(title_map.get(group_by, "Electricity Consumption"))
    ax.set_xlabel(xlabel_map.get(group_by, "Month"))
    ax.set_ylabel(ylabel_map.get(value_col, "Electricity Consumption (kWh)"))

    if show_legend:

        handles, labels = ax.get_legend_handles_labels()

        new_labels = []

        for l in labels:

            parts = l.strip("()").split(", ")

            if parts == ["0"]:
                new_labels.append("No tariff")

            elif parts == ["1"]:
                new_labels.append("Tariff active")

            elif len(parts) == 2 and "usage_group" in (splits or []):

                usage_map = {
                    "low": "Low usage households",
                    "medium": "Medium usage households",
                    "high": "High usage households"
                }

                tariff_label = "Tariff active" if parts[1] == "1" else "No tariff"

                new_labels.append(f"{usage_map.get(parts[0], parts[0])} – {tariff_label}")

            elif len(parts) == 2 and "price" in (splits or []):

                price_label = "Low price period" if parts[0] == "low" else "High price period"
                tariff_label = "Tariff active" if parts[1] == "1" else "No tariff"

                new_labels.append(f"{price_label} – {tariff_label}")

            else:
                new_labels.append(l)

        ax.legend(handles, new_labels)

    plt.xticks(rotation=30)

    return ax



def plot_tariff_adoption_by_usage(
    df,
    figsize=(8,5),
    dpi=120,
    show_percent=True
):

    g = (
        df
        .groupBy("usage_group")
        .agg(F.mean("tariff_active").alias("share"))
        .toPandas()
    )

    g = g.set_index("usage_group")["share"]

    order = ["low", "medium", "high"]
    g = g.reindex(order)

    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)

    g.plot(kind="bar", ax=ax)

    ax.set_title("Tariff Adoption by Household Usage Group")
    ax.set_xlabel("Household Usage Group")
    ax.set_ylabel("Share Choosing Tariff")

    plt.xticks(rotation=0)

    if show_percent:
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))

    return ax



def plot_event_study_tariff(
    df,
    value_col="mean_consumption",
    line_cols="usage_group",
    window=6
):

    data = df.filter(F.col("tariff_start").isNotNull())

    data = data.withColumn(
        "event_time",
        (
            (F.year("TIDPUNKT") - F.year("tariff_start")) * 12 +
            (F.month("TIDPUNKT") - F.month("tariff_start"))
        )
    )

    data = data.filter(
        (F.col("event_time") >= -window) &
        (F.col("event_time") <= window)
    )

    if isinstance(line_cols, str):
        line_cols = [line_cols]

    g = (
        data
        .groupBy(["event_time"] + line_cols)
        .agg(F.mean(value_col).alias(value_col))
        .toPandas()
    )

    g = g.pivot_table(
        index="event_time",
        columns=line_cols,
        values=value_col
    )

    ax = g.plot(marker="o")

    plt.axvline(0, linestyle="--")

    title = ", ".join(line_cols).replace("_", " ").title()
    ax.legend(title=title)

    plt.title("Electricity Consumption Around Tariff Adoption")
    plt.xlabel("Months Relative to Tariff Adoption")
    plt.ylabel("Average Electricity Consumption (kWh)")

    plt.xticks(range(-window, window+1))

    return ax


