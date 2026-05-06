import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# =====================================================
# Helpers
# =====================================================

def _extract_peak_long(df):
    """
    Convert peak1/2/3 columns into long format.
    This keeps peak time and consumption correctly paired.
    """

    pieces = []

    for i in [1, 2, 3]:
        temp = pd.DataFrame({
            "time": pd.to_datetime(df[f"peak{i}_time"], errors="coerce"),
            "consumption": pd.to_numeric(df[f"peak{i}_consumption"], errors="coerce"),
            "peak_rank": i
        })
        pieces.append(temp)

    out = pd.concat(pieces, ignore_index=True).dropna(subset=["time", "consumption"])

    out["month"] = out["time"].dt.month
    out["hour"] = out["time"].dt.hour

    return out


def _extract_peak_times(df):
    return _extract_peak_long(df)["time"]


def _extract_peak_consumption(df):
    return _extract_peak_long(df)["consumption"]


def _safe_max(*dfs):
    vals = []
    for x in dfs:
        if x is not None and len(x) > 0:
            m = x.max().max()
            if pd.notna(m):
                vals.append(m)
    return max(vals) if vals else 0


def _safe_abs_max(df):
    if df is None or len(df) == 0:
        return 0
    m = df.abs().max().max()
    return m if pd.notna(m) else 0


# =====================================================
# Peak hour distribution
# =====================================================

def plot_peak_hour_distribution(df, mode="count"):
    """
    mode:
        count -> peak frequency
        consumption -> total peak consumption
    """

    temp = _extract_peak_long(df)

    if mode == "count":
        data = temp["hour"].value_counts().sort_index()
        ylabel = "Peak Count"

    elif mode == "consumption":
        data = temp.groupby("hour")["consumption"].sum()
        ylabel = "Total Peak Consumption (kWh)"

    else:
        raise ValueError("mode must be 'count' or 'consumption'")

    plt.figure(figsize=(8, 4))
    data.sort_index().plot.bar()

    plt.xlabel("Hour of Day")
    plt.ylabel(ylabel)
    plt.title(f"Peak Hour Distribution ({mode})")

    plt.tight_layout()
    plt.show()


# =====================================================
# Peak heatmap
# =====================================================

def plot_peak_heatmap(df, mode="count"):
    """
    Heatmap of peak demand by month and hour.
    """

    temp = _extract_peak_long(df)

    if mode == "count":
        heatmap = temp.pivot_table(
            index="month",
            columns="hour",
            aggfunc="size",
            fill_value=0
        )
        cbar_label = "Peak count"

    elif mode == "consumption":
        heatmap = temp.pivot_table(
            index="month",
            columns="hour",
            values="consumption",
            aggfunc="sum",
            fill_value=0
        )
        cbar_label = "Total peak consumption (kWh)"

    else:
        raise ValueError("mode must be 'count' or 'consumption'")

    heatmap = heatmap.reindex(index=range(1, 13), columns=range(24), fill_value=0)

    plt.figure(figsize=(10, 5))

    sns.heatmap(
        heatmap,
        cmap="YlOrRd",
        cbar_kws={"label": cbar_label}
    )

    plt.title(f"Peak Heatmap ({mode})")
    plt.xlabel("Hour")
    plt.ylabel("Month")

    plt.tight_layout()
    plt.show()


# =====================================================
# Peak consumption distribution
# =====================================================

def plot_peak_consumption_distribution(df):

    peaks = _extract_peak_long(df)["consumption"]

    plt.figure(figsize=(8, 4))
    plt.hist(peaks, bins=30)

    plt.xlabel("Peak Consumption (kWh)")
    plt.ylabel("Frequency")
    plt.title("Peak Consumption Distribution")

    plt.tight_layout()
    plt.show()


# =====================================================
# Peak rank boxplot
# =====================================================

def plot_peak_rank_boxplot(df):

    temp = _extract_peak_long(df)

    temp["peak_rank"] = temp["peak_rank"].map({
        1: "Peak 1",
        2: "Peak 2",
        3: "Peak 3"
    })

    plt.figure(figsize=(6, 4))
    sns.boxplot(data=temp, x="peak_rank", y="consumption")

    plt.xlabel("")
    plt.ylabel("Consumption (kWh)")
    plt.title("Peak Rank Comparison")

    plt.tight_layout()
    plt.show()


# =====================================================
# Tariff peak count heatmap
# =====================================================

def plot_tariff_peak_heatmap(df, price_label="all"):

    df = df.copy()

    never = df[df["tariff_start"].isna()]
    before = df[
        (df["tariff_start"].notna()) &
        (df["tariff_active"] == 0)
    ]
    after = df[df["tariff_active"] == 1]

    groups = {
        "Never adopters": never,
        "Adopters BEFORE": before,
        "Adopters AFTER": after
    }

    heatmaps = {}

    for name, subset in groups.items():

        temp = _extract_peak_long(subset)

        heatmap = temp.pivot_table(
            index="month",
            columns="hour",
            aggfunc="size",
            fill_value=0
        )

        heatmaps[name] = heatmap

    all_months = sorted(set().union(*[set(h.index) for h in heatmaps.values()]))
    all_hours = list(range(24))

    for key in heatmaps:
        heatmaps[key] = heatmaps[key].reindex(
            index=all_months,
            columns=all_hours,
            fill_value=0
        )

    never = heatmaps["Never adopters"]
    before = heatmaps["Adopters BEFORE"]
    after = heatmaps["Adopters AFTER"]

    diff = after - before

    vmax = _safe_max(never, before, after)
    diff_max = _safe_abs_max(diff)

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    sns.heatmap(
        never,
        cmap="YlOrRd",
        vmin=0,
        vmax=vmax,
        ax=axes[0, 0],
        cbar_kws={"label": "Peak count"}
    )
    axes[0, 0].set_title("Never adopters")

    sns.heatmap(
        before,
        cmap="YlOrRd",
        vmin=0,
        vmax=vmax,
        ax=axes[0, 1],
        cbar_kws={"label": "Peak count"}
    )
    axes[0, 1].set_title("Adopters BEFORE")

    sns.heatmap(
        after,
        cmap="YlOrRd",
        vmin=0,
        vmax=vmax,
        ax=axes[1, 0],
        cbar_kws={"label": "Peak count"}
    )
    axes[1, 0].set_title("Adopters AFTER")

    sns.heatmap(
        diff,
        cmap="coolwarm",
        center=0,
        vmin=-diff_max,
        vmax=diff_max,
        ax=axes[1, 1],
        cbar_kws={"label": "After − Before peak count"}
    )
    axes[1, 1].set_title("Difference (After − Before)")

    for ax in axes.flat:
        ax.set_xlabel("Hour")
        ax.set_ylabel("Month")

    title_map = {
        "all": "Peak Count Heatmap (Overall Peaks)",
        "high": "Peak Count Heatmap (High Price Period Peaks)",
        "low": "Peak Count Heatmap (Low Price Period Peaks)"
    }

    fig.suptitle(
        title_map.get(price_label, "Peak Count Heatmap"),
        fontsize=16,
        y=1.02
    )

    plt.tight_layout()
    plt.show()


# =====================================================
# Tariff peak consumption heatmap
# =====================================================

def plot_tariff_consumption_heatmap(df, price_label="all", modes=("sum", "mean")):
    """
    Automatically plots both:
        sum  -> total peak consumption / burden
        mean -> average peak size / conditional intensity
    """

    for mode in modes:

        if mode not in ["sum", "mean"]:
            raise ValueError("modes must contain only 'sum' or 'mean'")

        df_work = df.copy()

        never = df_work[df_work["tariff_start"].isna()]
        before = df_work[
            (df_work["tariff_start"].notna()) &
            (df_work["tariff_active"] == 0)
        ]
        after = df_work[df_work["tariff_active"] == 1]

        groups = {
            "Never adopters": never,
            "Adopters BEFORE": before,
            "Adopters AFTER": after
        }

        heatmaps = {}

        for name, subset in groups.items():

            temp = _extract_peak_long(subset)

            heatmap = temp.pivot_table(
                index="month",
                columns="hour",
                values="consumption",
                aggfunc=mode,
                fill_value=0 if mode == "sum" else None
            )

            heatmaps[name] = heatmap

        all_months = sorted(set().union(*[set(h.index) for h in heatmaps.values()]))
        all_hours = list(range(24))

        for key in heatmaps:
            heatmaps[key] = heatmaps[key].reindex(
                index=all_months,
                columns=all_hours,
                fill_value=0 if mode == "sum" else None
            )

        never = heatmaps["Never adopters"]
        before = heatmaps["Adopters BEFORE"]
        after = heatmaps["Adopters AFTER"]

        diff = after - before

        vmax = _safe_max(never, before, after)
        diff_max = _safe_abs_max(diff)

        fig, axes = plt.subplots(2, 2, figsize=(14, 10))

        if mode == "sum":
            cbar_label = "Total Peak Consumption (kWh)"
            diff_label = "After − Before total peak consumption"
        else:
            cbar_label = "Average Peak Consumption (kWh)"
            diff_label = "After − Before average peak consumption"

        sns.heatmap(
            never,
            cmap="YlOrRd",
            vmin=0,
            vmax=vmax,
            ax=axes[0, 0],
            cbar_kws={"label": cbar_label}
        )
        axes[0, 0].set_title("Never adopters")

        sns.heatmap(
            before,
            cmap="YlOrRd",
            vmin=0,
            vmax=vmax,
            ax=axes[0, 1],
            cbar_kws={"label": cbar_label}
        )
        axes[0, 1].set_title("Adopters BEFORE")

        sns.heatmap(
            after,
            cmap="YlOrRd",
            vmin=0,
            vmax=vmax,
            ax=axes[1, 0],
            cbar_kws={"label": cbar_label}
        )
        axes[1, 0].set_title("Adopters AFTER")

        sns.heatmap(
            diff,
            cmap="coolwarm",
            center=0,
            vmin=-diff_max,
            vmax=diff_max,
            ax=axes[1, 1],
            cbar_kws={"label": diff_label}
        )
        axes[1, 1].set_title("Difference (After − Before)")

        for ax in axes.flat:
            ax.set_xlabel("Hour")
            ax.set_ylabel("Month")

        title_map = {
            "all": f"Peak Consumption Heatmap - {mode.upper()} (Overall Peaks)",
            "high": f"Peak Consumption Heatmap - {mode.upper()} (High Price Period Peaks)",
            "low": f"Peak Consumption Heatmap - {mode.upper()} (Low Price Period Peaks)"
        }

        # fig.suptitle(
        #     title_map.get(price_label, f"Peak Consumption Heatmap - {mode.upper()}"),
        #     fontsize=16,
        #     y=1.02
        # )
        fig.suptitle(title_map.get(price_label, "Peak Consumption Heatmap"), fontsize=16, y=1.02)
        plt.tight_layout()
        plt.show()