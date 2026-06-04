import pandas as pd
import numpy as np
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


def _nice_upper_limit(value):
    """
    Round the colorbar maximum up to a readable value.

    This avoids colorbars such as 0, 1.97, 3.94, ... and prevents too many
    tick labels from being printed when the maximum is large.
    """
    if value is None or pd.isna(value):
        return 1

    value = float(value)
    if value <= 0:
        return 1

    magnitude = 10 ** np.floor(np.log10(value))
    normalized = value / magnitude

    if normalized <= 1:
        nice = 1
    elif normalized <= 2:
        nice = 2
    elif normalized <= 5:
        nice = 5
    else:
        nice = 10

    return nice * magnitude


def _make_nonnegative_ticks(vmax, n_ticks=5):
    """
    Create a small, fixed number of readable ticks for non-negative heatmaps.

    Example: 0, 2, 4, 6, 8 or 0, 2.5, 5, 7.5, 10.
    The key point is that all comparable panels receive the same tick values,
    without overcrowding the colorbar.
    """
    vmax = _nice_upper_limit(vmax)
    return np.linspace(0, vmax, n_ticks)


def _make_centered_ticks(vmax, n_ticks=5):
    """
    Create a small, fixed number of symmetric ticks for difference heatmaps.
    """
    vmax = _nice_upper_limit(vmax)
    return np.linspace(-vmax, vmax, n_ticks)



def _resolve_scale(scale, mode):
    """
    Accept either a scalar scale or a dictionary such as {"mean": 9.5, "sum": 1000}.
    """
    if isinstance(scale, dict):
        return scale.get(mode)
    return scale


def _split_tariff_groups(df):
    """
    Split data into never adopters, adopters before tariff, and adopters after tariff.
    """
    df_work = df.copy()

    return {
        "Never adopters": df_work[df_work["tariff_start"].isna()],
        "Adopters BEFORE": df_work[
            (df_work["tariff_start"].notna()) &
            (df_work["tariff_active"] == 0)
        ],
        "Adopters AFTER": df_work[df_work["tariff_active"] == 1]
    }


def _make_tariff_consumption_heatmaps(df, mode="mean", months=None, hours=None):
    """
    Build tariff-group heatmaps for peak consumption.

    mode:
        mean -> average peak consumption in each month-hour cell
        sum  -> total peak consumption in each month-hour cell

    Note: for descriptive before/after comparisons with staggered adoption,
    mean is usually preferred because sum is mechanically affected by the
    number of observations in each month-hour cell.
    """
    if mode not in ["sum", "mean"]:
        raise ValueError("mode must be 'sum' or 'mean'")

    groups = _split_tariff_groups(df)
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

    if months is None:
        months = sorted(set().union(*[set(h.index) for h in heatmaps.values()]))
    if hours is None:
        hours = list(range(24))

    for key in heatmaps:
        heatmaps[key] = heatmaps[key].reindex(
            index=months,
            columns=hours,
            fill_value=0 if mode == "sum" else None
        )

    heatmaps["Difference (After − Before)"] = (
        heatmaps["Adopters AFTER"] - heatmaps["Adopters BEFORE"]
    )

    return heatmaps


def get_tariff_consumption_color_scales(*dfs, modes=("mean",), include_difference=False):
    """
    Compute common color scales across multiple datasets / pricing periods.

    Use this when plotting Figure 10, Figure 11, and Figure 12 separately but
    you want their consumption panels to share the same color scale.

    Returns
    -------
    dict
        {
            "consumption_vmax": {mode: value},
            "diff_vmax": {mode: value}   # only meaningful if include_difference=True
        }

    Example
    -------
    scales = get_tariff_consumption_color_scales(
        df_high, df_low, df_all,
        modes=("mean",),
        include_difference=False
    )

    plot_tariff_consumption_heatmap(
        df_high,
        price_label="high",
        modes=("mean",),
        consumption_vmax=scales["consumption_vmax"]
    )
    """
    if not dfs:
        raise ValueError("At least one dataframe must be provided")

    consumption_vmax = {}
    diff_vmax = {}

    for mode in modes:
        if mode not in ["sum", "mean"]:
            raise ValueError("modes must contain only 'sum' or 'mean'")

        consumption_max_candidates = []
        diff_max_candidates = []

        for df in dfs:
            heatmaps = _make_tariff_consumption_heatmaps(df, mode=mode)

            # Common scale for the three consumption panels:
            # Never adopters, Adopters BEFORE, and Adopters AFTER.
            consumption_max_candidates.append(
                _safe_max(
                    heatmaps["Never adopters"],
                    heatmaps["Adopters BEFORE"],
                    heatmaps["Adopters AFTER"]
                )
            )

            if include_difference:
                diff_max_candidates.append(
                    _safe_abs_max(heatmaps["Difference (After − Before)"])
                )

        consumption_vmax[mode] = max(consumption_max_candidates) if consumption_max_candidates else 0
        diff_vmax[mode] = max(diff_max_candidates) if diff_max_candidates else None

    return {
        "consumption_vmax": consumption_vmax,
        "diff_vmax": diff_vmax
    }


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

# =====================================================
# Tariff peak count heatmap
# =====================================================

def plot_tariff_peak_heatmap(df, price_label="all", count_tick_count=5):

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

    # =====================================================
    # Color scales and fixed colorbar ticks
    # =====================================================

    # One common scale for all three count panels.
    count_vmax = _safe_max(never, before, after)
    count_vmax = _nice_upper_limit(count_vmax)
    count_ticks = _make_nonnegative_ticks(count_vmax, n_ticks=count_tick_count)

    diff_max = _safe_abs_max(diff)
    diff_ticks = _make_centered_ticks(diff_max, n_ticks=5)

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # -----------------------------------------------------
    # Never adopters
    # -----------------------------------------------------

    sns.heatmap(
        never,
        cmap="YlOrRd",
        vmin=0,
        vmax=count_vmax,
        ax=axes[0, 0],
        cbar_kws={"label": "Peak count", "ticks": count_ticks}
    )

    axes[0, 0].set_title("Never adopters")

    # -----------------------------------------------------
    # BEFORE
    # -----------------------------------------------------

    sns.heatmap(
        before,
        cmap="YlOrRd",
        vmin=0,
        vmax=count_vmax,
        ax=axes[0, 1],
        cbar_kws={"label": "Peak count", "ticks": count_ticks}
    )

    axes[0, 1].set_title("Adopters BEFORE")

    # -----------------------------------------------------
    # AFTER
    # -----------------------------------------------------

    sns.heatmap(
        after,
        cmap="YlOrRd",
        vmin=0,
        vmax=count_vmax,
        ax=axes[1, 0],
        cbar_kws={"label": "Peak count", "ticks": count_ticks}
    )

    axes[1, 0].set_title("Adopters AFTER")

    # -----------------------------------------------------
    # Difference
    # -----------------------------------------------------

    sns.heatmap(
        diff,
        cmap="coolwarm",
        center=0,
        vmin=-diff_max,
        vmax=diff_max,
        ax=axes[1, 1],
        cbar_kws={"label": "After − Before peak count", "ticks": diff_ticks}
    )

    axes[1, 1].set_title("Difference (After − Before)")

    # -----------------------------------------------------
    # Labels
    # -----------------------------------------------------

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

# =====================================================
# Tariff peak consumption heatmap
# =====================================================

def plot_tariff_consumption_heatmap(
    df,
    price_label="all",
    modes=("mean",),
    consumption_vmax=None,
    diff_vmax=None,
    difference_scale="local",
    consumption_tick_count=5,
    diff_tick_count=5
):


    if difference_scale not in ["local", "common"]:
        raise ValueError("difference_scale must be 'local' or 'common'")

    for mode in modes:

        if mode not in ["sum", "mean"]:
            raise ValueError("modes must contain only 'sum' or 'mean'")

        heatmaps = _make_tariff_consumption_heatmaps(df, mode=mode)

        never = heatmaps["Never adopters"]
        before = heatmaps["Adopters BEFORE"]
        after = heatmaps["Adopters AFTER"]
        diff = heatmaps["Difference (After − Before)"]

        # =====================================================
        # Color scales
        # =====================================================

        # One common scale for ALL THREE consumption panels.
        this_consumption_vmax = _resolve_scale(consumption_vmax, mode)
        if this_consumption_vmax is None:
            this_consumption_vmax = _safe_max(never, before, after)

        # Difference panel: separate diverging scale centered at zero.
        this_diff_vmax = _resolve_scale(diff_vmax, mode)
        if difference_scale == "local" or this_diff_vmax is None:
            this_diff_vmax = _safe_abs_max(diff)

        # Avoid matplotlib warnings when all values are zero or missing.
        if this_consumption_vmax == 0:
            this_consumption_vmax = 1
        if this_diff_vmax == 0:
            this_diff_vmax = 1

        # Fixed colorbar ticks.
        # Instead of forcing a tiny interval such as every 2 units, we show a
        # small fixed number of ticks. This keeps the legend readable even when
        # the scale is large.
        this_consumption_vmax = _nice_upper_limit(this_consumption_vmax)
        consumption_ticks = _make_nonnegative_ticks(
            this_consumption_vmax,
            n_ticks=consumption_tick_count
        )

        this_diff_vmax = _nice_upper_limit(this_diff_vmax)
        diff_ticks = _make_centered_ticks(
            this_diff_vmax,
            n_ticks=diff_tick_count
        )

        fig, axes = plt.subplots(2, 2, figsize=(14, 10))

        # =====================================================
        # Labels
        # =====================================================

        if mode == "sum":
            cbar_label = "Total Peak Consumption (kWh)"
            diff_label = "After − Before total peak consumption"
        else:
            cbar_label = "Average Peak Consumption (kWh)"
            diff_label = "After − Before average peak consumption"

        # =====================================================
        # Consumption panels: shared scale
        # =====================================================

        sns.heatmap(
            never,
            cmap="YlOrRd",
            vmin=0,
            vmax=this_consumption_vmax,
            ax=axes[0, 0],
            cbar_kws={"label": cbar_label, "ticks": consumption_ticks}
        )
        axes[0, 0].set_title("Never adopters")

        sns.heatmap(
            before,
            cmap="YlOrRd",
            vmin=0,
            vmax=this_consumption_vmax,
            ax=axes[0, 1],
            cbar_kws={"label": cbar_label, "ticks": consumption_ticks}
        )
        axes[0, 1].set_title("Adopters BEFORE")

        sns.heatmap(
            after,
            cmap="YlOrRd",
            vmin=0,
            vmax=this_consumption_vmax,
            ax=axes[1, 0],
            cbar_kws={"label": cbar_label, "ticks": consumption_ticks}
        )
        axes[1, 0].set_title("Adopters AFTER")

        # =====================================================
        # Difference panel: separate diverging scale
        # =====================================================

        sns.heatmap(
            diff,
            cmap="coolwarm",
            center=0,
            vmin=-this_diff_vmax,
            vmax=this_diff_vmax,
            ax=axes[1, 1],
            cbar_kws={"label": diff_label, "ticks": diff_ticks}
        )
        axes[1, 1].set_title("Difference (After − Before)")

        # =====================================================
        # Axis labels
        # =====================================================

        for ax in axes.flat:
            ax.set_xlabel("Hour")
            ax.set_ylabel("Month")

        title_map = {
            "all": f"Peak Consumption Heatmap - {mode.upper()} (Overall Peaks)",
            "high": f"Peak Consumption Heatmap - {mode.upper()} (High Price Period Peaks)",
            "low": f"Peak Consumption Heatmap - {mode.upper()} (Low Price Period Peaks)"
        }

        fig.suptitle(
            title_map.get(price_label, "Peak Consumption Heatmap"),
            fontsize=16,
            y=1.02
        )

        plt.tight_layout()
        plt.show()


# =====================================================
# Heatmaps for full / matched / unmatched groups
# =====================================================

def _make_average_peak_consumption_heatmap(df, months=None, hours=None):
    """
    Month × Hour heatmap:
    E(peak consumption | peak occurs in month m and hour h)
    """

    temp = _extract_peak_long(df)

    heatmap = temp.pivot_table(
        index="month",
        columns="hour",
        values="consumption",
        aggfunc="mean"
    )

    if months is None:
        months = list(range(1, 13))
    if hours is None:
        hours = list(range(24))

    return heatmap.reindex(index=months, columns=hours)


def plot_all_households_before_after_heatmap(
    df,
    price_label="high",
    consumption_vmax=None,
    tick_count=5
):
    """
    1×2 heatmap:
        1. All households BEFORE tariff
           = never adopters + adopters before tariff

        2. All households AFTER tariff
           = never adopters + adopters after tariff

    Input df can already be filtered by price, e.g.
        df[df["price"] == "high"]
    """

    before_df = df[
        df["tariff_start"].isna() |
        (
            df["tariff_start"].notna() &
            (df["tariff_active"] == 0)
        )
    ]

    after_df = df[
        df["tariff_start"].isna() |
        (
            df["tariff_start"].notna() &
            (df["tariff_active"] == 1)
        )
    ]

    before_heatmap = _make_average_peak_consumption_heatmap(before_df)
    after_heatmap = _make_average_peak_consumption_heatmap(after_df)

    if consumption_vmax is None:
        consumption_vmax = _safe_max(before_heatmap, after_heatmap)

    consumption_vmax = _nice_upper_limit(consumption_vmax)
    ticks = _make_nonnegative_ticks(consumption_vmax, n_ticks=tick_count)

    fig, axes = plt.subplots(1, 2, figsize=(18, 5), sharey=True)

    title_map = {
        "all": "Overall Peaks",
        "high": "High-Price Period Peaks",
        "low": "Low-Price Period Peaks"
    }

    peak_label = title_map.get(price_label, "Average Peak Consumption")

    sns.heatmap(
        before_heatmap,
        ax=axes[0],
        cmap="YlOrRd",
        vmin=0,
        vmax=consumption_vmax,
        cbar_kws={
            "label": "Average Peak Consumption (kWh)",
            "ticks": ticks
        }
    )

    axes[0].set_title(f"All Households: Before Tariff\n({peak_label})")
    axes[0].set_xlabel("Hour")
    axes[0].set_ylabel("Month")

    sns.heatmap(
        after_heatmap,
        ax=axes[1],
        cmap="YlOrRd",
        vmin=0,
        vmax=consumption_vmax,
        cbar_kws={
            "label": "Average Peak Consumption (kWh)",
            "ticks": ticks
        }
    )

    axes[1].set_title(f"All Households: After Tariff\n({peak_label})")
    axes[1].set_xlabel("Hour")
    axes[1].set_ylabel("")

    plt.tight_layout()
    plt.show()



def plot_matching_status_heatmaps(
    df_full,
    matched_control_ids,
    price_label="high",
    consumption_vmax=None,
    tick_count=5
):
    """
    2×2 heatmaps:
        1. All never adopters
        2. All adopters BEFORE tariff
        3. Matched never adopters
        4. Unmatched never adopters

    Input df_full should already be filtered by price if needed.
    Example:
        plot_matching_status_heatmaps(
            df_full=month_result_full[month_result_full["price"] == "high"],
            matched_control_ids=control_ids,
            price_label="high"
        )
    """

    matched_control_ids = set(matched_control_ids)

    all_never_ids = set(
        df_full.loc[df_full["tariff_start"].isna(), "aID"].unique()
    )

    all_adopter_ids = set(
        df_full.loc[df_full["tariff_start"].notna(), "aID"].unique()
    )

    unmatched_never_ids = all_never_ids - matched_control_ids

    groups = {
        "All never adopters": df_full[df_full["aID"].isin(all_never_ids)],
        "All adopters BEFORE tariff": df_full[
            (df_full["aID"].isin(all_adopter_ids)) &
            (df_full["tariff_active"] == 0)
        ],
        "Matched never adopters": df_full[df_full["aID"].isin(matched_control_ids)],
        "Unmatched never adopters": df_full[df_full["aID"].isin(unmatched_never_ids)]
    }

    heatmaps = {
        name: _make_average_peak_consumption_heatmap(subset)
        for name, subset in groups.items()
    }

    if consumption_vmax is None:
        consumption_vmax = _safe_max(*heatmaps.values())

    consumption_vmax = _nice_upper_limit(consumption_vmax)
    ticks = _make_nonnegative_ticks(consumption_vmax, n_ticks=tick_count)

    fig, axes = plt.subplots(2, 2, figsize=(14, 10), sharey=True)
    axes = axes.ravel()  

    for ax, (name, heatmap) in zip(axes, heatmaps.items()):
        sns.heatmap(
            heatmap,
            cmap="YlOrRd",
            vmin=0,
            vmax=consumption_vmax,
            ax=ax,
            cbar_kws={
                "label": "Average Peak Consumption (kWh)",
                "ticks": ticks
            }
        )

        ax.set_title(name)
        ax.set_xlabel("Hour")
        ax.set_ylabel("Month")

    title_map = {
        "all": "Average Peak Consumption by Adoption and Matching Status (Overall Peaks)",
        "high": "Average Peak Consumption by Adoption and Matching Status (High-Price Period Peaks)",
        "low": "Average Peak Consumption by Adoption and Matching Status (Low-Price Period Peaks)"
    }

    fig.suptitle(
        title_map.get(price_label, "Average Peak Consumption by Adoption and Matching Status"),
        fontsize=16,
        y=1.05
    )

    plt.tight_layout()
    plt.show()

    
def get_population_heatmap_common_vmax(
    dfs,
    matched_control_ids,
    tick_nice=True
):

    matched_control_ids = set(matched_control_ids)
    heatmaps = []

    for df in dfs:

        all_never_ids = set(
            df.loc[df["tariff_start"].isna(), "aID"].unique()
        )

        all_adopter_ids = set(
            df.loc[df["tariff_start"].notna(), "aID"].unique()
        )

        unmatched_never_ids = all_never_ids - matched_control_ids

        all_before_df = df[
            df["tariff_start"].isna() |
            (
                df["tariff_start"].notna() &
                (df["tariff_active"] == 0)
            )
        ]

        all_after_df = df[
            df["tariff_start"].isna() |
            (
                df["tariff_start"].notna() &
                (df["tariff_active"] == 1)
            )
        ]

        group_dfs = [
            all_before_df,
            all_after_df,
            df[df["aID"].isin(all_never_ids)],
            df[
                (df["aID"].isin(all_adopter_ids)) &
                (df["tariff_active"] == 0)
            ],
            df[
                (df["aID"].isin(matched_control_ids)) &
                (df["tariff_start"].isna())
            ],
            df[
                (df["aID"].isin(unmatched_never_ids)) &
                (df["tariff_start"].isna())
            ]
        ]

        for subset in group_dfs:
            heatmaps.append(
                _make_average_peak_consumption_heatmap(subset)
            )

    vmax = _safe_max(*heatmaps)

    if tick_nice:
        vmax = _nice_upper_limit(vmax)

    return vmax