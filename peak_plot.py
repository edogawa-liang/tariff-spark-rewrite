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


def _round_up_to_step(value, step):
    """
    Round a positive scale limit up to the nearest tick step.
    This makes colorbar ticks look consistent across panels.
    """
    if step is None or step <= 0:
        return value

    if value is None or pd.isna(value):
        return step

    value = float(value)
    if value <= 0:
        return step

    return np.ceil(value / step) * step


def _make_nonnegative_ticks(vmax, step=2):
    """
    Create fixed ticks for non-negative heatmaps, e.g. 0, 2, 4, 6, 8.
    """
    vmax = _round_up_to_step(vmax, step)
    return np.arange(0, vmax + step * 0.5, step)


def _make_centered_ticks(vmax, n_ticks=5):
    """
    Create symmetric ticks for difference heatmaps centered at zero.
    """
    vmax = float(vmax)
    if vmax <= 0 or pd.isna(vmax):
        vmax = 1
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

def plot_tariff_peak_heatmap(df, price_label="all", count_tick_step=2):

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
    count_vmax = _round_up_to_step(count_vmax, count_tick_step)
    count_ticks = _make_nonnegative_ticks(count_vmax, count_tick_step)

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
    consumption_tick_step=2,
    diff_tick_count=5
):
    """
    Plot tariff peak consumption heatmaps.

    Recommended use for the main text:
        mode = "mean"

    Color-scale logic:
        - The three consumption panels use the SAME scale:
          Never adopters, Adopters BEFORE, and Adopters AFTER.
        - The difference panel uses a separate diverging scale centered at zero.

    Parameters
    ----------
    df : pandas.DataFrame
        Dataset containing tariff_start, tariff_active, peak time, and peak consumption columns.

    price_label : {"all", "high", "low"}
        Used only for the figure title.

    modes : tuple/list of {"mean", "sum"}
        "mean" is recommended for descriptive before/after comparison.
        "sum" is available only if you explicitly want total aggregate burden.

    consumption_vmax : None, scalar, or dict
        If None, the three consumption panels share a local scale within this figure.
        If scalar, that value is used as vmax for all modes.
        If dict, use format {"mean": value, "sum": value}.

        To make Figures 10, 11, and 12 comparable, first run:
            scales = get_tariff_consumption_color_scales(
                df_high, df_low, df_all,
                modes=("mean",)
            )
        Then pass:
            consumption_vmax=scales["consumption_vmax"]

    diff_vmax : None, scalar, or dict
        Optional vmax for the difference panel. If None, the difference panel
        uses its own local symmetric scale around zero.

    difference_scale : {"local", "common"}
        "local" means each figure's difference panel is scaled separately.
        "common" means diff_vmax should be supplied, usually from
        get_tariff_consumption_color_scales(..., include_difference=True).

    consumption_tick_step : int or float
        Fixed tick interval for the three consumption colorbars.
        Default is 2, so the colorbars show 0, 2, 4, 6, 8, ...

    diff_tick_count : int
        Number of ticks shown on the difference colorbar.
        Default is 5, so the ticks are symmetric around zero.
    """

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
        # The consumption vmax is rounded up to the tick interval so all three
        # consumption colorbars show the same tick labels.
        this_consumption_vmax = _round_up_to_step(
            this_consumption_vmax,
            consumption_tick_step
        )
        consumption_ticks = _make_nonnegative_ticks(
            this_consumption_vmax,
            consumption_tick_step
        )

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
