import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import norm, ttest_1samp


# Build matched panel
def build_matched_panel(matches, month_result):

    matches = matches.copy()
    month_result = month_result.copy()

    matches["cohort"] = pd.to_datetime(matches["adoption_month"]).dt.to_period("M")

    treated_map = matches[["treated_id", "cohort"]].drop_duplicates().copy()
    treated_map.columns = ["aID", "cohort"]
    treated_map["treatment"] = 1

    control_map = matches[["control_id", "cohort"]].copy()
    control_map.columns = ["aID", "cohort"]
    control_map["treatment"] = 0

    match_map = pd.concat([treated_map, control_map], axis=0, ignore_index=True)

    df = month_result.merge(match_map, on="aID", how="inner")

    df["TIDPUNKT"] = pd.to_datetime(df["TIDPUNKT"]).dt.to_period("M")
    df["event_time"] = (df["TIDPUNKT"] - df["cohort"]).apply(lambda x: x.n)

    return df


# Monthly treatment effect + p-value
def compute_effect(df, outcome_col="top3_mean_consumption"):

    results = []

    for t in sorted(df["event_time"].dropna().unique()):
        d = df[df["event_time"] == t]

        treated = d[d["treatment"] == 1][outcome_col]
        control = d[d["treatment"] == 0][outcome_col]

        if len(treated) < 2 or len(control) < 2:
            continue

        mean_t = treated.mean()
        mean_c = control.mean()

        var_t = treated.var(ddof=1)
        var_c = control.var(ddof=1)

        n_t = len(treated)
        n_c = len(control)

        effect = mean_t - mean_c
        se = np.sqrt(var_t / n_t + var_c / n_c)

        t_stat = effect / se if se > 0 else np.nan
        p_value = 2 * (1 - norm.cdf(abs(t_stat))) if se > 0 else np.nan

        results.append({
            "event_time": t,
            "mean_treated": mean_t,
            "mean_control": mean_c,
            "effect": effect,
            "se": se,
            "t_stat": t_stat,
            "p_value": p_value,
            "n_treated": n_t,
            "n_control": n_c
        })

    return pd.DataFrame(results)


# Cohort-level monthly effect + p-value
def compute_effect_by_cohort(df, outcome_col="top3_mean_consumption"):

    results = []

    for c in sorted(df["cohort"].dropna().unique()):
        d_cohort = df[df["cohort"] == c]

        for t in sorted(d_cohort["event_time"].dropna().unique()):
            d = d_cohort[d_cohort["event_time"] == t]

            treated = d[d["treatment"] == 1][outcome_col]
            control = d[d["treatment"] == 0][outcome_col]

            if len(treated) < 2 or len(control) < 2:
                continue

            mean_t = treated.mean()
            mean_c = control.mean()

            var_t = treated.var(ddof=1)
            var_c = control.var(ddof=1)

            n_t = len(treated)
            n_c = len(control)

            effect = mean_t - mean_c
            se = np.sqrt(var_t / n_t + var_c / n_c)

            t_stat = effect / se if se > 0 else np.nan
            p_value = 2 * (1 - norm.cdf(abs(t_stat))) if se > 0 else np.nan

            results.append({
                "cohort": c,
                "event_time": t,
                "effect": effect,
                "se": se,
                "t_stat": t_stat,
                "p_value": p_value,
                "n_treated": n_t,
                "n_control": n_c
            })

    return pd.DataFrame(results)


# Confidence interval
def add_confidence_interval(effect_df, alpha=0.05):

    effect_df = effect_df.copy()

    z = norm.ppf(1 - alpha / 2)

    effect_df["ci_low"] = effect_df["effect"] - z * effect_df["se"]
    effect_df["ci_high"] = effect_df["effect"] + z * effect_df["se"]

    return effect_df


# Pre-trend test
def pretrend_test(effect_df, pre_start=-12, pre_end=-1):
    """
    Test whether pre-treatment effects are jointly close to zero
    using a simple one-sample t-test on estimated pre-period effects.

    Example:
    pre_start=-12, pre_end=-1 means event_time from -12 to -1.
    """

    pre = effect_df[
        (effect_df["event_time"] >= pre_start) &
        (effect_df["event_time"] <= pre_end)
    ].copy()

    pre = pre.dropna(subset=["effect"])

    if len(pre) < 2:
        return {
            "pre_start": pre_start,
            "pre_end": pre_end,
            "n_periods": len(pre),
            "mean_pre_effect": np.nan,
            "t_stat": np.nan,
            "p_value": np.nan
        }

    test = ttest_1samp(pre["effect"], popmean=0)

    return {
        "pre_start": pre_start,
        "pre_end": pre_end,
        "n_periods": len(pre),
        "mean_pre_effect": pre["effect"].mean(),
        "t_stat": test.statistic,
        "p_value": test.pvalue
    }


# Dynamic effect plot
def plot_dynamic_effect(effect_df):

    plt.figure(figsize=(8, 5))

    plt.plot(effect_df["event_time"], effect_df["effect"], marker="o")

    plt.fill_between(
        effect_df["event_time"],
        effect_df["ci_low"],
        effect_df["ci_high"],
        alpha=0.2
    )

    plt.axhline(0, linestyle="--")
    plt.axvline(0, linestyle="--")

    plt.xlabel("Event Time")
    plt.ylabel("Treatment Effect")
    plt.title("Dynamic Treatment Effect")

    plt.show()


def plot_dynamic_by_cohort(effect_df):

    for c in effect_df["cohort"].unique():

        d = effect_df[effect_df["cohort"] == c].sort_values("event_time")

        plt.figure(figsize=(6, 4))

        plt.plot(d["event_time"], d["effect"], marker="o")

        plt.fill_between(
            d["event_time"],
            d["ci_low"],
            d["ci_high"],
            alpha=0.2
        )

        plt.axhline(0, linestyle="--")
        plt.axvline(0, linestyle="--")

        plt.title(f"Cohort = {c}")
        plt.xlabel("Event Time")
        plt.ylabel("Treatment Effect")

        plt.show()


def plot_all_cohorts(effect_df):

    plt.figure(figsize=(8, 5))

    for c in effect_df["cohort"].unique():

        d = effect_df[effect_df["cohort"] == c].sort_values("event_time")
        plt.plot(d["event_time"], d["effect"], alpha=0.5, label=str(c))

    plt.axhline(0, linestyle="--")
    plt.axvline(0, linestyle="--")

    plt.xlabel("Event Time")
    plt.ylabel("Treatment Effect")
    plt.title("Dynamic Effect by Cohort")

    plt.legend(bbox_to_anchor=(1.05, 1))
    plt.show()


# Average treatment effect ATT + p-value
def compute_average_treatment_effect(effect_df, post_period_only=True):

    if post_period_only:
        df = effect_df[effect_df["event_time"] >= 0].copy()
    else:
        df = effect_df.copy()

    df = df.dropna(subset=["effect"])

    att = df["effect"].mean()
    se = df["effect"].std(ddof=1) / np.sqrt(len(df))

    t_stat = att / se if se > 0 else np.nan
    p_value = 2 * (1 - norm.cdf(abs(t_stat))) if se > 0 else np.nan

    ci_low = att - 1.96 * se
    ci_high = att + 1.96 * se

    return {
        "ATT": att,
        "SE": se,
        "t_stat": t_stat,
        "p_value": p_value,
        "CI_low": ci_low,
        "CI_high": ci_high,
        "n_periods": len(df)
    }


def compute_att_by_cohort(effect_df, post_period_only=True):

    results = []

    for c in effect_df["cohort"].unique():

        d = effect_df[effect_df["cohort"] == c].copy()

        if post_period_only:
            d = d[d["event_time"] >= 0]

        d = d.dropna(subset=["effect"])

        if len(d) < 2:
            continue

        att = d["effect"].mean()
        se = d["effect"].std(ddof=1) / np.sqrt(len(d))

        t_stat = att / se if se > 0 else np.nan
        p_value = 2 * (1 - norm.cdf(abs(t_stat))) if se > 0 else np.nan

        ci_low = att - 1.96 * se
        ci_high = att + 1.96 * se

        results.append({
            "cohort": c,
            "ATT": att,
            "SE": se,
            "t_stat": t_stat,
            "p_value": p_value,
            "CI_low": ci_low,
            "CI_high": ci_high,
            "n_periods": len(d)
        })

    return pd.DataFrame(results)


def run_full_analysis(
    df,
    outcome_col="top3_mean_consumption",
    pre_start=-12,
    pre_end=-1
):

    print("===== OVERALL EFFECT =====")

    overall_df = compute_effect(df, outcome_col)
    overall_df = add_confidence_interval(overall_df)

    print("\nOverall dynamic effect:")
    print(overall_df.round(4))

    plot_dynamic_effect(overall_df)

    print("\n===== PRE-TREND TEST =====")
    pretrend = pretrend_test(overall_df, pre_start=pre_start, pre_end=pre_end)
    print(pretrend)

    print("\n===== OVERALL ATT =====")
    att = compute_average_treatment_effect(overall_df)
    print(att)

    print("\n===== COHORT DYNAMIC EFFECT =====")

    cohort_df = compute_effect_by_cohort(df, outcome_col)
    cohort_df = add_confidence_interval(cohort_df)

    plot_dynamic_by_cohort(cohort_df)
    plot_all_cohorts(cohort_df)

    print("\n===== ATT BY COHORT =====")

    att_cohort_df = compute_att_by_cohort(cohort_df)
    print(att_cohort_df.round(4))

    return {
        "overall": overall_df,
        "cohort": cohort_df,
        "att": att,
        "att_by_cohort": att_cohort_df,
        "pretrend": pretrend
    }


def save_results(results, save_path):

    os.makedirs(save_path, exist_ok=True)

    if "overall" in results:
        results["overall"].to_csv(
            os.path.join(save_path, "overall_dynamic.csv"),
            index=False
        )

    if "cohort" in results:
        results["cohort"].to_csv(
            os.path.join(save_path, "cohort_dynamic.csv"),
            index=False
        )

    if "att" in results:
        pd.DataFrame([results["att"]]).to_csv(
            os.path.join(save_path, "att_overall.csv"),
            index=False
        )

    if "att_by_cohort" in results:
        results["att_by_cohort"].to_csv(
            os.path.join(save_path, "att_by_cohort.csv"),
            index=False
        )

    if "pretrend" in results:
        pd.DataFrame([results["pretrend"]]).to_csv(
            os.path.join(save_path, "pretrend_test.csv"),
            index=False
        )

    print(f"✅ Results saved to: {save_path}")