import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import norm

# Build matched panel
def build_matched_panel(matches, month_result):
    """
    Build matched panel dataset with:
    - treated kept once per cohort
    - controls allowed to repeat across pairs
    - event_time
    """

    matches = matches.copy()
    month_result = month_result.copy()

    # 1. cohort
    matches["cohort"] = pd.to_datetime(matches["adoption_month"]).dt.to_period("M")

    # 2. treated: drop duplicates
    treated_map = matches[["treated_id", "cohort"]].drop_duplicates().copy()
    treated_map.columns = ["aID", "cohort"]
    treated_map["treatment"] = 1

    # 3. controls: keep duplicates (with replacement)
    control_map = matches[["control_id", "cohort"]].copy()
    control_map.columns = ["aID", "cohort"]
    control_map["treatment"] = 0

    # 4. combine
    match_map = pd.concat([treated_map, control_map], axis=0, ignore_index=True)

    # 5. merge panel
    df = month_result.merge(match_map, on="aID", how="inner")

    # 6. time
    df["TIDPUNKT"] = pd.to_datetime(df["TIDPUNKT"]).dt.to_period("M")
    df["event_time"] = (df["TIDPUNKT"] - df["cohort"]).apply(lambda x: x.n)

    return df


# monthly treatment effect 
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

        results.append({
            "event_time": t,
            "mean_treated": mean_t,
            "mean_control": mean_c,
            "effect": effect,
            "se": se,
            "t_stat": t_stat,
            "n_treated": n_t,
            "n_control": n_c
        })

    return pd.DataFrame(results)


def compute_effect_by_cohort(df, outcome_col="top3_mean_consumption"):

    results = []

    cohorts = sorted(df["cohort"].dropna().unique())

    for c in cohorts:
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

            results.append({
                "cohort": c,
                "event_time": t,
                "effect": effect,
                "se": se,
                "n_treated": n_t,
                "n_control": n_c
            })

    return pd.DataFrame(results)


# Confidence Interval
def add_confidence_interval(effect_df, alpha=0.05):
    z = norm.ppf(1 - alpha / 2)

    effect_df["ci_low"] = effect_df["effect"] - z * effect_df["se"]
    effect_df["ci_high"] = effect_df["effect"] + z * effect_df["se"]

    return effect_df


# dynamic effect
def plot_dynamic_effect(effect_df):
    plt.figure(figsize=(8,5))

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

    cohorts = effect_df["cohort"].unique()

    for c in cohorts:
        d = effect_df[effect_df["cohort"] == c].sort_values("event_time")

        plt.figure(figsize=(6,4))

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

    plt.figure(figsize=(8,5))

    for c in effect_df["cohort"].unique():
        d = effect_df[effect_df["cohort"] == c].sort_values("event_time")

        plt.plot(d["event_time"], d["effect"], alpha=0.5, label=str(c))

    plt.axhline(0, linestyle="--")
    plt.axvline(0, linestyle="--")

    plt.xlabel("Event Time")
    plt.ylabel("Treatment Effect")
    plt.title("Dynamic Effect by Cohort")

    plt.legend(bbox_to_anchor=(1.05,1))
    plt.show()


# Average treatment effect（ATT）
def compute_average_treatment_effect(effect_df, post_period_only=True):
    """
    Compute average treatment effect (ATT)
    """

    if post_period_only:
        df = effect_df[effect_df["event_time"] >= 0]
    else:
        df = effect_df.copy()

    att = df["effect"].mean()

    # 簡單 SE（平均）
    se = df["effect"].std(ddof=1) / np.sqrt(len(df))

    ci_low = att - 1.96 * se
    ci_high = att + 1.96 * se

    return {
        "ATT": att,
        "SE": se,
        "CI_low": ci_low,
        "CI_high": ci_high
    }


def compute_att_by_cohort(effect_df, post_period_only=True):

    results = []

    for c in effect_df["cohort"].unique():

        d = effect_df[effect_df["cohort"] == c]

        if post_period_only:
            d = d[d["event_time"] >= 0]

        if len(d) == 0:
            continue

        att = d["effect"].mean()
        se = d["effect"].std(ddof=1) / np.sqrt(len(d))

        ci_low = att - 1.96 * se
        ci_high = att + 1.96 * se

        results.append({
            "cohort": c,
            "ATT": att,
            "SE": se,
            "CI_low": ci_low,
            "CI_high": ci_high,
            "n_periods": len(d)
        })

    return pd.DataFrame(results)


def run_full_analysis(df, outcome_col="top3_mean_consumption"):

    print("===== OVERALL EFFECT =====")

    # overall
    overall_df = compute_effect(df, outcome_col)
    overall_df = add_confidence_interval(overall_df)

    plot_dynamic_effect(overall_df)

    att = compute_average_treatment_effect(overall_df)

    print("ATT:", att)

    print("\n===== COHORT DYNAMIC EFFECT =====")

    # cohort dynamic
    cohort_df = compute_effect_by_cohort(df, outcome_col)
    cohort_df = add_confidence_interval(cohort_df)

    plot_dynamic_by_cohort(cohort_df)
    plot_all_cohorts(cohort_df)

    print("\n===== ATT BY COHORT =====")

    att_cohort_df = compute_att_by_cohort(cohort_df)

    print(att_cohort_df)

    return {
        "overall": overall_df,
        "cohort": cohort_df,
        "att": att,
        "att_by_cohort": att_cohort_df
    }



def save_results(results, save_path):
    """
    Save results dict to CSV files
    """

    os.makedirs(save_path, exist_ok=True)

    # 1. overall dynamic
    if "overall" in results:
        results["overall"].to_csv(
            os.path.join(save_path, "overall_dynamic.csv"),
            index=False
        )

    # 2. cohort dynamic
    if "cohort" in results:
        results["cohort"].to_csv(
            os.path.join(save_path, "cohort_dynamic.csv"),
            index=False
        )

    # 3. overall ATT (dict → DataFrame)
    if "att" in results:
        pd.DataFrame([results["att"]]).to_csv(
            os.path.join(save_path, "att_overall.csv"),
            index=False
        )

    # 4. cohort ATT
    if "att_by_cohort" in results:
        results["att_by_cohort"].to_csv(
            os.path.join(save_path, "att_by_cohort.csv"),
            index=False
        )

    print(f"✅ Results saved to: {save_path}")


    results = run_full_analysis(df)

save_results(results, "/lakehouse/default/Files/output/matching_result")