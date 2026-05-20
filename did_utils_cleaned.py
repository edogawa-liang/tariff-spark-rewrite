# did_utils.py

import os

import numpy as np
import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt


# ============================================================
# Helper functions
# ============================================================

def unique_list(seq):
    """Remove duplicates while preserving order."""
    out = []
    for x in seq:
        if x not in out:
            out.append(x)
    return out


def print_result_table(df, title=None, digits=4):
    """Print a rounded result table."""

    if title is not None:
        print("\n" + title)
        print("=" * len(title))

    out = df.copy()

    numeric_cols = [
        "coef",
        "std_error",
        "p_value",
        "ci_lower",
        "ci_upper",
    ]

    for col in numeric_cols:
        if col in out.columns:
            out[col] = out[col].round(digits)

    print(out.to_string(index=False))


# ============================================================
# Prepare data
# ============================================================

def prepare_price_panel(
    month_result,
    price_value,
    outcome_col="top3_mean_consumption",
):
    """
    Keep only one price period: 'high', 'low', or 'all'.
    """

    df = month_result.copy()
    df = df[df["price"] == price_value].copy()

    df["TIDPUNKT"] = pd.to_datetime(df["TIDPUNKT"]).dt.to_period("M").astype(str)
    df[outcome_col] = pd.to_numeric(df[outcome_col], errors="coerce")

    return df


def build_matched_panel(matches, month_result, outcome_col=None):
    """
    Build matched panel for matched DiD.
    """

    matches = matches.copy()
    month_result = month_result.copy()

    matches["cohort"] = pd.to_datetime(matches["adoption_month"]).dt.to_period("M")

    # Treated households
    treated_map = matches[["treated_id", "cohort"]].drop_duplicates().copy()
    treated_map.columns = ["aID", "cohort"]
    treated_map["treatment"] = 1
    treated_map["match_weight"] = 1.0

    # Matched controls
    matches["n_controls_for_treated"] = matches.groupby(
        ["treated_id", "cohort"]
    )["control_id"].transform("count")

    control_map = matches[
        ["control_id", "cohort", "n_controls_for_treated"]
    ].copy()

    control_map.columns = ["aID", "cohort", "n_controls_for_treated"]
    control_map["treatment"] = 0
    control_map["match_weight"] = 1.0 / control_map["n_controls_for_treated"]

    control_map = (
        control_map
        .groupby(["aID", "cohort", "treatment"], as_index=False)["match_weight"]
        .sum()
    )

    match_map = pd.concat(
        [
            treated_map[["aID", "cohort", "treatment", "match_weight"]],
            control_map[["aID", "cohort", "treatment", "match_weight"]],
        ],
        axis=0,
        ignore_index=True,
    )

    df = month_result.merge(match_map, on="aID", how="inner")

    df["TIDPUNKT"] = pd.to_datetime(df["TIDPUNKT"]).dt.to_period("M")
    df["month_id"] = df["TIDPUNKT"].astype(str)
    df["cohort_id"] = df["cohort"].astype(str)

    df["event_time"] = (df["TIDPUNKT"] - df["cohort"]).apply(lambda x: x.n)

    df["post"] = (df["event_time"] >= 0).astype(int)
    df["D"] = df["treatment"] * df["post"]

    df["entity_id"] = (
        df["aID"].astype(str)
        + "_cohort_"
        + df["cohort_id"].astype(str)
        + "_tr_"
        + df["treatment"].astype(str)
    )

    if outcome_col is not None:
        if outcome_col in df.columns:
            df[outcome_col] = pd.to_numeric(df[outcome_col], errors="coerce")
        else:
            print(f"Warning: outcome column not found: {outcome_col}")

    return df


# ============================================================
# Two-way fixed effects residualization
# ============================================================

def weighted_group_mean(frame, cols, group_col, weight_col):
    """
    Compute weighted group means for multiple columns.
    """

    w = frame[weight_col].astype(float)
    tmp = frame[cols].multiply(w, axis=0)

    numerator = tmp.groupby(frame[group_col]).sum()
    denominator = w.groupby(frame[group_col]).sum()

    means = numerator.div(denominator, axis=0)

    return means


def residualize_twfe(
    data,
    cols,
    entity_col="entity_id",
    time_col="month_id",
    weight_col="match_weight",
    keep_cols=None,
    max_iter=100,
    tol=1e-10,
):
    """
    Residualize columns with respect to entity and time fixed effects
    using iterative weighted demeaning.
    """

    if keep_cols is None:
        keep_cols = []

    base_cols = unique_list([entity_col, time_col, weight_col] + keep_cols + cols)

    work = data[base_cols].copy()
    work = work.dropna(subset=cols + [entity_col, time_col, weight_col])

    R = work[cols].astype(float).copy()

    for _ in range(max_iter):
        old_values = R.values.copy()

        temp = pd.concat(
            [
                work[[entity_col, weight_col]].reset_index(drop=True),
                R.reset_index(drop=True),
            ],
            axis=1,
        )

        entity_means = weighted_group_mean(
            temp,
            cols,
            entity_col,
            weight_col,
        )

        R -= entity_means.reindex(work[entity_col]).to_numpy()

        temp = pd.concat(
            [
                work[[time_col, weight_col]].reset_index(drop=True),
                R.reset_index(drop=True),
            ],
            axis=1,
        )

        time_means = weighted_group_mean(
            temp,
            cols,
            time_col,
            weight_col,
        )

        R -= time_means.reindex(work[time_col]).to_numpy()

        diff = np.nanmax(np.abs(R.values - old_values))

        if diff < tol:
            break

    R.index = work.index

    return work, R


# ============================================================
# Basic DiD estimation
# ============================================================

def fit_basic_did(
    df,
    outcome_col,
    entity_col="entity_id",
    time_col="month_id",
    weight_col="match_weight",
    cluster_col="entity_id",
):
    """
    Estimate:
        Y_it = alpha_i + lambda_t + beta D_it + e_it
    """

    use_cols = unique_list(
        [outcome_col, "D", entity_col, time_col, weight_col, cluster_col]
    )

    data = df[use_cols].dropna(
        subset=[outcome_col, "D", entity_col, time_col, weight_col, cluster_col]
    ).copy()

    work, R = residualize_twfe(
        data=data,
        cols=[outcome_col, "D"],
        entity_col=entity_col,
        time_col=time_col,
        weight_col=weight_col,
        keep_cols=[cluster_col],
    )

    y = R[outcome_col]
    X = R[["D"]]
    weights = work[weight_col].astype(float)

    model = sm.WLS(y, X, weights=weights)

    result = model.fit(
        cov_type="cluster",
        cov_kwds={"groups": work[cluster_col]},
    )

    coef = result.params["D"]
    se = result.bse["D"]

    out = {
        "outcome": outcome_col,
        "coef": coef,
        "std_error": se,
        "p_value": result.pvalues["D"],
        "ci_lower": coef - 1.96 * se,
        "ci_upper": coef + 1.96 * se,
        "n_obs": len(work),
        "n_entities": work[entity_col].nunique(),
    }

    return result, out


def run_basic_did_one(df, outcome_col, cluster_col="aID"):
    """
    Run baseline DiD for one outcome column.
    """

    model, result = fit_basic_did(
        df=df,
        outcome_col=outcome_col,
        cluster_col=cluster_col,
    )

    results_df = pd.DataFrame([result])

    return model, results_df


def run_basic_did_by_cohort(
    df,
    outcome_col,
    cohorts=None,
    min_treated=30,
    entity_col="entity_id",
    time_col="month_id",
    weight_col="match_weight",
    cluster_col="aID",
):
    """
    Run baseline DiD separately for each adoption cohort.
    """

    if cohorts is None:
        cohorts = sorted(df["cohort_id"].dropna().unique())

    cohort_models = {}
    all_results = []

    for cohort in cohorts:
        df_g = df[df["cohort_id"] == cohort].copy()

        n_treated = df_g.loc[df_g["treatment"] == 1, "aID"].nunique()

        if n_treated < min_treated:
            print(f"Skipping cohort {cohort}: only {n_treated} treated households")
            continue

        n_periods = df_g.loc[df_g["D"] == 1, "month_id"].nunique()

        print(f"Running cohort {cohort}, treated households = {n_treated}")

        try:
            model, row = fit_basic_did(
                df=df_g,
                outcome_col=outcome_col,
                entity_col=entity_col,
                time_col=time_col,
                weight_col=weight_col,
                cluster_col=cluster_col,
            )

            row["cohort"] = cohort
            row["n_treated"] = n_treated
            row["n_periods"] = n_periods

            cohort_models[cohort] = model
            all_results.append(row)

        except Exception as e:
            print(f"Failed cohort {cohort}: {e}")

    if len(all_results) == 0:
        return cohort_models, pd.DataFrame()

    cohort_results_df = pd.DataFrame(all_results)

    preferred_cols = [
        "cohort",
        "outcome",
        "coef",
        "std_error",
        "p_value",
        "ci_lower",
        "ci_upper",
        "n_treated",
        "n_periods",
        "n_obs",
        "n_entities",
    ]

    existing_cols = [c for c in preferred_cols if c in cohort_results_df.columns]
    other_cols = [c for c in cohort_results_df.columns if c not in existing_cols]

    cohort_results_df = cohort_results_df[existing_cols + other_cols]

    return cohort_models, cohort_results_df


# ============================================================
# Event-study estimation
# ============================================================

def make_event_dummies(
    data,
    event_col="event_time",
    treatment_col="treatment",
    event_window=(-12, 12),
    reference=-1,
):
    """
    Create treatment x event-time dummies.
    Reference period is omitted.
    """

    data = data.copy()

    min_k, max_k = event_window

    data = data[
        (data[event_col] >= min_k)
        & (data[event_col] <= max_k)
    ].copy()

    event_terms = []

    for k in range(min_k, max_k + 1):
        if k == reference:
            continue

        col = f"event_{k}"

        data[col] = (
            (data[event_col] == k)
            & (data[treatment_col] == 1)
        ).astype(int)

        event_terms.append(col)

    return data, event_terms


def fit_event_study(
    df,
    outcome_col,
    event_window=(-12, 12),
    reference=-1,
    entity_col="entity_id",
    time_col="month_id",
    weight_col="match_weight",
    cluster_col="entity_id",
):
    """
    Estimate event-study model:
        Y_it = alpha_i + lambda_t
               + sum beta_k [Treated_i x 1(event_time = k)]
               + e_it
    """

    data, event_terms = make_event_dummies(
        df,
        event_col="event_time",
        treatment_col="treatment",
        event_window=event_window,
        reference=reference,
    )

    use_cols = unique_list(
        [outcome_col] + event_terms + [entity_col, time_col, weight_col, cluster_col]
    )

    data = data[use_cols].dropna(
        subset=[outcome_col, entity_col, time_col, weight_col, cluster_col]
    ).copy()

    valid_terms = []

    for col in event_terms:
        if col in data.columns and data[col].sum() > 0 and data[col].var() > 0:
            valid_terms.append(col)

    if len(valid_terms) == 0:
        raise ValueError("No valid event-time dummies. Check event_window and data.")

    cols_to_residualize = [outcome_col] + valid_terms

    work, R = residualize_twfe(
        data=data,
        cols=cols_to_residualize,
        entity_col=entity_col,
        time_col=time_col,
        weight_col=weight_col,
        keep_cols=[cluster_col],
    )

    y = R[outcome_col]
    X = R[valid_terms]
    weights = work[weight_col].astype(float)

    model = sm.WLS(y, X, weights=weights)

    result = model.fit(
        cov_type="cluster",
        cov_kwds={"groups": work[cluster_col]},
    )

    rows = []

    for term in valid_terms:
        k = int(term.replace("event_", ""))
        coef = result.params[term]
        se = result.bse[term]

        rows.append({
            "outcome": outcome_col,
            "event_time": k,
            "coef": coef,
            "std_error": se,
            "p_value": result.pvalues[term],
            "ci_lower": coef - 1.96 * se,
            "ci_upper": coef + 1.96 * se,
        })

    event_df = pd.DataFrame(rows).sort_values("event_time")

    return result, event_df


def run_event_study_one(
    df,
    outcome_col,
    event_window=(-12, 12),
    reference=-1,
    cluster_col="aID",
):
    """
    Run event-study for one outcome column.
    """

    model, event_df = fit_event_study(
        df=df,
        outcome_col=outcome_col,
        event_window=event_window,
        reference=reference,
        cluster_col=cluster_col,
    )

    return model, event_df


def run_event_study_by_cohort(
    df,
    outcome_col,
    cohorts=None,
    event_window=(-12, 12),
    reference=-1,
    min_treated=30,
    cluster_col="aID",
):
    """
    Run event-study separately for each adoption cohort.
    """

    if cohorts is None:
        cohorts = sorted(df["cohort_id"].dropna().unique())

    cohort_event_models = {}
    all_results = []

    for cohort in cohorts:
        df_g = df[df["cohort_id"] == cohort].copy()

        n_treated = df_g.loc[df_g["treatment"] == 1, "aID"].nunique()

        if n_treated < min_treated:
            print(f"Skipping cohort {cohort}: only {n_treated} treated households")
            continue

        print(f"Running cohort {cohort}, treated households = {n_treated}")

        try:
            model, event_df = fit_event_study(
                df=df_g,
                outcome_col=outcome_col,
                event_window=event_window,
                reference=reference,
                cluster_col=cluster_col,
            )

            event_df["cohort"] = cohort
            event_df["n_treated"] = n_treated

            cohort_event_models[cohort] = model
            all_results.append(event_df)

        except Exception as e:
            print(f"Failed cohort {cohort}: {e}")

    if len(all_results) == 0:
        return cohort_event_models, pd.DataFrame()

    cohort_event_results_df = pd.concat(all_results, ignore_index=True)

    return cohort_event_models, cohort_event_results_df


# ============================================================
# Save CSV results
# ============================================================

def save_results(
    overall_df,
    cohort_df,
    event_df,
    cohort_event_df,
    save_path,
):
    """
    Save DID result tables to CSV.

    Saves:
        overall_effect.csv
        cohort_effect.csv
        dynamic_effect.csv
        dynamic_cohort_effect.csv
    """

    os.makedirs(save_path, exist_ok=True)

    overall_df.to_csv(
        os.path.join(save_path, "overall_effect.csv"),
        index=False,
    )

    cohort_df.to_csv(
        os.path.join(save_path, "cohort_effect.csv"),
        index=False,
    )

    event_df.to_csv(
        os.path.join(save_path, "dynamic_effect.csv"),
        index=False,
    )

    cohort_event_df.to_csv(
        os.path.join(save_path, "dynamic_cohort_effect.csv"),
        index=False,
    )

    print(f"✅ CSV results saved to: {save_path}")


# ============================================================
# Plot event-study results
# ============================================================

def plot_event_study(
    event_results_df,
    outcome_col,
    title=None,
    save_path=None,
    filename="dynamic_effect.png",
):
    """
    Plot overall dynamic DID / event-study effect.

    This function keeps the original name:
        plot_event_study(...)

    It both shows the figure and optionally saves it.
    """

    d = event_results_df[
        event_results_df["outcome"] == outcome_col
    ].copy()

    d = d.sort_values("event_time")

    if d.empty:
        raise ValueError(f"No event-study results found for outcome: {outcome_col}")

    fig, ax = plt.subplots(figsize=(9, 5))

    ax.plot(
        d["event_time"],
        d["coef"],
        linewidth=2,
    )

    ax.fill_between(
        d["event_time"],
        d["ci_lower"],
        d["ci_upper"],
        alpha=0.2,
    )

    ax.axhline(0, linestyle="--", linewidth=1)
    ax.axvline(0, linestyle="--", linewidth=1)

    ax.set_xlabel("Event Time")
    ax.set_ylabel("Treatment Effect")
    ax.set_title(title if title is not None else f"Event-study: {outcome_col}")

    fig.tight_layout()

    if save_path is not None:
        os.makedirs(save_path, exist_ok=True)
        fig.savefig(
            os.path.join(save_path, filename),
            dpi=300,
            bbox_inches="tight",
        )

    plt.show()

    return fig, ax


def plot_event_study_by_cohort(
    cohort_event_results_df,
    outcome_col,
    cohorts=None,
    title=None,
    save_path=None,
):
    """
    Plot one cohort-specific dynamic DID / event-study figure per cohort.

    This function keeps the original name:
        plot_event_study_by_cohort(...)

    It shows every cohort figure and optionally saves every figure.
    """

    d0 = cohort_event_results_df[
        cohort_event_results_df["outcome"] == outcome_col
    ].copy()

    if cohorts is not None:
        d0 = d0[d0["cohort"].isin(cohorts)]

    if d0.empty:
        raise ValueError(f"No cohort event-study results found for outcome: {outcome_col}")

    if save_path is not None:
        os.makedirs(save_path, exist_ok=True)

    figures = {}

    for cohort in sorted(d0["cohort"].unique()):

        d = d0[d0["cohort"] == cohort].copy()
        d = d.sort_values("event_time")

        fig, ax = plt.subplots(figsize=(7, 4.5))

        ax.plot(
            d["event_time"],
            d["coef"],
            linewidth=2,
        )

        ax.fill_between(
            d["event_time"],
            d["ci_lower"],
            d["ci_upper"],
            alpha=0.2,
        )

        ax.axhline(0, linestyle="--", linewidth=1)
        ax.axvline(0, linestyle="--", linewidth=1)

        ax.set_xlabel("Event Time")
        ax.set_ylabel("Treatment Effect")
        ax.set_title(
            title if title is not None
            else f"Event-study: Cohort = {cohort}"
        )

        fig.tight_layout()

        if save_path is not None:
            safe_cohort = str(cohort).replace("/", "_").replace(":", "_")
            fig.savefig(
                os.path.join(save_path, f"dynamic_cohort_{safe_cohort}.png"),
                dpi=300,
                bbox_inches="tight",
            )

        plt.show()

        figures[cohort] = (fig, ax)

    return figures


def plot_all_cohorts(
    cohort_event_results_df,
    outcome_col,
    cohorts=None,
    save_path=None,
    filename="dynamic_all_cohorts.png",
):
    """
    Plot all cohort-specific dynamic effects in one figure.

    It both shows the figure and optionally saves it.
    """

    d0 = cohort_event_results_df[
        cohort_event_results_df["outcome"] == outcome_col
    ].copy()

    if cohorts is not None:
        d0 = d0[d0["cohort"].isin(cohorts)]

    if d0.empty:
        raise ValueError(f"No cohort event-study results found for outcome: {outcome_col}")

    fig, ax = plt.subplots(figsize=(9, 5))

    for cohort in sorted(d0["cohort"].unique()):

        d = d0[d0["cohort"] == cohort].copy()
        d = d.sort_values("event_time")

        ax.plot(
            d["event_time"],
            d["coef"],
            alpha=0.6,
            linewidth=2,
            label=str(cohort),
        )

    ax.axhline(0, linestyle="--", linewidth=1)
    ax.axvline(0, linestyle="--", linewidth=1)

    ax.set_xlabel("Event Time")
    ax.set_ylabel("Treatment Effect")
    ax.set_title("Dynamic Treatment Effect by Cohort")

    ax.legend(
        title="Cohort",
        bbox_to_anchor=(1.05, 1),
        loc="upper left",
    )

    fig.tight_layout()

    if save_path is not None:
        os.makedirs(save_path, exist_ok=True)
        fig.savefig(
            os.path.join(save_path, filename),
            dpi=300,
            bbox_inches="tight",
        )

    plt.show()

    return fig, ax


# ============================================================
# Save all outputs: CSV + figures
# ============================================================

def save_all_outputs(
    overall_df,
    cohort_df,
    event_df,
    cohort_event_df,
    outcome_col,
    save_path,
):
    """
    Save all CSV results and show + save all figures.

    This function will:
        1. save overall_df, cohort_df, event_df, cohort_event_df as CSV
        2. show and save the overall dynamic event-study figure
        3. show and save one dynamic figure for each cohort
        4. show and save one figure with all cohorts together
    """

    os.makedirs(save_path, exist_ok=True)

    save_results(
        overall_df=overall_df,
        cohort_df=cohort_df,
        event_df=event_df,
        cohort_event_df=cohort_event_df,
        save_path=save_path,
    )

    plot_event_study(
        event_results_df=event_df,
        outcome_col=outcome_col,
        save_path=save_path,
        filename="dynamic_effect.png",
    )

    plot_event_study_by_cohort(
        cohort_event_results_df=cohort_event_df,
        outcome_col=outcome_col,
        save_path=save_path,
    )

    plot_all_cohorts(
        cohort_event_results_df=cohort_event_df,
        outcome_col=outcome_col,
        save_path=save_path,
        filename="dynamic_all_cohorts.png",
    )

    print(f"✅ All outputs saved to: {save_path}")
