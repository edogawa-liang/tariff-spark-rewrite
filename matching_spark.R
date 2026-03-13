library(SparkR)
library(ggplot2)

# =========================================================
# Spark session
# =========================================================
sparkR.session(
  appName = "risk_set_matching_peak_sparkr"
)

# =========================================================
# Helpers
# =========================================================
sql_quote <- function(x) {
  paste0("'", gsub("'", "\\\\'", as.character(x)), "'")
}

create_dirs <- function(dirs) {
  for (d in dirs) {
    if (!dir.exists(d)) {
      dir.create(d, recursive = TRUE)
    }
  }
}

# ---------------------------------------------------------
# Prepare data
# ---------------------------------------------------------
prepare_data <- function(
    data,
    id_col = "aID",
    month_col = "TIDPUNKT",
    adoption_col = "tariff_start",
    price_filter = "all",
    view_name = "monthly_agg_prepared"
) {
  createOrReplaceTempView(data, "monthly_agg_raw")

  qry <- paste0("
    SELECT
      CAST(", id_col, " AS STRING) AS id,
      TO_DATE(", month_col, ") AS month,
      TO_DATE(", adoption_col, ") AS adoption_month,
      price,
      CAST(top3_mean_consumption AS DOUBLE) AS top3_mean_consumption
    FROM monthly_agg_raw
    WHERE price = ", sql_quote(price_filter), "
  ")

  df <- sql(qry)
  createOrReplaceTempView(df, view_name)
  invisible(df)
}

# ---------------------------------------------------------
# Build profile SQL for a given Ti
# mode = 'time_series' or 'summary'
# id_condition is SQL predicate on id/adoption_month
# ---------------------------------------------------------
build_profile_sql <- function(
    view_name,
    Ti,
    lookback_months = 12,
    feature_mode = "time_series",
    id_condition = "1 = 1"
) {
  Ti_chr <- as.character(Ti)

  if (!feature_mode %in% c("time_series", "summary")) {
    stop("feature_mode must be 'time_series' or 'summary'")
  }

  if (feature_mode == "time_series") {
    peak_expr <- paste(
      sprintf(
        "MAX(CASE WHEN pos = %d THEN top3_mean_consumption END) AS peak_%d",
        seq_len(lookback_months),
        seq_len(lookback_months)
      ),
      collapse = ",\n      "
    )

    qry <- paste0("
      WITH base AS (
        SELECT
          id,
          month,
          adoption_month,
          top3_mean_consumption
        FROM ", view_name, "
        WHERE ", id_condition, "
          AND month < DATE(", sql_quote(Ti_chr), ")
          AND month >= ADD_MONTHS(DATE(", sql_quote(Ti_chr), "), -", lookback_months, ")
      ),
      ranked AS (
        SELECT
          id,
          month,
          adoption_month,
          top3_mean_consumption,
          ROW_NUMBER() OVER (PARTITION BY id ORDER BY month DESC) AS rn_desc,
          COUNT(*) OVER (PARTITION BY id) AS n_obs
        FROM base
      ),
      selected AS (
        SELECT
          id,
          top3_mean_consumption,
          ", lookback_months, " - rn_desc + 1 AS pos
        FROM ranked
        WHERE rn_desc <= ", lookback_months, "
          AND n_obs >= ", lookback_months, "
      )
      SELECT
        id,
        ", peak_expr, "
      FROM selected
      GROUP BY id
    ")

    return(sql(qry))
  }

  if (feature_mode == "summary") {
    qry <- paste0("
      WITH base AS (
        SELECT
          id,
          month,
          adoption_month,
          top3_mean_consumption
        FROM ", view_name, "
        WHERE ", id_condition, "
          AND month < DATE(", sql_quote(Ti_chr), ")
          AND month >= ADD_MONTHS(DATE(", sql_quote(Ti_chr), "), -", lookback_months, ")
      ),
      ranked AS (
        SELECT
          id,
          month,
          top3_mean_consumption,
          COUNT(*) OVER (PARTITION BY id) AS n_obs,
          LAG(top3_mean_consumption) OVER (PARTITION BY id ORDER BY month) AS lag_val
        FROM base
      )
      SELECT
        id,
        AVG(top3_mean_consumption) AS peak_mean,
        STDDEV_SAMP(top3_mean_consumption) AS peak_sd,
        AVG(ABS(top3_mean_consumption - lag_val)) AS peak_volatility
      FROM ranked
      WHERE n_obs >= ", lookback_months, "
      GROUP BY id
    ")

    return(sql(qry))
  }
}

# ---------------------------------------------------------
# Build one treated profile
# ---------------------------------------------------------
build_treated_profile <- function(
    view_name,
    treated_id,
    Ti,
    lookback_months,
    feature_mode
) {
  id_condition <- paste0("id = ", sql_quote(treated_id))
  sdf <- build_profile_sql(
    view_name = view_name,
    Ti = Ti,
    lookback_months = lookback_months,
    feature_mode = feature_mode,
    id_condition = id_condition
  )
  out <- collect(sdf)
  if (nrow(out) == 0) return(NULL)
  out
}

# ---------------------------------------------------------
# Build control profiles at Ti
# controls: id != treated_id AND (adoption_month IS NULL OR adoption_month > Ti)
# ---------------------------------------------------------
build_control_profiles <- function(
    view_name,
    treated_id,
    Ti,
    lookback_months,
    feature_mode
) {
  Ti_chr <- as.character(Ti)

  id_condition <- paste0(
    "id <> ", sql_quote(treated_id),
    " AND (adoption_month IS NULL OR adoption_month > DATE(", sql_quote(Ti_chr), "))"
  )

  sdf <- build_profile_sql(
    view_name = view_name,
    Ti = Ti,
    lookback_months = lookback_months,
    feature_mode = feature_mode,
    id_condition = id_condition
  )

  out <- collect(sdf)
  if (nrow(out) == 0) return(NULL)
  out
}

# =========================================================
# 1) Risk-set matching
# =========================================================
risk_set_matching_peak <- function(
    data,
    id_col = "aID",
    month_col = "TIDPUNKT",
    adoption_col = "tariff_start",
    lookback_months = 12,
    k_neighbors = 5,
    price_filter = "all",
    feature_mode = "time_series"
) {

  if (!feature_mode %in% c("time_series", "summary")) {
    stop("feature_mode must be 'time_series' or 'summary'")
  }

  prepare_data(
    data = data,
    id_col = id_col,
    month_col = month_col,
    adoption_col = adoption_col,
    price_filter = price_filter,
    view_name = "monthly_agg_prepared"
  )

  adopters_sdf <- sql("
    SELECT DISTINCT id, adoption_month
    FROM monthly_agg_prepared
    WHERE adoption_month IS NOT NULL
    ORDER BY id, adoption_month
  ")

  adopters <- collect(adopters_sdf)

  if (nrow(adopters) == 0) {
    return(data.frame())
  }

  results_list <- vector("list", nrow(adopters))

  for (i in seq_len(nrow(adopters))) {
    treated_id <- adopters$id[i]
    Ti <- adopters$adoption_month[i]

    treated_profile <- build_treated_profile(
      view_name = "monthly_agg_prepared",
      treated_id = treated_id,
      Ti = Ti,
      lookback_months = lookback_months,
      feature_mode = feature_mode
    )

    if (is.null(treated_profile)) {
      results_list[[i]] <- NULL
      next
    }

    control_profiles <- build_control_profiles(
      view_name = "monthly_agg_prepared",
      treated_id = treated_id,
      Ti = Ti,
      lookback_months = lookback_months,
      feature_mode = feature_mode
    )

    if (is.null(control_profiles) || nrow(control_profiles) == 0) {
      results_list[[i]] <- NULL
      next
    }

    feature_cols <- setdiff(colnames(control_profiles), "id")

    X_control <- as.matrix(control_profiles[, feature_cols, drop = FALSE])
    X_treated <- as.numeric(treated_profile[1, feature_cols, drop = TRUE])

    # scale using control distribution, same as original logic
    center_vals <- colMeans(X_control, na.rm = TRUE)
    scale_vals <- apply(X_control, 2, sd, na.rm = TRUE)
    scale_vals[is.na(scale_vals) | scale_vals == 0] <- 1

    X_control_scaled <- scale(X_control, center = center_vals, scale = scale_vals)
    X_treated_scaled <- scale(matrix(X_treated, nrow = 1), center = center_vals, scale = scale_vals)

    # Euclidean distance
    dists <- sqrt(rowSums((X_control_scaled - matrix(
      X_treated_scaled[1, ],
      nrow = nrow(X_control_scaled),
      ncol = ncol(X_control_scaled),
      byrow = TRUE
    ))^2))

    k_use <- min(k_neighbors, length(dists))
    if (k_use == 0) {
      results_list[[i]] <- NULL
      next
    }

    ord <- order(dists)[seq_len(k_use)]
    matched <- control_profiles[ord, , drop = FALSE]

    matched$control_id <- matched$id
    matched$treated_id <- treated_id
    matched$adoption_month <- Ti
    matched$distance <- dists[ord]

    results_list[[i]] <- matched[, c(
      "treated_id", "control_id", "adoption_month", "distance", feature_cols
    ), drop = FALSE]
  }

  results <- do.call(rbind, results_list)
  if (is.null(results)) results <- data.frame()
  rownames(results) <- NULL
  results
}

# =========================================================
# 2) Build profile dataset for balance diagnostics
# =========================================================
build_profiles <- function(
    data,
    id_col = "aID",
    month_col = "TIDPUNKT",
    adoption_col = "tariff_start",
    lookback_months = 12,
    price_filter = "all",
    feature_mode = "time_series"
) {

  if (!feature_mode %in% c("time_series", "summary")) {
    stop("feature_mode must be 'time_series' or 'summary'")
  }

  prepare_data(
    data = data,
    id_col = id_col,
    month_col = month_col,
    adoption_col = adoption_col,
    price_filter = price_filter,
    view_name = "monthly_agg_prepared_profiles"
  )

  adopters_sdf <- sql("
    SELECT DISTINCT id, adoption_month
    FROM monthly_agg_prepared_profiles
    WHERE adoption_month IS NOT NULL
    ORDER BY id, adoption_month
  ")

  adopters <- collect(adopters_sdf)

  if (nrow(adopters) == 0) {
    return(data.frame())
  }

  out_list <- vector("list", nrow(adopters))

  for (i in seq_len(nrow(adopters))) {
    uid <- adopters$id[i]
    Ti <- adopters$adoption_month[i]

    prof <- build_treated_profile(
      view_name = "monthly_agg_prepared_profiles",
      treated_id = uid,
      Ti = Ti,
      lookback_months = lookback_months,
      feature_mode = feature_mode
    )

    if (is.null(prof)) {
      out_list[[i]] <- NULL
      next
    }

    prof$adoption_month <- Ti
    out_list[[i]] <- prof
  }

  profiles <- do.call(rbind, out_list)
  if (is.null(profiles)) profiles <- data.frame()
  rownames(profiles) <- NULL
  profiles
}

# =========================================================
# 3) Balance table
# =========================================================
balance_table <- function(profiles, matches) {

  if (nrow(matches) == 0) {
    stop("matches is empty")
  }

  treated_ids <- unique(matches$treated_id)

  treated <- profiles[profiles$id %in% treated_ids, , drop = FALSE]

  # 用 matches 保留 control 重複次數
  control_ids <- matches$control_id
  control <- merge(
    data.frame(id = control_ids, stringsAsFactors = FALSE),
    profiles,
    by = "id",
    all.x = TRUE,
    sort = FALSE
  )

  covariates <- setdiff(colnames(profiles), c("id", "adoption_month"))

  smd <- function(x, y) {
    (mean(x, na.rm = TRUE) - mean(y, na.rm = TRUE)) /
      sqrt((var(x, na.rm = TRUE) + var(y, na.rm = TRUE)) / 2)
  }

  balance_list <- lapply(covariates, function(v) {
    data.frame(
      covariate = v,
      treated_mean = mean(treated[[v]], na.rm = TRUE),
      control_mean = mean(control[[v]], na.rm = TRUE),
      SMD = smd(treated[[v]], control[[v]]),
      stringsAsFactors = FALSE
    )
  })

  balance <- do.call(rbind, balance_list)
  rownames(balance) <- NULL
  balance
}

# =========================================================
# 4) Love plot
# =========================================================
love_plot <- function(balance, title = "Covariate Balance") {
  ggplot(balance, aes(x = abs(SMD), y = reorder(covariate, abs(SMD)))) +
    geom_point(size = 3, color = "steelblue") +
    geom_vline(xintercept = 0.1, linetype = "dashed", color = "red") +
    labs(
      title = title,
      x = "|Standardized Mean Difference|",
      y = "Covariate"
    ) +
    theme_minimal()
}

# =========================================================
# 5) Run everything
# =========================================================

# Read parquet from Spark
df <- read.df("output/data/monthly_agg.parquet", source = "parquet")

# -------------------
# time_series mode
# -------------------
matches_ts <- risk_set_matching_peak(
  data = df,
  feature_mode = "time_series",
  lookback_months = 12,
  k_neighbors = 5
)

profiles_ts <- build_profiles(
  data = df,
  feature_mode = "time_series",
  lookback_months = 12
)

balance_ts <- balance_table(
  profiles = profiles_ts,
  matches = matches_ts
)

print(balance_ts)
print(love_plot(balance_ts, title = "Love Plot - Time Series"))

# -------------------
# summary mode
# -------------------
matches_summary <- risk_set_matching_peak(
  data = df,
  feature_mode = "summary",
  lookback_months = 12,
  k_neighbors = 5
)

profiles_summary <- build_profiles(
  data = df,
  feature_mode = "summary",
  lookback_months = 12
)

balance_summary <- balance_table(
  profiles = profiles_summary,
  matches = matches_summary
)

print(balance_summary)
print(love_plot(balance_summary, title = "Love Plot - Summary Statistics"))

# =========================================================
# Output folders
# =========================================================
dirs <- c(
  "output",
  "output/matching",
  "output/diagnostics",
  "output/figures"
)

create_dirs(dirs)

# =========================================================
# Save matching parquet
# =========================================================
matches_ts_sdf <- createDataFrame(matches_ts)
matches_summary_sdf <- createDataFrame(matches_summary)

write.df(
  matches_ts_sdf,
  path = "output/matching/matches_ts.parquet",
  source = "parquet",
  mode = "overwrite"
)

write.df(
  matches_summary_sdf,
  path = "output/matching/matches_summary.parquet",
  source = "parquet",
  mode = "overwrite"
)

# =========================================================
# Save balance table CSV
# =========================================================
write.csv(
  balance_ts,
  "output/diagnostics/balance_ts.csv",
  row.names = FALSE
)

write.csv(
  balance_summary,
  "output/diagnostics/balance_summary.csv",
  row.names = FALSE
)

# =========================================================
# Save love plots
# =========================================================
ggsave(
  "output/figures/loveplot_ts.png",
  plot = love_plot(balance_ts, "Love Plot - Time Series"),
  width = 7,
  height = 5,
  dpi = 300
)

ggsave(
  "output/figures/loveplot_summary.png",
  plot = love_plot(balance_summary, "Love Plot - Summary Statistics"),
  width = 7,
  height = 5,
  dpi = 300
)