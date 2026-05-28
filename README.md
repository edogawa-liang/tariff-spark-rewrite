# Shifting the Peak

*Household Responses to Power-Based Electricity Tariffs in Gothenburg*

## Overview

This project analyzes the impact of electricity tariffs using the Gothenburg electricity consumption dataset.

The analysis focuses on evaluating tariff effects on electricity consumption behavior.


## Environment

All notebooks are designed to run in:

- Microsoft Fabric
- Spark Notebook environment


## Main Workflow

Recommended execution order:

```text
pre_spark.ipynb
    Data preprocessing and preparation
    ↓
matching_analysis-high.ipynb
    Matching and cohort construction
    ↓
inference-high_pooled.ipynb
    Tariff effect estimation
    ↓
did_analysis-calendar2_1year-high.ipynb
    Difference-in-Differences analysis


EDA and validation notebooks:

- `eda_spark.ipynb`
  - EDA for the full dataset

- `eda_matching_calendar2_1year-high.ipynb`
  - EDA and validation for the matched dataset

```

## Result