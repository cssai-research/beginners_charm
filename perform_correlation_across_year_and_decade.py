import os
import gc
import warnings
import numpy as np
import pandas as pd
from scipy.stats import pearsonr, spearmanr, kendalltau

# warnings.filterwarnings("ignore", category=RuntimeWarning)


def find_correlation_coefficient(df, column_1, column_2, column_3=None):
    """
    Calculate correlation coefficients (Pearson, Spearman, Kendall) between two variables
    with optional grouping by a third variable, and save results to CSV.

    Parameters:
    -----------
    df : pandas.DataFrame
        The dataframe containing the data
    column_1 : str
        Name of the first variable
    column_2 : str
        Name of the second variable
    column_3 : str, optional
        Name of the grouping variable

    Returns:
    --------
    pandas.DataFrame
        A formatted table with correlation results
    """

    def calculate_correlations(data, col1, col2):
        """Helper function to calculate all three correlation types"""
        # Remove rows with NaN values in either column
        clean_data = data[[col1, col2]].dropna()

        if len(clean_data) < 2:
            return {
                "n": len(clean_data),
                "pearson_r": np.nan,
                "pearson_p": np.nan,
                "spearman_r": np.nan,
                "spearman_p": np.nan,
                "kendall_r": np.nan,
                "kendall_p": np.nan,
            }

        try:
            # Pearson correlation
            pearson_r, pearson_p = pearsonr(clean_data[col1], clean_data[col2])

            # Spearman correlation
            spearman_r, spearman_p = spearmanr(clean_data[col1], clean_data[col2])

            # Kendall correlation
            kendall_r, kendall_p = kendalltau(clean_data[col1], clean_data[col2])

            return {
                "n": len(clean_data),
                "pearson_r": pearson_r,
                "pearson_p": pearson_p,
                "spearman_r": spearman_r,
                "spearman_p": spearman_p,
                "kendall_r": kendall_r,
                "kendall_p": kendall_p,
            }
        except Exception as e:
            print(f"Error calculating correlations: {e}")
            return {
                "n": len(clean_data),
                "pearson_r": np.nan,
                "pearson_p": np.nan,
                "spearman_r": np.nan,
                "spearman_p": np.nan,
                "kendall_r": np.nan,
                "kendall_p": np.nan,
            }

    # Validate columns exist
    missing_cols = []
    for col in [column_1, column_2] + ([column_3] if column_3 else []):
        if col not in df.columns:
            missing_cols.append(col)

    if missing_cols:
        raise ValueError(f"Columns not found in dataframe: {missing_cols}")

    results = []

    # Calculate correlation for entire dataset (without grouping)
    overall_corr = calculate_correlations(df, column_1, column_2)
    results.append(
        {
            "Group": "All Data",
            "N": overall_corr["n"],
            "Pearson_r": overall_corr["pearson_r"],
            "Pearson_p": overall_corr["pearson_p"],
            "Spearman_r": overall_corr["spearman_r"],
            "Spearman_p": overall_corr["spearman_p"],
            "Kendall_r": overall_corr["kendall_r"],
            "Kendall_p": overall_corr["kendall_p"],
        }
    )

    # If grouping variable is provided, calculate correlations for each group
    if column_3:
        groups = df[column_3].dropna().unique()
        groups = sorted(groups)  # Sort for consistent output

        for group in groups:
            group_data = df[df[column_3] == group]
            group_corr = calculate_correlations(group_data, column_1, column_2)

            results.append(
                {
                    "Group": str(group),
                    "N": group_corr["n"],
                    "Pearson_r": group_corr["pearson_r"],
                    "Pearson_p": group_corr["pearson_p"],
                    "Spearman_r": group_corr["spearman_r"],
                    "Spearman_p": group_corr["spearman_p"],
                    "Kendall_r": group_corr["kendall_r"],
                    "Kendall_p": group_corr["kendall_p"],
                }
            )

    # Create results dataframe
    results_df = pd.DataFrame(results)

    # Format the results for better readability
    numeric_cols = [
        "Pearson_r",
        "Pearson_p",
        "Spearman_r",
        "Spearman_p",
        "Kendall_r",
        "Kendall_p",
    ]
    for col in numeric_cols:
        results_df[col] = results_df[col].round(8)

    # Create Final_Figures folder if it doesn't exist
    os.makedirs("Final_Figures", exist_ok=True)

    # Generate filename
    if column_3:
        filename = f"{column_1}_{column_2}_{column_3}_correlation.csv"
    else:
        filename = f"{column_1}_{column_2}_correlation.csv"

    # Save to CSV
    if not os.path.exists("Final_Figures/Full_Data"):
        os.makedirs("Final_Figures/Full_Data")

    filepath = os.path.join("Final_Figures/Full_Data", filename)
    results_df.to_csv(filepath, index=False)
    print(f"Results saved to: {filepath}")

    return results_df


def perform_yearly_disruption_correlations_on_full_data(
    filepath="data/disruption_analysis.csv", chunksize=1000000
):
    """This function finds correlation coefficient of beginner_author_ratio, disruption_percentile, year on all data"""
    print(f"Reading CSV in chunks from: {filepath}")
    required_cols = [
        "doctype",
        "team_size",
        "year",
        "first_time_author_ratio",
        "avg_career_age",
        "senior_author_ratio",
        "senior_author_avg_disruption",
        "mid_career_author_ratio",
        "mid_author_avg_disruption",
        "early_author_avg_disruption",
        "early_career_author_ratio",
        "disruption",
    ]
    chunks = []
    for i, chunk in enumerate(
        pd.read_csv(filepath, usecols=required_cols, chunksize=chunksize)
    ):
        chunks.append(chunk)
        if (i + 1) % 100 == 0:
            print(f"  Processed {(i + 1) * chunksize:,} rows...")
        gc.collect()

    print(f"Concatenating {len(chunks)} chunks...")
    df = pd.concat(chunks, ignore_index=True)
    del chunks
    gc.collect()

    print(f"Loaded dataframe with shape: {df.shape}")

    # --- Preprocessing ---
    df = df[df.doctype == "article"]
    df = df[df["team_size"] <= 40]
    df["year"] = df["year"].astype(int)
    df["Disruption"] = df["disruption"].astype(float)
    df["decade_start"] = ((df["year"] - 1) // 10) * 10 + 1
    df["decade_end"] = df["decade_start"] + 9
    df["decade"] = df["decade_start"].astype(str) + "-" + df["decade_end"].astype(str)

    # Removing problematic data (if any)
    df = df[
        ~((df["first_time_author_ratio"] == 1) & (df["avg_career_age"] > 0))
        & ~(
            (df["first_time_author_ratio"] == 1)
            & (df["senior_author_avg_disruption"] > 0)
        )
        & ~((df["first_time_author_ratio"] == 1) & (df["senior_author_ratio"] > 0))
        & ~((df["first_time_author_ratio"] == 1) & (df["mid_career_author_ratio"] > 0))
        & ~(
            (df["first_time_author_ratio"] == 1) & (df["mid_author_avg_disruption"] > 0)
        )
        & ~(
            (df["first_time_author_ratio"] == 1) & (df["early_career_author_ratio"] > 0)
        )
        & ~(
            (df["first_time_author_ratio"] == 1)
            & (df["early_author_avg_disruption"] > 0)
        )
    ]

    df[f"disruption_percentile"] = df["disruption"].rank(pct=True) * 100
    df = df[["year", "decade", "disruption_percentile", "first_time_author_ratio"]]

    print("Number of Papers by Decade:")
    print(df["decade"].value_counts().sort_index())

    # find disruption percentile

    # print("Correlation Coefficient Between Beginner Author Ratio and Disruption Percentile by Year")
    # print(find_correlation_coefficient(df, "first_time_author_ratio", "disruption_percentile", "year"))

    # print("Correlation Coefficient Between Beginner Author Ratio and Disruption Percentile by Decade")
    # print(find_correlation_coefficient(df, "first_time_author_ratio", "disruption_percentile", "decade"))

    print("Debugging 1940")
    year_1940 = df[df["year"] == 1940]
    print(
        f"Unique first_time_author_ratio values: {year_1940['first_time_author_ratio'].nunique()}"
    )
    print(
        f"Unique first_time_author_ratio values: {year_1940['first_time_author_ratio'].value_counts()}"
    )
    print(
        f"Unique disruption_percentile values: {year_1940['disruption_percentile'].nunique()}"
    )


if __name__ == "__main__":
    perform_yearly_disruption_correlations_on_full_data()


# Number of Papers in Different Year Ranges:
# 1931 - 2020: 29,056,318
# 1941 - 2020: 29,054,261
# 1951 - 2020: 29,007,048
# 1961 - 2020: 28,771,295
# 1971 - 2020: 28,040,628
