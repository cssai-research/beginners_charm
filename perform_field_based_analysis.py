import gc
import time
import pandas as pd
import numpy as np
import os
import warnings
import matplotlib.pyplot as plt
import seaborn as sns
import seaborn as sns
from scipy.stats import pearsonr, spearmanr, kendalltau
from functools import wraps


warnings.filterwarnings("ignore", category=RuntimeWarning)

# Set pandas options for memory efficiency
pd.options.mode.chained_assignment = None
pd.set_option("mode.copy_on_write", True)

START_YEAR = 1941


def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()

        elapsed_time = end_time - start_time

        # Format the time appropriately
        if elapsed_time < 0.001:
            time_str = f"{elapsed_time * 1_000_000:.2f} microseconds"
        elif elapsed_time < 1:
            time_str = f"{elapsed_time * 1000:.2f} milliseconds"
        elif elapsed_time < 60:
            time_str = f"{elapsed_time:.2f} seconds"
        else:
            minutes = int(elapsed_time // 60)
            seconds = elapsed_time % 60
            time_str = f"{minutes} min {seconds:.2f} sec"

        print(f"%% {func.__name__}() took {time_str}")

        return result

    return wrapper


@timer
def load_full_disruption_data(
    filepath="data/disruption_analysis.csv", chunksize=1000000
):
    print(f"Reading CSV in chunks from: {filepath}")

    required_cols = [
        "doctype",
        "team_size",
        "year",
        "first_time_author_ratio",
        "avg_career_age",
        "senior_author_avg_disruption",
        "mid_career_author_ratio",
        "early_author_avg_disruption",
        "early_career_author_ratio",
        "mid_author_avg_disruption",
        "disruption",
        "citation_count",
        "C10",
        "avg_disruption",
        "avg_citation_count",
        "Atyp_Median_Z",
        "avg_reference_age",
        "median_reference_age",
        "avg_reference_popularity",
        "median_reference_popularity",
        "senior_author_ratio",
        "level_0_field_names",
        "level_1_field_names",
        "first_time_author_count",
        "early_career_author_count",
        "senior_author_count",
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

    # In the preperation script early_career = 1-5 and mid_career = 6-10
    # In analysis we will use early_career = 1-10
    df["early_career_author_ratio"] = (
        df["early_career_author_ratio"] + df["mid_career_author_ratio"]
    )

    df["early_author_avg_disruption"] = np.where(
        df["early_author_avg_disruption"].isna()
        | df["mid_author_avg_disruption"].isna(),
        np.nan,
        (df["early_author_avg_disruption"] + df["mid_author_avg_disruption"]) / 2,
    )

    print(f"Starting year filtering... Keeping articles from {START_YEAR}")
    df = df[df["year"] >= START_YEAR]

    df.drop(
        columns=[
            "decade_start",
            "decade_end",
            "mid_career_author_ratio",
            "mid_author_avg_disruption",
        ],
        inplace=True,
    )

    # --- Percentiles ---
    for col in [
        "disruption",
        "citation_count",
        "C10",
        "avg_disruption",
        "avg_citation_count",
        "Atyp_Median_Z",
        "avg_reference_age",
        "median_reference_age",
        "avg_reference_popularity",
        "median_reference_popularity",
    ]:
        df[f"{col}_percentile"] = df[col].rank(pct=True) * 100

    # --- Derived Columns ---
    print("Creating derived columns...")

    def percentile_group(x):
        if pd.isna(x):
            return None
        if x < 60:
            return "0-60 percentile"
        elif x < 70:
            return "60-70 percentile"
        elif x < 80:
            return "70-80 percentile"
        elif x < 90:
            return "80-90 percentile"
        else:
            return "90-100 percentile"

    df["co_authors_disruption_group"] = df["avg_disruption_percentile"].apply(
        percentile_group
    )
    df["co_authors_citation_group"] = df["avg_citation_count_percentile"].apply(
        percentile_group
    )

    avg_disruption_sorted = df["avg_disruption"].sort_values().values

    def find_percentile_in_reference(value, reference_array):
        if pd.isna(value):
            return np.nan
        return (
            np.searchsorted(reference_array, value, side="right")
            / len(reference_array)
            * 100
        )

    df["senior_author_disruption_percentile"] = df[
        "senior_author_avg_disruption"
    ].apply(lambda x: find_percentile_in_reference(x, avg_disruption_sorted))
    df["early_career_disruption_percentile"] = df["early_author_avg_disruption"].apply(
        lambda x: find_percentile_in_reference(x, avg_disruption_sorted)
    )

    df["senior_author_disruption_bucket"] = df[
        "senior_author_disruption_percentile"
    ].apply(percentile_group)
    df["early_career_disruption_bucket"] = df[
        "early_career_disruption_percentile"
    ].apply(percentile_group)

    gc.collect()

    print(f"Final dataframe ready. Shape: {df.shape}")
    print("Total Number of articles:", f"{len(df):,}")
    print("=" * 80)
    return df


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
    filepath = os.path.join("Final_Figures", filename)
    results_df.to_csv(filepath, index=False)
    print(f"Results saved to: {filepath}")

    return results_df


def setup_plotting_style_mahdee():
    plt.figure(figsize=(8, 6), dpi=300)

    sns.set_style("white")

    plt.rcParams["font.size"] = 12
    plt.rcParams["axes.labelsize"] = 14
    plt.rcParams["axes.titlesize"] = 16
    plt.rcParams["xtick.labelsize"] = 12
    plt.rcParams["ytick.labelsize"] = 12
    plt.rcParams["legend.fontsize"] = 12
    plt.rcParams["figure.titlesize"] = 18

    plt.rcParams["axes.linewidth"] = 1.5
    plt.rcParams["grid.linewidth"] = 0.8
    plt.rcParams["lines.linewidth"] = 2.0

    sns.set_palette("colorblind")

    # Save format settings
    plt.rcParams["savefig.format"] = "pdf"
    plt.rcParams["savefig.bbox"] = "tight"
    plt.rcParams["savefig.pad_inches"] = 0.1


def setup_plotting_style():
    sns.set(context="talk", style="whitegrid")


@timer
def plot_firsttime_authors_by_field_grid(
    df,
    target_column="disruption_percentile",
    field_column="field_name",
    author_ratio_column="first_time_author_ratio",
    min_group_size=5,
    min_sample_threshold=50,
    n_bins=10,
    ci=95,
    ylim=(10, 90),
    show_ci=True,
    binning_method="equal",
    ci_method="sem",
    n_bootstrap=1000,
    team_sizes=None,
    fields_to_plot=None,
    save_path=None,
):
    """
    Create a 3x4 grid of subplots, each showing the relationship between First-Time Author Ratio
    and disruption score across different fields for different team sizes.
    """
    setup_plotting_style()
    gc.collect()

    fig, axes = plt.subplots(2, 4, figsize=(24, 12), sharex=True, sharey=True, dpi=300)
    axes_flat = axes.flatten()

    # Get fields to plot
    if fields_to_plot is None:
        fields = sorted(df[field_column].unique())
    else:
        fields = [
            field for field in fields_to_plot if field in df[field_column].unique()
        ]

    # Check if any valid fields remain
    if not fields:
        raise ValueError(
            f"None of the specified fields found in the data. Available fields: {sorted(df[field_column].unique())}"
        )

    # Use Tab10 color palette for fields
    colors = sns.color_palette("tab10", n_colors=len(fields))
    field_colors = {field: colors[i] for i, field in enumerate(fields)}

    markers = {field: "o" for field in fields}

    if team_sizes is None:
        team_sizes = range(1, 9)

    needed_columns = [author_ratio_column, target_column, "team_size", field_column]
    df_slim = df[needed_columns].copy()
    gc.collect()

    for idx, team_size in enumerate(team_sizes):
        ax = axes_flat[idx]
        subset = df_slim[df_slim["team_size"] == team_size].copy()

        if len(subset) == 0:
            del subset
            gc.collect()
            continue

        for field in fields:
            field_subset = subset[subset[field_column] == field].copy()

            if len(field_subset) == 0:
                del field_subset
                gc.collect()
                continue

            results = []

            zero_mask = field_subset[author_ratio_column] == 0
            zero_values = field_subset.loc[zero_mask, target_column]
            if len(zero_values) >= min_group_size:
                zero_median = zero_values.median()
                zero_std = zero_values.std()
                zero_sem = zero_std / np.sqrt(len(zero_values))

                if ci_method == "bootstrap":
                    boot = np.random.choice(
                        zero_values, (n_bootstrap, len(zero_values))
                    )
                    boot_meds = np.median(boot, axis=1)
                    lower = np.percentile(boot_meds, (100 - ci) / 2)
                    upper = np.percentile(boot_meds, 100 - (100 - ci) / 2)
                    del boot, boot_meds
                else:
                    z = 1.96
                    lower = zero_median - z * zero_sem
                    upper = zero_median + z * zero_sem

                results.append(
                    {
                        "x": 0,
                        "median": zero_median,
                        "count": len(zero_values),
                        "median_ci_lower": lower,
                        "median_ci_upper": upper,
                    }
                )

            del zero_mask, zero_values
            gc.collect()

            values = field_subset[field_subset[author_ratio_column] > 0][
                [author_ratio_column, target_column]
            ].dropna()
            if len(values) == 0:
                del values, field_subset
                gc.collect()
                continue

            # Binning
            if binning_method == "equal":
                bin_edges = np.linspace(0, 1, n_bins + 1)
                values["bin"] = pd.cut(
                    values[author_ratio_column], bins=bin_edges, include_lowest=True
                )
                del bin_edges
            else:
                values["bin"] = pd.qcut(
                    values[author_ratio_column], q=n_bins, duplicates="drop"
                )

            grouped = values.groupby("bin", observed=False)

            for bin_interval, group in grouped:
                if len(group) < min_group_size:
                    continue

                median = group[target_column].median()
                std = group[target_column].std()
                sem = std / np.sqrt(len(group))

                if ci_method == "bootstrap":
                    boot = np.random.choice(
                        group[target_column], (n_bootstrap, len(group))
                    )
                    boot_meds = np.median(boot, axis=1)
                    lower = np.percentile(boot_meds, (100 - ci) / 2)
                    upper = np.percentile(boot_meds, 100 - (100 - ci) / 2)
                    del boot, boot_meds
                else:
                    z = 1.96
                    lower = median - z * sem
                    upper = median + z * sem

                # Compute bin midpoint
                if binning_method == "equal":
                    midpoint = bin_interval.mid
                else:
                    midpoint = group[author_ratio_column].median()

                results.append(
                    {
                        "x": midpoint,
                        "median": median,
                        "count": len(group),
                        "median_ci_lower": lower,
                        "median_ci_upper": upper,
                    }
                )

            del values, grouped
            gc.collect()

            if not results:
                del field_subset
                gc.collect()
                continue

            group_stats = pd.DataFrame(results)

            # Apply minimum sample threshold check
            filtered_stats = group_stats[group_stats["count"] >= min_sample_threshold]
            del group_stats

            if filtered_stats.empty:
                del filtered_stats, field_subset
                gc.collect()
                continue

            # Plot the line
            sns.lineplot(
                x="x",
                y="median",
                data=filtered_stats,
                color=field_colors[field],
                marker=markers[field],
                markersize=10,
                linewidth=2.5,
                label=field if idx == 0 else None,  # Only add labels on first plot
                ax=ax,
            )

            if show_ci:
                ax.fill_between(
                    filtered_stats["x"],
                    filtered_stats["median_ci_lower"],
                    filtered_stats["median_ci_upper"],
                    alpha=0.1,
                    color=field_colors[field],
                )

            del filtered_stats, field_subset
            gc.collect()

        # Customize subplot
        ax.set_title(f"Team Size: {team_size}", fontweight="bold", fontsize=14)
        ax.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1.0])
        ax.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1"])

        # Remove x and y axis labels from individual subplots
        ax.set_xlabel("")
        ax.set_ylabel("")

        if ylim:
            ax.set_ylim(ylim)

        sns.despine(ax=ax, top=True, right=True)

        del subset
        gc.collect()

    del df_slim
    gc.collect()

    # Add a single legend for the entire figure
    handles, labels_list = axes_flat[0].get_legend_handles_labels()
    legend = fig.legend(
        handles,
        labels_list,
        loc="lower center",
        bbox_to_anchor=(0.5, 0.02),
        ncol=min(len(fields), 4),  # Limit to 4 columns for better readability
        frameon=True,
        fontsize=14,
    )

    # Remove individual legends
    for ax in axes_flat:
        if ax.get_legend() is not None:
            ax.get_legend().remove()

    # Handle empty subplots if less than 8 team sizes
    for i in range(len(team_sizes), len(axes_flat)):
        axes_flat[i].set_visible(False)

    # Add common axis labels
    fig.text(
        0.5,
        0.001,
        "Beginner Author Ratio",
        ha="center",
        fontsize=18,
        fontweight="bold",
    )

    ylabel_map = {
        "disruption_percentile": "Disruption Percentile",
        "C10": "C10",
        "c10_percentile": "C_10 Percentile",
    }
    fig.text(
        0.005,
        0.5,
        ylabel_map.get(target_column, target_column),
        va="center",
        rotation="vertical",
        fontsize=18,
        fontweight="bold",
    )

    # Add title
    fig.suptitle(
        f"Impact of Beginner Author Ratio on {ylabel_map.get(target_column, target_column)} Across Fields (Level 0)",
        fontsize=24,
        fontweight="bold",
        y=0.98,
    )

    # Adjust layout
    plt.tight_layout(rect=[0.04, 0.08, 1, 0.95])

    # Save figure if path is provided
    if save_path:
        plt.savefig(save_path, bbox_inches="tight", dpi=300)

    plt.close(fig)
    del axes_flat, colors, field_colors, markers, handles, labels_list
    gc.collect()
    return fig, axes


def main():
    if not os.path.exists("Final_Figures"):
        os.makedirs("Final_Figures")

    setup_plotting_style()

    final_df = load_full_disruption_data()

    # Drop unnecessary columns to save memory
    print("\n=== Dropping unnecessary columns to save memory ===")
    columns_to_keep = [
        "team_size",
        "first_time_author_ratio",
        "disruption_percentile",
        "level_0_field_names",
        "level_1_field_names",
    ]

    columns_to_drop = [col for col in final_df.columns if col not in columns_to_keep]
    final_df.drop(columns=columns_to_drop, inplace=True)
    print(f"Dropped {len(columns_to_drop)} unnecessary columns")
    print(f"Dataframe shape after dropping: {final_df.shape}")
    gc.collect()

    # ============== LEVEL 0 FIELD ANALYSIS ==============
    print("\n" + "=" * 80)
    print("=== LEVEL 0 FIELD ANALYSIS ===")
    print("=" * 80)

    # Convert level_0_field_names to list and explode
    print("Converting level_0_field_names to lists...")
    final_df["level_0_field_names"] = final_df["level_0_field_names"].apply(
        lambda x: x.split("|") if isinstance(x, str) and x else []
    )

    print("Creating Level 0 dataframe...")
    df_level_0 = final_df[final_df["level_0_field_names"].apply(len) > 0].copy()
    df_level_0 = df_level_0.explode("level_0_field_names").reset_index(drop=True)
    df_level_0.rename(columns={"level_0_field_names": "field_name"}, inplace=True)
    df_level_0 = df_level_0[df_level_0["field_name"].notna()].copy()

    # Drop level_1_field_names from level_0 dataframe
    df_level_0.drop(columns=["level_1_field_names"], inplace=True, errors="ignore")

    print(f"Level 0 dataframe shape: {df_level_0.shape}")
    print(f"Number of unique level 0 fields: {df_level_0['field_name'].nunique()}")
    print(f"Level 0 field value counts:\n{df_level_0['field_name'].value_counts()}\n")
    gc.collect()

    # Calculate correlation coefficients for Level 0 fields
    print("Calculating correlation coefficients for Level 0 fields...")
    out_df_level_0 = find_correlation_coefficient(
        df_level_0, "first_time_author_ratio", "disruption_percentile", "field_name"
    )
    print("Level 0 Correlations:")
    print(out_df_level_0)
    print()

    # Plot Level 0 fields
    print("Plotting Level 0 fields...")
    fig, axes = plot_firsttime_authors_by_field_grid(
        df=df_level_0,
        target_column="disruption_percentile",
        field_column="field_name",
        fields_to_plot=None,
        save_path="Final_Figures/Final_Disruption_by_First_Time_Author_Ratio_level_0_fields_1-8.pdf",
    )
    plt.close(fig)
    del fig, axes

    # Clean up Level 0 dataframe
    print("Cleaning up Level 0 data from memory...")
    del df_level_0, out_df_level_0
    gc.collect()

    # ============== LEVEL 1 FIELD ANALYSIS ==============
    print("\n" + "=" * 80)
    print("=== LEVEL 1 FIELD ANALYSIS ===")
    print("=" * 80)

    # Convert level_1_field_names to list and explode
    print("Converting level_1_field_names to lists...")
    final_df["level_1_field_names"] = final_df["level_1_field_names"].apply(
        lambda x: x.split("|") if isinstance(x, str) and x else []
    )

    # Drop level_0_field_names to save memory
    final_df.drop(columns=["level_0_field_names"], inplace=True, errors="ignore")
    gc.collect()

    print("Creating Level 1 dataframe...")
    df_level_1 = final_df[final_df["level_1_field_names"].apply(len) > 0].copy()
    df_level_1 = df_level_1.explode("level_1_field_names").reset_index(drop=True)
    df_level_1.rename(columns={"level_1_field_names": "field_name"}, inplace=True)
    df_level_1 = df_level_1[df_level_1["field_name"].notna()].copy()

    print(f"Level 1 dataframe shape: {df_level_1.shape}")
    print(f"Number of unique level 1 fields: {df_level_1['field_name'].nunique()}")
    print(f"Level 1 field value counts:\n{df_level_1['field_name'].value_counts()}\n")

    # Clean up final_df as we don't need it anymore
    del final_df
    gc.collect()

    # Calculate correlation coefficients for Level 1 fields
    print("Calculating correlation coefficients for Level 1 fields...")
    out_df_level_1 = find_correlation_coefficient(
        df_level_1, "first_time_author_ratio", "disruption_percentile", "field_name"
    )
    print("Level 1 Correlations:")
    print(out_df_level_1)
    print()

    # Clean up Level 1 dataframe
    print("Cleaning up Level 1 data from memory...")
    del df_level_1, out_df_level_1
    gc.collect()

    print("\n" + "=" * 80)
    print("=== Analysis Complete ===")
    print("=" * 80)


if __name__ == "__main__":
    main()
