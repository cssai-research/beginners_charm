import gc
import time
import pandas as pd
import numpy as np
import os
import glob
import json
import warnings
import matplotlib.pyplot as plt
import seaborn as sns
import seaborn as sns
from matplotlib.colors import Normalize
from scipy.stats import pearsonr, spearmanr, kendalltau
from pathlib import Path
from itertools import combinations
from scipy.stats import ks_2samp
from math import atanh, isfinite, erfc, sqrt
from textwrap import dedent
from typing import Union, Optional
import matplotlib.colors as mcolors
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
        & ~((df["first_time_author_ratio"] == 1) & (df["senior_author_avg_disruption"] > 0))
        & ~((df["first_time_author_ratio"] == 1) & (df["senior_author_ratio"] > 0))
        & ~((df["first_time_author_ratio"] == 1) & (df["mid_career_author_ratio"] > 0))
        & ~((df["first_time_author_ratio"] == 1) & (df["mid_author_avg_disruption"] > 0))
        & ~((df["first_time_author_ratio"] == 1) & (df["early_career_author_ratio"] > 0))
        & ~((df["first_time_author_ratio"] == 1) & (df["early_author_avg_disruption"] > 0))
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
def plot_team_size_by_career_ratios_grid(
    df,
    target_column="disruption_percentile",
    career_columns=None,
    min_group_size=5,
    min_sample_threshold=50,
    n_bins=10,
    ci=95,
    ylim=(30, 80),
    show_ci=True,
    binning_method="equal",
    ci_method="sem",
    n_bootstrap=1000,
    team_sizes=None,
    save_path=None,
):
    """
    Create a 2x4 grid of subplots, each showing the relationship between different career ratio variables
    and disruption score with confidence intervals for different team sizes.
    """
    setup_plotting_style()
    gc.collect()

    if career_columns is None:
        career_columns = [
            "first_time_author_ratio",
            "early_career_author_ratio",
            "senior_author_ratio",
        ]

    # Create a 2x4 grid of subplots instead of 4x4
    fig, axes = plt.subplots(2, 4, figsize=(24, 12), sharex=True, sharey=True, dpi=300)
    axes_flat = axes.flatten()

    # Define color palette and markers
    full_palette = sns.color_palette("viridis_r", 8)
    color_indices = [0, 1, 5]
    selected_colors = [full_palette[i] for i in color_indices]
    colors = {col: selected_colors[i] for i, col in enumerate(career_columns)}

    markers = {
        "first_time_author_ratio": "o",
        "early_career_author_ratio": "s",
        "senior_author_ratio": "d",
    }

    labels = {
        "first_time_author_ratio": "Beginner Authors",
        "early_career_author_ratio": "Early-Career Authors",
        "senior_author_ratio": "Senior Authors",
    }

    # Create plots for each team size - modified for 2x4 grid (8 total plots)
    if team_sizes is None:
        team_sizes = list(range(1, 8)) + ["8+"]  # 1-7 individual, then "8+"

    needed_columns = career_columns + [target_column, "team_size"]
    df_slim = df[needed_columns].copy()

    for idx, team_size in enumerate(team_sizes):
        ax = axes_flat[idx]

        # Handle the special case for "8+" (last cell)
        if team_size == "8+":
            subset = df_slim[df_slim["team_size"] >= 8].copy()
            title_text = "Team Size: 8+"
        else:
            subset = df_slim[df_slim["team_size"] == team_size].copy()
            title_text = f"Team Size: {team_size}"

        if len(subset) == 0:
            continue

        for group_column in career_columns:
            results = []

            # Handle zero values separately
            zero_mask = subset[group_column] == 0
            zero_values = subset.loc[zero_mask, target_column]
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

            values = subset[subset[group_column] > 0][
                [group_column, target_column]
            ].dropna()
            if len(values) == 0:
                continue

            # Binning
            if binning_method == "equal":
                bin_edges = np.linspace(0, 1, n_bins + 1)
                values["bin"] = pd.cut(
                    values[group_column], bins=bin_edges, include_lowest=True
                )
            else:
                values["bin"] = pd.qcut(
                    values[group_column], q=n_bins, duplicates="drop"
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
                else:
                    z = 1.96
                    lower = median - z * sem
                    upper = median + z * sem

                # Compute bin midpoint
                if binning_method == "equal":
                    midpoint = bin_interval.mid
                else:
                    midpoint = group[group_column].median()

                results.append(
                    {
                        "x": midpoint,
                        "median": median,
                        "count": len(group),
                        "median_ci_lower": lower,
                        "median_ci_upper": upper,
                    }
                )

            group_stats = pd.DataFrame(results)

            # Apply minimum sample threshold check
            filtered_stats = group_stats[group_stats["count"] >= min_sample_threshold]
            if filtered_stats.empty:
                continue

            # Plot the line
            sns.lineplot(
                x="x",
                y="median",
                data=filtered_stats,
                color=colors[group_column],
                marker=markers[group_column],
                markersize=10,  # Increased from 6 to 10
                linewidth=2.5,  # Increased from 1.5 to 2.5
                label=(
                    labels[group_column] if idx == 0 else None
                ),  # Only add labels on first plot
                ax=ax,
            )

            if show_ci:
                ax.fill_between(
                    filtered_stats["x"],
                    filtered_stats["median_ci_lower"],
                    filtered_stats["median_ci_upper"],
                    alpha=0.1,
                    color=colors[group_column],
                )

            gc.collect()

        # Customize subplot
        ax.set_title(title_text, fontweight="bold", fontsize=14)
        ax.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1.0])
        ax.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1"])

        # Remove x and y axis labels from individual subplots
        ax.set_xlabel("")
        ax.set_ylabel("")

        if ylim:
            ax.set_ylim(ylim)

        sns.despine(ax=ax, top=True, right=True)

        # Clear memory
        del subset
        gc.collect()

    # Add a single legend for the entire figure
    handles, labels_list = axes_flat[0].get_legend_handles_labels()
    legend = fig.legend(
        handles,
        labels_list,
        loc="lower center",
        bbox_to_anchor=(0.5, 0.02),
        ncol=4,
        frameon=True,
        fontsize=14,
    )

    # Remove individual legends
    for ax in axes_flat:
        if ax.get_legend() is not None:
            ax.get_legend().remove()

    # No need to hide subplots since we're using exactly 8 (2x4)

    # Add common axis labels
    fig.text(0.5, 0.01, "Author Ratio", ha="center", fontsize=18, fontweight="bold")

    ylabel_map = {
        "disruption_percentile": "Disruption Percentile",
        "C10": "C10",
        "c10_percentile": "C_10 Percentile",
    }
    fig.text(
        0.01,
        0.5,
        ylabel_map.get(target_column, target_column),
        va="center",
        rotation="vertical",
        fontsize=18,
        fontweight="bold",
    )

    # Add title
    fig.suptitle(
        f"Impact of Author Career Stage on {ylabel_map.get(target_column, target_column)}",
        fontsize=24,
        fontweight="bold",
        y=0.98,
    )

    # Adjust layout
    plt.tight_layout(rect=[0.03, 0.05, 1, 0.95])

    # Save figure if path is provided
    if save_path:
        plt.savefig(save_path, bbox_inches="tight", dpi=300)

    # Close figure and clean up
    plt.close(fig)
    gc.collect()

    return fig, axes


def _bh_fdr(pvals):
    """Benjamini–Hochberg FDR correction (returns adjusted p-values)."""
    p = np.asarray(pvals, dtype=float)
    n = len(p)
    order = np.argsort(p)
    adj = np.empty(n, dtype=float)
    running_min = 1.0
    for rank_from_end, idx in enumerate(order[::-1], start=1):
        rank = n - rank_from_end + 1
        val = p[idx] * n / rank
        running_min = min(running_min, val)
        adj[idx] = running_min
    return np.minimum(adj, 1.0)


def _bonferroni(pvals):
    p = np.asarray(pvals, dtype=float)
    return np.minimum(p * len(p), 1.0)


def _bootstrap_kendall_tau(x, y, n_boot=2000, rng=None):
    """
    Percentile bootstrap for Kendall's tau over paired (x,y) points.
    Returns (ci_low, ci_high). If <2 points, returns (np.nan, np.nan).
    """
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    n = len(x)
    if n < 2:
        return (np.nan, np.nan)
    if rng is None:
        rng = np.random.default_rng()
    boots = np.empty(n_boot, dtype=float)
    idx = np.arange(n)
    for b in range(n_boot):
        sample = rng.choice(idx, size=n, replace=True)
        tb, _ = kendalltau(x[sample], y[sample], variant="b", nan_policy="omit")
        boots[b] = tb if np.isfinite(tb) else np.nan
    boots = boots[np.isfinite(boots)]
    if boots.size == 0:
        return (np.nan, np.nan)
    return (np.percentile(boots, 2.5), np.percentile(boots, 97.5))


@timer
def kendall_tau_to_pnas_si_table_with_ci(
    df,
    target_column="disruption_percentile",
    career_columns=None,
    min_group_size=5,
    min_sample_threshold=50,
    n_bins=10,
    binning_method="equal",  # "equal" or "quantile"
    team_sizes=None,  # default: 1..7 and "8+"
    correction="fdr_bh",  # "fdr_bh", "bonferroni", or None
    tau_fmt="{:.3f}",
    ci_fmt="{:.3f}",
    caption="Kendall’s $\\tau$ between author ratio and disruption percentile by team size and career stage.",
    label="tab:kendall_team_career",
    save_path="Figures/SI_Kendall_Tau_Table.tex",
    # CI controls:
    add_ci=True,
    ci_level=95,
    n_boot=2000,
    random_state=42,
):
    """
    Computes Kendall’s τ per subplot (team size × career line) with the same binning
    used in your plotting function, applies multiple-testing correction to p-values,
    and outputs a PNAS SI-style LaTeX table where each cell shows:

        τ [CI_low, CI_high]***   (stars based on adjusted p-values)

    CI is a percentile bootstrap over the binned points (x = ratio midpoint, y = median target).
    """
    if career_columns is None:
        career_columns = [
            "first_time_author_ratio",  # Beginner
            "early_career_author_ratio",  # Early-career
            "senior_author_ratio",  # Senior
        ]

    pretty_cols = {
        "first_time_author_ratio": "Beginner",
        "early_career_author_ratio": "Early-career",
        "senior_author_ratio": "Senior",
    }
    if team_sizes is None:
        team_sizes = list(range(1, 8)) + ["8+"]

    needed = career_columns + [target_column, "team_size"]
    df_slim = df[needed].copy()

    recs = []
    rng = np.random.default_rng(random_state)

    for ts in team_sizes:
        if ts == "8+":
            sub = df_slim[df_slim["team_size"] >= 8].copy()
            ts_label = "8+"
        else:
            sub = df_slim[df_slim["team_size"] == ts].copy()
            ts_label = str(ts)

        if sub.empty:
            continue

        for col in career_columns:
            # Build points exactly like the plotting code: x=ratio midpoint, y=median(target)
            pts = []

            # x = 0 bucket
            zero_vals = sub.loc[sub[col] == 0, target_column].dropna()
            if len(zero_vals) >= min_group_size:
                pts.append(
                    {
                        "x": 0.0,
                        "median": float(zero_vals.median()),
                        "count": int(len(zero_vals)),
                    }
                )

            # x > 0 buckets
            pos = sub.loc[sub[col] > 0, [col, target_column]].dropna()
            if len(pos) > 0:
                if binning_method == "equal":
                    edges = np.linspace(0, 1, n_bins + 1)
                    pos = pos.copy()
                    pos["bin"] = pd.cut(pos[col], bins=edges, include_lowest=True)
                    grouped = pos.groupby("bin", observed=False)
                    for interval, g in grouped:
                        if len(g) < min_group_size:
                            continue
                        pts.append(
                            {
                                "x": float(interval.mid),
                                "median": float(g[target_column].median()),
                                "count": int(len(g)),
                            }
                        )
                else:  # quantile
                    pos = pos.copy()
                    pos["bin"] = pd.qcut(pos[col], q=n_bins, duplicates="drop")
                    grouped = pos.groupby("bin", observed=False)
                    for _, g in grouped:
                        if len(g) < min_group_size:
                            continue
                        pts.append(
                            {
                                "x": float(g[col].median()),
                                "median": float(g[target_column].median()),
                                "count": int(len(g)),
                            }
                        )

            pts = pd.DataFrame(pts)
            if pts.empty:
                tau, p, n_pts = (np.nan, np.nan, 0)
                ci_lo, ci_hi = (np.nan, np.nan)
            else:
                pts = pts[pts["count"] >= min_sample_threshold].sort_values("x")
                n_pts = int(len(pts))
                if n_pts < 2:
                    tau, p = (np.nan, np.nan)
                    ci_lo, ci_hi = (np.nan, np.nan)
                else:
                    tau, p = kendalltau(
                        pts["x"].to_numpy(),
                        pts["median"].to_numpy(),
                        variant="b",
                        nan_policy="omit",
                    )
                    if add_ci:
                        ci_lo, ci_hi = _bootstrap_kendall_tau(
                            pts["x"].to_numpy(),
                            pts["median"].to_numpy(),
                            n_boot=n_boot,
                            rng=rng,
                        )
                    else:
                        ci_lo, ci_hi = (np.nan, np.nan)

            recs.append(
                {
                    "team_size": ts_label,
                    "career": pretty_cols.get(col, col),
                    "tau": tau,
                    "p_raw": p,
                    "n_points": n_pts,
                    "ci_low": ci_lo,
                    "ci_high": ci_hi,
                }
            )
            del pos
            gc.collect()

        del sub
        gc.collect()

    res = pd.DataFrame.from_records(recs)

    # Multiple testing correction over all valid p-values
    mask_valid = res["p_raw"].notna()
    pvals = res.loc[mask_valid, "p_raw"].values

    if correction == "fdr_bh" and pvals.size > 0:
        res.loc[mask_valid, "p_adj"] = _bh_fdr(pvals)
        corr_note = "Benjamini–Hochberg FDR correction applied across all tests."
    elif correction == "bonferroni" and pvals.size > 0:
        res.loc[mask_valid, "p_adj"] = _bonferroni(pvals)
        corr_note = "Bonferroni correction applied across all tests."
    else:
        res["p_adj"] = res["p_raw"]
        corr_note = "No multiple-testing correction applied."

    # Stars from adjusted p-values
    def stars(p):
        if not np.isfinite(p):
            return ""
        if p < 0.001:
            return "***"
        if p < 0.010:
            return "**"
        if p < 0.050:
            return "*"
        return ""

    # Compose cell text: τ [lo, hi] + stars
    def cell_text(row):
        if not np.isfinite(row["tau"]):
            return "--"
        tau_s = tau_fmt.format(row["tau"])
        if add_ci and np.isfinite(row["ci_low"]) and np.isfinite(row["ci_high"]):
            ci_s = f"[{ci_fmt.format(row['ci_low'])}, {ci_fmt.format(row['ci_high'])}]"
        else:
            ci_s = "[--, --]"
        return f"{tau_s} {ci_s}{stars(row['p_adj'])}"

    res["cell"] = res.apply(cell_text, axis=1)

    # Pivot to wide: rows are team sizes; columns are the three careers
    order_rows = [str(i) for i in range(1, 8)] + ["8+"]
    order_cols = ["Beginner", "Early-career", "Senior"]
    wide = res.pivot(index="team_size", columns="career", values="cell").reindex(
        index=order_rows, columns=order_cols
    )
    wide = wide.fillna("--")

    # Build LaTeX using the provided PNAS SI template
    lines = []
    lines.append("\\begin{table}\\centering")
    lines.append(f"\\caption{{{caption}}}")
    lines.append("\\begin{tabular}{lrrr}")
    lines.append("Team size & Beginner & Early-career & Senior \\\\")
    lines.append("\\midrule")
    for ts in order_rows:
        if ts not in wide.index:
            lines.append(f"{ts} & -- & -- & -- \\\\")
        else:
            row = wide.loc[ts]
            lines.append(
                f"{ts} & {row['Beginner']} & {row['Early-career']} & {row['Senior']} \\\\"
            )
    lines.append("\\bottomrule")
    lines.append("\\end{tabular}")
    lines.append("\\vspace{0.25em}")
    # concise SI footnote
    lines.append(
        "\\footnotesize{Entries are Kendall’s $\\tau$ with bootstrap "
        f"{ci_level}\\% percentile CIs in brackets; significance stars reflect "
        "adjusted $p$-values: $^{*}p<0.05$, $^{**}p<0.01$, $^{***}p<0.001$. "
        + corr_note
        + "}"
    )
    lines.append("\\end{table}")

    latex_str = "\n".join(lines)

    if save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, "w") as f:
            f.write(latex_str)

    return res, latex_str


@timer
def plot_disruption_by_team_size(
    df,
    target_column="disruption_percentile",
    team_size_column="team_size",
    min_group_size=100,
    n_bins=20,
    save_path=None,
):
    setup_plotting_style()
    required_cols = [team_size_column, "first_time_author_ratio", target_column]
    df = df[required_cols].copy()
    df["team_size_category"] = df[team_size_column].apply(_categorize_team_size)
    df = df[df["team_size_category"].isin(["1-5", "6-10", "11-15", "16-30"])].copy()

    # Define the order of team size categories for consistent visualization
    team_size_order = ["1-5", "6-10", "11-15", "16-30"]
    bin_edges = np.linspace(0, 1, n_bins + 1)

    results = []

    for team_cat in team_size_order:
        team_df = df[df["team_size_category"] == team_cat]

        for i, edge in enumerate(bin_edges):
            if i == 0 and edge == 0:
                bin_data = team_df[team_df["first_time_author_ratio"] <= 0.001]
            else:
                bin_width = 1.0 / n_bins
                bin_start = max(0, edge - bin_width)
                bin_end = edge

                bin_data = team_df[
                    (team_df["first_time_author_ratio"] > bin_start)
                    & (team_df["first_time_author_ratio"] <= bin_end)
                ]

            if len(bin_data) >= min_group_size:
                bin_mean = bin_data[target_column].mean()
                bin_median = bin_data[target_column].median()
                bin_count = len(bin_data)
                bin_std = bin_data[target_column].std()
                bin_sem = bin_std / np.sqrt(bin_count) if bin_count > 0 else 0

                results.append(
                    {
                        "team_size_category": team_cat,
                        "bin_edge": edge,
                        "mean": bin_mean,
                        "median": bin_median,
                        "count": bin_count,
                        "std": bin_std,
                        "sem": bin_sem,
                    }
                )

    group_stats = pd.DataFrame(results)
    del df, results
    gc.collect()

    group_stats["median_ci_lower"] = group_stats["median"] - 1.96 * group_stats["sem"]
    group_stats["median_ci_upper"] = group_stats["median"] + 1.96 * group_stats["sem"]

    if len(group_stats) == 0:
        raise ValueError(
            f"No groups with at least {min_group_size} observations found after binning"
        )

    setup_plotting_style()
    fig, ax = plt.subplots(figsize=(12, 7), dpi=300)
    palette = sns.color_palette("viridis", len(team_size_order))

    gc.collect()

    for idx, team_cat in enumerate(team_size_order):
        cat_data = group_stats[group_stats["team_size_category"] == team_cat]

        # Only plot if we have enough data points
        if len(cat_data) >= 3:  # Require at least 3 points to form a meaningful line
            sns.lineplot(
                x="bin_edge",
                y="median",
                data=cat_data,
                color=palette[idx],
                marker="o",
                markersize=6,
                linewidth=2,
                label=f"Team Size: {team_cat}",
                ax=ax,
            )

            # Add custom confidence intervals as a shaded area
            ax.fill_between(
                cat_data["bin_edge"],
                cat_data["median_ci_lower"],
                cat_data["median_ci_upper"],
                alpha=0.1,
                color=palette[idx],
            )

    # Set labels and title
    ax.set_xlabel("Beginner Author Ratio", fontsize=12)
    ax.set_ylabel("Disruption", fontweight="bold", fontsize=12)

    # Set x-axis ticks at reasonable intervals
    ax.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_xticklabels(["0.00", "0.20", "0.40", "0.60", "0.80", "1.00"])

    # Set y-axis limits if target is percentile
    if "percentile" in target_column.lower():
        ax.set_ylim(20, 80)

    ax.set_title(f"Disruption by Beginner Author Ratio Across Team Sizes", fontsize=14)

    # Add legend with better positioning
    ax.legend(title="Team Size", bbox_to_anchor=(1.02, 1), loc="upper left")

    sns.despine(ax=ax, top=True, right=True)
    plt.tight_layout()

    # Save figure if path is provided
    if save_path:
        plt.savefig(save_path, bbox_inches="tight", dpi=300)

    # Close figure and clean up
    plt.close(fig)
    gc.collect()

    return fig, ax, group_stats


# Helper function to categorize team sizes into groups
def _categorize_team_size(size):
    """
    Categorize team sizes into groups:
    1-5, 6-10, 11-15, 16-20, 21-30, 31-40
    """
    if size <= 5:
        return "1-5"
    elif size <= 10:
        return "6-10"
    elif size <= 15:
        return "11-15"
    elif size <= 30:
        return "16-30"
    else:
        return "other"
    # Note from Mahdee: Kept team_size up to 30 because, later values doesn't shwo statistical significance


@timer
def plot_firsttime_authors_by_decade_grid(
    df,
    target_column="disruption_percentile",
    decade_column="decade",
    author_ratio_column="first_time_author_ratio",
    min_group_size=5,
    min_sample_threshold=50,
    n_bins=10,
    ci=95,
    ylim=(10, 80),
    show_ci=True,
    binning_method="equal",
    ci_method="sem",
    n_bootstrap=1000,
    team_sizes=None,
    save_path=None,
):
    """
    Create a 2x4 grid of subplots, each showing the relationship between Beginner Author Ratio
    and disruption score across different decades for different team sizes.
    """
    setup_plotting_style()

    fig, axes = plt.subplots(2, 4, figsize=(24, 12), sharex=True, sharey=True, dpi=300)
    axes_flat = axes.flatten()

    decades = sorted(df[decade_column].unique())

    colors = sns.color_palette("magma", len(decades))
    decade_colors = {decade: colors[i] for i, decade in enumerate(decades)}

    markers = {decade: "o" for decade in decades}

    # Modified for 2x4 grid: team sizes 1-7 individually, then "8+"
    if team_sizes is None:
        team_sizes = list(range(1, 8)) + ["8+"]

    needed_columns = [author_ratio_column, target_column, "team_size", decade_column]
    df_slim = df[needed_columns].copy()

    for idx, team_size in enumerate(team_sizes):
        ax = axes_flat[idx]

        # Handle the special case for "8+" (last cell)
        if team_size == "8+":
            subset = df_slim[df_slim["team_size"] >= 8].copy()
            title_text = "Team Size: 8+"
        else:
            subset = df_slim[df_slim["team_size"] == team_size].copy()
            title_text = f"Team Size: {team_size}"

        if len(subset) == 0:
            continue

        for decade in decades:
            decade_subset = subset[subset[decade_column] == decade].copy()

            if len(decade_subset) == 0:
                continue

            results = []

            zero_mask = decade_subset[author_ratio_column] == 0
            zero_values = decade_subset.loc[zero_mask, target_column]
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

            values = decade_subset[decade_subset[author_ratio_column] > 0][
                [author_ratio_column, target_column]
            ].dropna()
            if len(values) == 0:
                continue

            # Binning
            if binning_method == "equal":
                bin_edges = np.linspace(0, 1, n_bins + 1)
                values["bin"] = pd.cut(
                    values[author_ratio_column], bins=bin_edges, include_lowest=True
                )
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

            # Note: The results list becomes empty when none of the data points for a particular decade within a team size meet the minimum group size requirements.
            if not results:
                continue

            group_stats = pd.DataFrame(results)

            # Apply minimum sample threshold check
            filtered_stats = group_stats[group_stats["count"] >= min_sample_threshold]
            if filtered_stats.empty:
                continue

            # Plot the line
            sns.lineplot(
                x="x",
                y="median",
                data=filtered_stats,
                color=decade_colors[decade],
                marker=markers[decade],
                markersize=10,
                linewidth=2.5,
                label=(
                    f"{decade}s" if idx == 0 else None
                ),  # Only add labels on first plot
                ax=ax,
            )

            if show_ci:
                ax.fill_between(
                    filtered_stats["x"],
                    filtered_stats["median_ci_lower"],
                    filtered_stats["median_ci_upper"],
                    alpha=0.1,
                    color=decade_colors[decade],
                )

            gc.collect()

        # Customize subplot
        ax.set_title(title_text, fontweight="bold", fontsize=14)
        ax.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1.0])
        ax.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1"])

        # Remove x and y axis labels from individual subplots
        ax.set_xlabel("")
        ax.set_ylabel("")

        if ylim:
            ax.set_ylim(ylim)

        sns.despine(ax=ax, top=True, right=True)

        # Clear memory
        del subset
        gc.collect()

    # Add a single legend for the entire figure
    handles, labels_list = axes_flat[0].get_legend_handles_labels()
    legend = fig.legend(
        handles,
        labels_list,
        loc="lower center",
        bbox_to_anchor=(0.5, 0.02),
        ncol=len(decades),
        frameon=True,
        fontsize=14,
    )

    # Remove individual legends
    for ax in axes_flat:
        if ax.get_legend() is not None:
            ax.get_legend().remove()

    # No need to hide subplots since we're using exactly 8 (2x4)

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
        0.01,
        0.5,
        ylabel_map.get(target_column, target_column),
        va="center",
        rotation="vertical",
        fontsize=18,
        fontweight="bold",
    )

    # Add title
    fig.suptitle(
        f"Impact of Beginner Author Ratio on {ylabel_map.get(target_column, target_column)} Across Decades",
        fontsize=24,
        fontweight="bold",
        y=0.98,
    )

    # Adjust layout
    plt.tight_layout(rect=[0.03, 0.05, 1, 0.95])

    # Save figure if path is provided
    if save_path:
        plt.savefig(save_path, bbox_inches="tight", dpi=300)

    # Close figure and clean up
    plt.close(fig)
    gc.collect()

    return fig, axes


def bin_first_time_author_ratio(
    df,
    ratio_col="first_time_author_ratio",
    out_col="first_time_author_ratio_group",
    k=10,
):
    """
    Create K equal-width bins - INPLACE modification to save memory
    """
    df[ratio_col] = df[ratio_col].astype(float).clip(0.0, 1.0)

    bins = np.linspace(0.0, 1.0, k + 1)
    labels = [f"{bins[i]:.2f} - {bins[i+1]:.2f}" for i in range(k)]

    df[out_col] = pd.cut(
        df[ratio_col],
        bins=bins,
        labels=labels,
        include_lowest=True,
        right=True,
    )

    del bins, labels
    gc.collect()

    return df


def plot_atyp_combination(
    df,
    group_column="first_time_author_ratio_group",
    save_path="Figures/Sup_4_atyp_combination.pdf",
):
    gc.collect()
    setup_plotting_style()

    # Work with only necessary columns
    df_slim = df[[group_column, "Atyp_Median_Z"]].copy()
    gc.collect()

    fig, ax = plt.subplots(figsize=(10, 6), dpi=300)
    gc.collect()

    if pd.api.types.is_categorical_dtype(df_slim[group_column]):
        hue_order = list(df_slim[group_column].cat.categories)
    else:
        hue_order = sorted(df_slim[group_column].dropna().unique().tolist())

    sns.ecdfplot(
        data=df_slim,  # Use slim version
        x="Atyp_Median_Z",
        hue=group_column,
        hue_order=hue_order,
        ax=ax,
        palette="flare",
    )
    del df_slim  # Delete after use
    gc.collect()

    ax.set_xscale("symlog")
    ax.set_xlabel("Atyp Median")
    ax.set_ylabel("Cumulative Distribution")
    ax.axvline(x=0, color="red", linestyle="--", linewidth=2)

    legend = ax.get_legend()
    if legend is not None:
        legend.set_title("Beginner Author Ratio")
        legend.set_bbox_to_anchor((1.0, 0.0))
        legend.set_loc("lower right")
        plt.setp(legend.get_texts(), fontsize="small")
        plt.setp(legend.get_title(), fontsize="small")

    # Cache min/max before multiple accesses
    atyp_min = df["Atyp_Median_Z"].min()
    atyp_max = df["Atyp_Median_Z"].max()

    ax.text(
        -40,
        0.95,
        "Novel\nCombinations",
        color="black",
        ha="left",
        va="top",
        transform=ax.get_xaxis_transform(),
    )
    ax.text(
        1,
        0.95,
        "Conventional\nCombination",
        color="black",
        ha="left",
        va="top",
        transform=ax.get_xaxis_transform(),
    )
    ax.axvspan(xmin=atyp_min, xmax=0, facecolor="lightblue", alpha=0.1)
    ax.axvspan(xmin=0, xmax=atyp_max, facecolor="pink", alpha=0.05)

    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, bbox_inches="tight", dpi=300)

    gc.collect()
    return fig, ax


def run_ks_tests_pairwise(
    df,
    group_col="first_time_author_ratio_group",
    value_col="Atyp_Median_Z",
    results_dir="Results",
    table_name="PNAS_KS_Atyp_Median_Z_k3",
):
    Path(results_dir).mkdir(parents=True, exist_ok=True)

    # Clean input
    x = df[[group_col, value_col]].copy()
    x = x[np.isfinite(x[value_col])].dropna(subset=[group_col, value_col])

    # Keep category order if present; else sort
    if pd.api.types.is_categorical_dtype(x[group_col]):
        groups = [g for g in x[group_col].cat.categories if g in x[group_col].unique()]
    else:
        groups = sorted(x[group_col].dropna().unique().tolist())

    rows = []
    for a, b in combinations(groups, 2):
        xa = x.loc[x[group_col] == a, value_col].to_numpy()
        xb = x.loc[x[group_col] == b, value_col].to_numpy()
        if len(xa) < 2 or len(xb) < 2:
            D, p = np.nan, np.nan
        else:
            # Two-sided KS test
            D, p = ks_2samp(xa, xb, alternative="two-sided", mode="auto")

        rows.append(
            {
                "group_a": str(a),
                "group_b": str(b),
                "n_a": int(len(xa)),
                "n_b": int(len(xb)),
                "D": float(D) if D == D else np.nan,  # keep NaN if not computed
                "p_value": float(p) if p == p else np.nan,
                # Simple descriptives to aid interpretation
                "median_a": float(np.median(xa)) if len(xa) else np.nan,
                "median_b": float(np.median(xb)) if len(xb) else np.nan,
                "median_diff": (
                    (float(np.median(xa)) - float(np.median(xb)))
                    if (len(xa) and len(xb))
                    else np.nan
                ),
            }
        )

    res = pd.DataFrame(rows)

    # Multiple testing corrections (m = number of pairwise tests)
    m = len(res)

    # Bonferroni
    res["p_bonferroni"] = (res["p_value"] * m).clip(upper=1.0)

    # Holm (step-down)
    def holm_stepdown(pvals):
        pvals = np.asarray(pvals, dtype=float)
        order = np.argsort(pvals)
        adj = np.empty_like(pvals)
        running_max = 0.0
        for i, idx in enumerate(order):
            raw_adj = (m - i) * pvals[idx]
            running_max = max(running_max, raw_adj)
            adj[idx] = min(1.0, running_max)
        return adj

    res["p_holm"] = holm_stepdown(res["p_value"].to_numpy())

    # Benjamini–Hochberg FDR
    def fdr_bh(pvals):
        pvals = np.asarray(pvals, dtype=float)
        order = np.argsort(pvals)
        p_sorted = pvals[order]
        q = np.empty_like(pvals)
        running_min = 1.0
        for i in range(m - 1, -1, -1):
            q_i = p_sorted[i] * m / (i + 1)
            running_min = min(running_min, q_i)
            q[i] = running_min
        out = np.empty_like(pvals)
        out[order] = np.clip(q, 0, 1)
        return out

    res["q_bh"] = fdr_bh(res["p_value"].to_numpy())

    # Significance stars (Holm-adjusted)
    def stars(p):
        if np.isnan(p):
            return "n/a"
        if p < 1e-3:
            return "***"
        if p < 1e-2:
            return "**"
        if p < 0.05:
            return "*"
        return "ns"

    res["signif_holm"] = res["p_holm"].apply(stars)

    # Pretty rounding for export/print
    printable = res.copy()
    for col in [
        "D",
        "p_value",
        "p_bonferroni",
        "p_holm",
        "q_bh",
        "median_a",
        "median_b",
        "median_diff",
    ]:
        printable[col] = printable[col].astype(float).round(4)

    # Save CSV + LaTeX
    csv_path = Path(results_dir) / f"{table_name}.csv"
    tex_path = Path(results_dir) / f"{table_name}.tex"

    printable.to_csv(csv_path, index=False)

    # Minimal LaTeX table (PNAS-ready for SI)
    latex_cols = [
        "group_a",
        "group_b",
        "n_a",
        "n_b",
        "D",
        "p_value",
        "p_bonferroni",
        "p_holm",
        "q_bh",
        "signif_holm",
        "median_a",
        "median_b",
        "median_diff",
    ]
    latex_table = printable[latex_cols].to_latex(
        index=False,
        escape=True,
        caption="Two-sample Kolmogorov–Smirnov tests comparing the distributions of $\\it{Atyp\\_Median\\_Z}$ across first-time author ratio quartiles (k=3). Reported are sample sizes, KS statistic $D$, raw and adjusted $p$-values (Bonferroni, Holm), FDR $q$-values (Benjamini–Hochberg), and group medians.",
        label="tab:ks_atyp_median_k3",
    )
    Path(tex_path).write_text(latex_table)

    # Console summary (publication-style)
    print(
        "\nTwo-sample Kolmogorov–Smirnov tests (Atyp_Median_Z by first-time author ratio quartiles)"
    )
    # Overall Ns per group
    group_counts = x.groupby(group_col)[value_col].size()
    print("Group sizes (n):")
    for g in groups:
        print(f"  {g}: {int(group_counts.get(g, 0))}")
    print("\nPairwise comparisons (Holm-adjusted):")
    for _, r in printable.iterrows():
        print(
            f"  {r['group_a']} vs {r['group_b']}: "
            f"D={r['D']:.4f}, p={r['p_value']:.4g}, Holm p={r['p_holm']:.4g} {r['signif_holm']}; "
            f"n_a={int(r['n_a'])}, n_b={int(r['n_b'])}; "
            f"median_a={r['median_a']:.4f}, median_b={r['median_b']:.4f}, Δmedian={r['median_diff']:.4f}"
        )

    print(f"\nSaved CSV: {csv_path}")
    print(f"Saved LaTeX: {tex_path}")

    return res


def plot_expanded_disruption_heatmaps(
    df,
    target_column="disruption_percentile",
    main_columns=[
        "first_time_author_ratio",
        "early_career_author_ratio",
        "senior_author_ratio",
    ],
    x_bins=11,
    y_bins=11,
    method="median",
    min_group_size=10,
    cmap="Reds",
    annotate=True,
    save_path=None,
):
    """
    Create a figure with three subplots showing disruption values for different combinations of author ratios.
    Single row: First-time authors vs career stages, and career stages compared against each other
    """
    setup_plotting_style_mahdee()

    if method not in ["mean", "median"]:
        raise ValueError("Method must be either 'mean' or 'median'")

    # Close any existing figures
    plt.close("all")

    # Create figure and subplots (1 row, 3 columns)
    fig, axes = plt.subplots(1, 3, figsize=(30, 10), dpi=300)

    # Extract columns for easier reference
    fresh_ratio = main_columns[0]
    career_stages = main_columns[1:3]

    # Define all axis pairs we want to plot (3 combinations)
    plot_configs = [
        # first_time_author_ratio (x-axis) vs each career stage (y-axis)
        {"x_column": fresh_ratio, "y_column": career_stages[0]},  # Fresh vs Early
        {"x_column": fresh_ratio, "y_column": career_stages[1]},  # Fresh vs Senior
        # Career stages compared to each other
        {"x_column": career_stages[0], "y_column": career_stages[1]},  # Early vs Senior
    ]

    # Lists to store results
    pivot_dfs = []
    all_values = []

    # Process each subplot configuration
    for config in plot_configs:
        x_column = config["x_column"]
        y_column = config["y_column"]

        # Create a copy with only the necessary columns
        df_subset = df[[x_column, y_column, target_column]].copy()

        # Create equally spaced bins between 0 and 1 for both author ratios
        x_bin_edges = np.linspace(0, 1, x_bins + 1)
        y_bin_edges = np.linspace(0, 1, y_bins + 1)

        # Create bin categories with labels
        df_subset["x_bin"] = pd.cut(
            df_subset[x_column], bins=x_bin_edges, include_lowest=True
        )
        df_subset["y_bin"] = pd.cut(
            df_subset[y_column], bins=y_bin_edges, include_lowest=True
        )

        # Drop original ratio columns as they're no longer needed
        df_subset.drop([x_column, y_column], axis=1, inplace=True)
        gc.collect()

        # Group by bins and compute the aggregated value and count
        grouped = (
            df_subset.groupby(["y_bin", "x_bin"], observed=False)
            .agg({target_column: [method, "count"]})
            .reset_index()
        )

        # Clean up the subset dataframe
        del df_subset
        gc.collect()

        # Rename columns for clarity
        grouped.columns = ["y_bin", "x_bin", "value", "count"]

        # Filter by minimum group size
        grouped = grouped[grouped["count"] >= min_group_size]

        # Create pivot table for heatmap
        pivot_df = grouped.pivot_table(
            index="y_bin", columns="x_bin", values="value", observed=False
        )

        # Create counts pivot table for reference
        counts_df = grouped.pivot_table(
            index="y_bin", columns="x_bin", values="count", observed=False
        )

        # Store the pivot df and counts
        pivot_dfs.append({"pivot": pivot_df, "counts": counts_df})

        # Collect all non-NaN values for consistent color scaling
        all_values.extend(pivot_df.values[~np.isnan(pivot_df.values)])

        # Free memory from grouped dataframe
        del grouped
        gc.collect()

    # Create a common color scale
    vmin, vmax = min(all_values), max(all_values)
    norm = Normalize(vmin=vmin, vmax=vmax)

    # Process each subplot
    for i, (config, pivot_data) in enumerate(zip(plot_configs, pivot_dfs)):
        ax = axes[i]
        pivot_df = pivot_data["pivot"]
        counts_df = pivot_data["counts"]

        x_column = config["x_column"]
        y_column = config["y_column"]

        # Get bin centers for tick labels
        x_centers = [b.mid for b in pivot_df.columns]
        y_centers = [b.mid for b in pivot_df.index]

        # Format tick labels
        x_labels = [f"{c:.1f}" for c in x_centers]
        y_labels = [f"{c:.1f}" for c in y_centers]

        # Create the heatmap with masked data
        masked_data = np.ma.masked_invalid(pivot_df.values)
        im = ax.imshow(
            masked_data,
            cmap=cmap,
            aspect="auto",
            origin="lower",
            interpolation="nearest",
            norm=norm,  # Apply the common normalization
        )

        # Set tick positions and labels
        ax.set_xticks(np.arange(len(x_centers)))
        ax.set_yticks(np.arange(len(y_centers)))
        ax.set_xticklabels(x_labels)
        ax.set_yticklabels(y_labels)

        # Rotate the tick labels and set their alignment
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")

        # Add labels - Change "Fresh Author Ratio" to "First-Time Author Ratio"
        x_label_display = (
            "Beginner Author Ratio"
            if x_column == "first_time_author_ratio"
            else x_column.replace("_", " ")
        )
        y_label_display = y_column.replace("_", " ")

        ax.set_xlabel(x_label_display, fontweight="bold", fontsize=14)
        ax.set_ylabel(y_label_display, fontweight="bold", fontsize=14)

        # Annotate cells with the values
        if annotate:
            for j in range(len(pivot_df.index)):
                for k in range(len(pivot_df.columns)):
                    if not np.isnan(pivot_df.values[j, k]):
                        try:
                            count = (
                                counts_df.values[j, k]
                                if counts_df is not None
                                else "N/A"
                            )
                            # Format count based on magnitude
                            if count >= 1000000:
                                count_text = f"{count/1000000:.2f}M"
                            elif count > 1000:
                                count_text = f"{count/1000:.2f}K"
                            else:
                                count_text = f"{count}"
                        except:
                            count_text = ""

                        # Format the value text more cleanly
                        value_text = f"{pivot_df.values[j, k]:.2f}"

                        # Display value with better formatting
                        ax.text(
                            k,
                            j + 0.15,  # Position value slightly above center
                            value_text,
                            ha="center",
                            va="center",
                            color="black",
                            fontsize=9,
                            fontweight="bold",
                        )

                        # Display count in smaller font below
                        if count_text:
                            ax.text(
                                k,
                                j - 0.15,  # Position count slightly below center
                                f"n={count_text}",
                                ha="center",
                                va="center",
                                color="black",
                                fontsize=6,
                                alpha=0.8,  # Make it slightly transparent
                            )

        # Add grid lines
        ax.set_xticks(np.arange(-0.5, len(x_centers), 1), minor=True)
        ax.set_yticks(np.arange(-0.5, len(y_centers), 1), minor=True)
        ax.grid(which="minor", color="w", linestyle="-", linewidth=1)
        ax.tick_params(which="minor", bottom=False, left=False)
        sns.despine(ax=ax, top=True, right=True)

        # Clean up subplot-specific variables
        del masked_data, x_centers, y_centers, x_labels, y_labels
        gc.collect()

    # Add a common colorbar
    cbar_ax = fig.add_axes([0.92, 0.25, 0.02, 0.5])  # [left, bottom, width, height]
    cbar = fig.colorbar(im, cax=cbar_ax)
    cbar.set_label(
        f"{method.capitalize()} {target_column.replace('_', ' ')}",
        fontweight="bold",
        fontsize=14,
    )

    # Add a common title for the entire figure
    fig.suptitle(
        f"Comparison of {target_column.replace('_', ' ')} across Different Author Ratio Combinations",
        fontweight="bold",
        fontsize=16,
        y=0.98,
    )

    # Add a note about the cell values
    fig.text(
        0.5,
        0.01,
        "Note: Each cell shows the median disruption percentile value and sample size (n)",
        fontsize=12,
        ha="center",
        va="bottom",
        style="italic",
    )

    # Adjust spacing instead of using tight_layout to avoid the warning
    fig.subplots_adjust(left=0.05, right=0.90, bottom=0.15, top=0.85, wspace=0.3)

    # Save figure if path is provided
    if save_path:
        plt.savefig(save_path, bbox_inches="tight", dpi=300)

    del all_values, vmin, vmax, norm, im, cbar
    gc.collect()
    return fig, axes, pivot_dfs


def _pretty_axis_label(colname: str) -> str:
    return (
        "Beginner Author Ratio"
        if colname == "first_time_author_ratio"
        else colname.replace("_", " ").title()
    )


def _bin_center_labels(interval_index, fmt="{:.1f}"):
    return [fmt.format(iv.mid) for iv in interval_index]


def _fmt_count(n: Optional[Union[float, int]], compress=True) -> str:
    if n is None or (isinstance(n, float) and np.isnan(n)):
        return ""
    n = int(n)
    if not compress:
        return f"{n}"
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n/1_000:.2f}K"
    return f"{n}"


def pnas_si_matrix_tables_from_heatmaps(
    pivot_dfs,
    plot_configs,
    method="median",
    target_column="disruption_percentile",
    caption_prefix="Disruption percentiles by author-ratio bins.",
    label_prefix="tab:si-disruption-matrix",
    value_fmt="{:.2f}",
    compress_counts=True,
    na_text="--",
    use_makecell=True,  # if False, prints "value (n=…)" on one line
    table_env="table",  # "table" or "table*"
    font_size_cmd="\\scriptsize",
):
    """
    Produce PNAS SI–style matrix LaTeX tables (like the heatmap) for each panel.
    Each cell shows the aggregated value and sample size n.
    Y-axis order is flipped so rows align with the matplotlib heatmap (origin="lower").
    """
    out = []
    for panel_idx, pdata in enumerate(pivot_dfs):
        pivot = pdata["pivot"]
        counts = pdata["counts"]

        xcol = plot_configs[panel_idx]["x_column"]
        ycol = plot_configs[panel_idx]["y_column"]
        x_label = _pretty_axis_label(xcol)
        y_label = _pretty_axis_label(ycol)

        # Labels from bin centers
        x_centers = _bin_center_labels(pivot.columns)
        y_centers = _bin_center_labels(pivot.index)

        # Flip y-axis to match heatmap orientation (lowest bin at bottom)
        y_centers = list(reversed(y_centers))
        pivot_values = np.flipud(pivot.values)
        counts_values = np.flipud(
            counts.values if counts is not None else np.full_like(pivot_values, np.nan)
        )

        # Header row
        header_cells = [y_label + " (bin center)"] + x_centers
        header_row = " & ".join(header_cells) + " \\\\"

        # Body rows
        body_lines = []
        for i, ylab in enumerate(y_centers):
            row_cells = [ylab]
            for j, _ in enumerate(x_centers):
                v = pivot_values[i, j]
                n = counts_values[i, j] if counts is not None else None

                if isinstance(v, float) and np.isnan(v):
                    cell = na_text
                else:
                    vtxt = value_fmt.format(v)
                    ntxt = (
                        _fmt_count(n, compress=compress_counts) if n is not None else ""
                    )
                    if use_makecell:
                        if ntxt:
                            cell = f"\\makecell{{{vtxt} \\\\ \\scriptsize n={ntxt}}}"
                        else:
                            cell = f"\\makecell{{{vtxt}}}"
                    else:
                        cell = f"{vtxt}" + (f" (n={ntxt})" if ntxt else "")
                row_cells.append(cell)
            body_lines.append(" & ".join(row_cells) + " \\\\")
        body_str = "\n".join(body_lines)

        # Caption + label
        panel_letter = chr(ord("A") + panel_idx)
        cap_core = (
            f"Panel {panel_letter}: {y_label} (rows) vs {x_label} (columns). "
            f"Cells show {method} {target_column.replace('_',' ')} and sample size ($n$)."
        )
        caption = f"{caption_prefix} {cap_core}"
        lab = f"{label_prefix}-{panel_idx+1}"

        align_spec = "l" + "c" * len(x_centers)

        tex = dedent(
            f"""
            \\begin{{{table_env}}}\\centering
            \\caption{{{caption}}}
            {font_size_cmd}
            \\setlength\\tabcolsep{{4pt}}
            \\renewcommand{{\\arraystretch}}{{1.1}}
            \\begin{{tabular}}{{{align_spec}}}
            \\toprule
            & \\multicolumn{{{len(x_centers)}}}{{c}}{{{x_label} (bin center)}} \\\\
            \\cmidrule(lr){{2-{len(x_centers)+1}}}
            {header_row}
            \\midrule
            {body_str}
            \\bottomrule
            \\end{{tabular}}
            \\label{{{lab}}}
            \\end{{{table_env}}}
            """
        ).strip()
        out.append(tex)

    return out


def plot_firsttime_authors_by_coauthor_disruption(
    df,
    target_column="disruption_percentile",
    coauthor_group_column="co_authors_disruption_group",
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
    coauthor_groups_to_plot=None,
    save_path=None,
):
    """
    Create a 2x4 grid of subplots, each showing the relationship between First-Time Author Ratio
    and disruption score across different co-author disruption groups for different team sizes.
    """
    setup_plotting_style()

    fig, axes = plt.subplots(2, 4, figsize=(24, 12), sharex=True, sharey=True, dpi=300)
    axes_flat = axes.flatten()

    # Get co-author disruption groups to plot
    if coauthor_groups_to_plot is None:
        coauthor_groups = sorted(df[coauthor_group_column].unique())
    else:
        coauthor_groups = [
            group
            for group in coauthor_groups_to_plot
            if group in df[coauthor_group_column].unique()
        ]

    # Check if any valid groups remain
    if not coauthor_groups:
        raise ValueError(
            f"None of the specified co-author groups found in the data. Available groups: {sorted(df[coauthor_group_column].unique())}"
        )

    # Use rocket color palette for disruption groups to show progression
    colors = sns.color_palette("rocket_r", n_colors=len(coauthor_groups))
    group_colors = {group: colors[i] for i, group in enumerate(coauthor_groups)}

    # Use different markers for each group
    marker_list = ["o", "s", "^", "D", "v"]
    markers = {
        group: marker_list[i % len(marker_list)]
        for i, group in enumerate(coauthor_groups)
    }

    if team_sizes is None:
        team_sizes = range(2, 10)  # 8 team sizes for 2x4 grid

    needed_columns = [
        author_ratio_column,
        target_column,
        "team_size",
        coauthor_group_column,
    ]
    df_slim = df[needed_columns].copy()

    for idx, team_size in enumerate(team_sizes):
        if idx >= 8:  # Only process first 8 team sizes for 2x4 grid
            break

        ax = axes_flat[idx]
        subset = df_slim[df_slim["team_size"] == team_size].copy()

        if len(subset) == 0:
            continue

        for group in coauthor_groups:
            group_subset = subset[subset[coauthor_group_column] == group].copy()

            if len(group_subset) == 0:
                continue

            results = []

            # Handle zero values separately
            zero_mask = group_subset[author_ratio_column] == 0
            zero_values = group_subset.loc[zero_mask, target_column]
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

            # Handle non-zero values
            values = group_subset[group_subset[author_ratio_column] > 0][
                [author_ratio_column, target_column]
            ].dropna()
            if len(values) == 0:
                continue

            # Binning
            if binning_method == "equal":
                bin_edges = np.linspace(0, 1, n_bins + 1)
                values["bin"] = pd.cut(
                    values[author_ratio_column], bins=bin_edges, include_lowest=True
                )
            else:
                values["bin"] = pd.qcut(
                    values[author_ratio_column], q=n_bins, duplicates="drop"
                )

            grouped = values.groupby("bin", observed=False)

            for bin_interval, group_data in grouped:
                if len(group_data) < min_group_size:
                    continue

                median = group_data[target_column].median()
                std = group_data[target_column].std()
                sem = std / np.sqrt(len(group_data))

                if ci_method == "bootstrap":
                    boot = np.random.choice(
                        group_data[target_column], (n_bootstrap, len(group_data))
                    )
                    boot_meds = np.median(boot, axis=1)
                    lower = np.percentile(boot_meds, (100 - ci) / 2)
                    upper = np.percentile(boot_meds, 100 - (100 - ci) / 2)
                else:
                    z = 1.96
                    lower = median - z * sem
                    upper = median + z * sem

                # Compute bin midpoint
                if binning_method == "equal":
                    midpoint = bin_interval.mid
                else:
                    midpoint = group_data[author_ratio_column].median()

                results.append(
                    {
                        "x": midpoint,
                        "median": median,
                        "count": len(group_data),
                        "median_ci_lower": lower,
                        "median_ci_upper": upper,
                    }
                )

            group_stats = pd.DataFrame(results)

            # Apply minimum sample threshold check
            filtered_stats = group_stats[group_stats["count"] >= min_sample_threshold]
            if filtered_stats.empty:
                continue

            # Plot the line
            sns.lineplot(
                x="x",
                y="median",
                data=filtered_stats,
                color=group_colors[group],
                marker=markers[group],
                markersize=8,
                linewidth=2.5,
                label=group if idx == 0 else None,  # Only add labels on first plot
                ax=ax,
            )

            if show_ci:
                ax.fill_between(
                    filtered_stats["x"],
                    filtered_stats["median_ci_lower"],
                    filtered_stats["median_ci_upper"],
                    alpha=0.1,
                    color=group_colors[group],
                )

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

        # Clear memory
        del subset
        gc.collect()

    # Add a single legend for the entire figure
    handles, labels_list = axes_flat[0].get_legend_handles_labels()
    legend = fig.legend(
        handles,
        labels_list,
        loc="lower center",
        bbox_to_anchor=(0.5, 0.02),
        ncol=min(
            len(coauthor_groups), 5
        ),  # Up to 5 columns for the 5 disruption groups
        frameon=True,
        fontsize=12,
    )

    # Remove individual legends
    for ax in axes_flat:
        if ax.get_legend() is not None:
            ax.get_legend().remove()

    # Handle empty subplots if less than 8 team sizes
    for i in range(len(team_sizes), 8):
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

    if coauthor_group_column == "co_authors_disruption_group":
        title_var = "Team Disruption Percentile"
    elif coauthor_group_column == "co_authors_citation_group":
        title_var = "Team Citation Percentile"

    fig.text(
        0.01,
        0.5,
        ylabel_map.get(target_column, target_column),
        va="center",
        rotation="vertical",
        fontsize=18,
        fontweight="bold",
    )

    # Add title
    fig.suptitle(
        f"Beginner Author Ratio and {ylabel_map.get(target_column, target_column)} accross {title_var}",
        fontsize=22,
        fontweight="bold",
        y=0.98,
    )

    # Adjust layout
    plt.tight_layout(rect=[0.03, 0.05, 1, 0.95])

    # Save figure if path is provided
    if save_path:
        plt.savefig(save_path, bbox_inches="tight", dpi=300)

    gc.collect()

    return fig, axes


def analyze_author_count_combinations(df, min_count=10, method="median"):
    """
    Analyze combinations of author counts and their relationship to disruption percentiles.

    This function:
    1. Groups by all 3 author count columns (no binning)
    2. For each combination, calculates statistics on disruption percentile based on method
    3. Sorts results by the chosen disruption percentile statistic (descending)

    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe containing author count columns and disruption_percentile
    min_count : int, default=10
        Minimum number of records required in each combination
    method : str, default="median"
        Method to use for analysis, either "median" or "mean"

    Returns
    -------
    pandas.DataFrame
        Dataframe with columns for each author count, chosen disruption_percentile statistic, and N_Records
    """
    # Validate method parameter
    if method not in ["median", "mean"]:
        raise ValueError("Method must be either 'median' or 'mean'")

    # Define the author count columns
    count_columns = [
        "first_time_author_count",
        "early_career_author_count",
        "senior_author_count",
    ]

    # Step 1: Group by all 3 count columns
    grouped = df.groupby(count_columns)

    # Step 2: Calculate statistics for each group
    if method == "median":
        stats = (
            grouped["disruption_percentile"]
            .agg(["median", "mean", "count"])
            .reset_index()
        )
        main_stat_name = "Median_Disruption_Percentile"
        main_stat_column = "median"
    else:  # method == "mean"
        stats = grouped["disruption_percentile"].agg(["mean", "count"]).reset_index()
        main_stat_name = "Mean_Disruption_Percentile"
        main_stat_column = "mean"

    # Delete grouped object immediately
    del grouped
    gc.collect()

    # Create the result dataframe
    result_columns = {
        "first_time_author_count": stats["first_time_author_count"],
        "early_career_author_count": stats["early_career_author_count"],
        "senior_author_count": stats["senior_author_count"],
        main_stat_name: stats[main_stat_column],
        "N_Records": stats["count"],
    }

    # Add mean disruption percentile if using median method
    if method == "median":
        result_columns["Mean_Disruption_Percentile"] = stats["mean"]

    combination_df = pd.DataFrame(result_columns)

    # Delete stats after extracting what we need
    del stats
    gc.collect()

    # Add team_size column
    combination_df["team_size"] = (
        combination_df["first_time_author_count"]
        + combination_df["early_career_author_count"]
        + combination_df["senior_author_count"]
    )

    # Filter by minimum count
    combination_df = combination_df[combination_df["N_Records"] >= min_count]

    # Step 3: Sort by chosen disruption statistic (descending)
    combination_df = combination_df.sort_values(main_stat_name, ascending=False)

    # Determine output columns
    output_columns = [
        "first_time_author_count",
        "early_career_author_count",
        "senior_author_count",
        main_stat_name,
        "N_Records",
    ]

    result = combination_df[output_columns].sort_values(
        by=[main_stat_name, "first_time_author_count"], ascending=[False, True]
    )

    # Clean up temporary objects
    del combination_df, result_columns, output_columns
    gc.collect()

    return result


def create_team_disruption_svg_direct(
    df,
    output_filename="Final_Figures/Final_team_disruption.svg",
    top_n=25,
    method="median",
):
    """
    Create an SVG file visualizing team composition and disruption percentiles using direct SVG generation.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing author count columns and disruption_percentile columns
    output_filename : str, default="team_disruption.svg"
        Filename for the output SVG file
    top_n : int, default=25
        Number of top teams to display
    method : str, default="median"
        Method used for analysis, either "median" or "mean"
    """
    # Determine correct disruption column based on method
    disruption_col = (
        f"{'Median' if method == 'median' else 'Mean'}_Disruption_Percentile"
    )

    # Take top N rows and sort by disruption
    plot_data = df.head(top_n).copy()
    plot_data = plot_data.sort_values(disruption_col, ascending=False).reset_index(
        drop=True
    )

    # Add team_size column if not present
    if "team_size" not in plot_data.columns:
        plot_data["team_size"] = (
            plot_data["first_time_author_count"]
            + plot_data["early_career_author_count"]
            + plot_data["senior_author_count"]
        )

    # Set up dimensions and margins
    width = 1000
    height = 800
    margin = {"top": 40, "right": 120, "bottom": 60, "left": 200}
    graph_width = width - margin["left"] - margin["right"]
    graph_height = height - margin["top"] - margin["bottom"]

    # Define career columns and map colors using viridis_r palette
    career_columns = [
        "first_time_author_count",
        "early_career_author_count",
        "senior_author_count",
    ]

    full_palette = sns.color_palette("viridis_r", 8)
    color_indices = [0, 1, 3, 5]
    selected_colors = [mcolors.to_hex(full_palette[i]) for i in color_indices]
    colors = {col: selected_colors[i] for i, col in enumerate(career_columns)}

    # Emoji mapping
    emoji_map = {
        "first_time_author_count": "🌱",
        "early_career_author_count": "🚀",
        "senior_author_count": "🧠",
    }

    # Disruption scale - dynamic based on data
    min_disruption = max(40, plot_data[disruption_col].min() - 5)
    max_disruption = min(75, plot_data[disruption_col].max() + 5)

    # Round to nearest 5 for cleaner ticks
    min_disruption = 5 * round(min_disruption / 5)
    max_disruption = 5 * round(max_disruption / 5)

    # Start SVG content
    svg_content = f"""<?xml version="1.0" encoding="UTF-8" standalone="no"?>
    <svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">
        <rect x="0" y="0" width="{width}" height="{height}" fill="white"/>
        <style>
            .title {{ font-size: 18px; font-weight: bold; }}
            .axis-label {{ font-size: 12px; }}
            .tick-label {{ font-size: 10px; }}
            .bar {{ stroke: #888; stroke-width: 0.5; }}
            .bar-label {{ font-size: 10px; }}
            .legend {{ font-size: 10px; }}
        </style>

         <!-- Title (centered) -->
        <text class="title" x="{width/2}" y="25" text-anchor="middle">Career-Stage Based Team Composition and {method.title()} Disruption</text>

        <!-- X-axis label -->
        <text class="axis-label" x="{margin['left'] + graph_width/2}" y="{height - 15}" text-anchor="middle">{method.title()} Disruption Percentile</text>

        <!-- X-axis -->
        <line x1="{margin['left']}" y1="{height - margin['bottom']}" x2="{width - margin['right']}" y2="{height - margin['bottom']}" stroke="black" />
    """

    # Add X-axis ticks - dynamic based on min/max
    tick_range = range(int(min_disruption), int(max_disruption + 1), 5)
    for tick in tick_range:
        tick_x = (
            margin["left"]
            + ((tick - min_disruption) / (max_disruption - min_disruption))
            * graph_width
        )
        svg_content += f"""
        <line x1="{tick_x}" y1="{height - margin['bottom']}" x2="{tick_x}" y2="{height - margin['bottom'] + 5}" stroke="black" />
        <text class="tick-label" x="{tick_x}" y="{height - margin['bottom'] + 15}" text-anchor="middle">{tick}</text>
        """

    # Add bars for each team
    bar_height = graph_height / len(plot_data)
    bar_padding = bar_height * 0.2

    for i, (idx, row) in enumerate(plot_data.iterrows()):
        y_position = margin["top"] + i * bar_height + bar_height / 2
        bar_width = (
            (row[disruption_col] - min_disruption) / (max_disruption - min_disruption)
        ) * graph_width

        # Draw segmented bar
        start_x = margin["left"]
        team_size = row["team_size"]

        # Create emojis for y-tick
        team_emojis = ""
        for col in career_columns:
            if row[col] > 0:
                team_emojis += emoji_map[col] * int(row[col])

        # Add y-tick with emojis
        svg_content += f"""
        <text class="tick-label" x="{margin['left'] - 10}" y="{y_position}" text-anchor="end" dy="0.35em">{team_emojis}</text>
        """

        for col in career_columns:
            if row[col] > 0:
                segment_width = (row[col] / team_size) * bar_width
                svg_content += f"""
                <rect class="bar" x="{start_x}" y="{y_position - (bar_height - bar_padding*2)/2}"
                      width="{segment_width}" height="{bar_height - bar_padding*2}"
                      fill="{colors[col]}" />
                """

                # No emoji rendering here anymore - moved to y-ticks
                start_x += segment_width

        svg_content += f"""
        <text class="bar-label" x="{margin['left'] + bar_width + 5}" y="{y_position}" dy="0.35em">
            {row[disruption_col]:.2f} <tspan fill="#888">n = {row['N_Records']:,}</tspan>
        </text>
        """

    # Legend
    legend_y = height / 2 - margin["bottom"]
    legend_x = width - margin["right"] + 10

    svg_content += f"""
    <rect x="{legend_x - 10}" y="{legend_y - 10}" width="130" height="100" fill="white" stroke="#ddd" />
    """

    for i, col in enumerate(career_columns):
        label = col.replace("_author_count", "").replace("_", " ").title()
        y = legend_y + i * 20
        svg_content += f"""
        <rect x="{legend_x}" y="{y}" width="10" height="10" fill="{colors[col]}" />
        <text x="{legend_x + 15}" y="{y + 8}" class="legend">{emoji_map[col]} {label}</text>
        """

    # Close SVG
    svg_content += "</svg>"

    # Write to file
    with open(output_filename, "w", encoding="utf-8") as f:
        f.write(svg_content)

    print(f"SVG visualization saved to {output_filename}")


# --- Core: compute correlations and stream results to disk (CUMULATIVE ONLY) ---
@timer
def compute_correlations_to_disk(
    final_df,
    out_dir="intermediate_data",
    min_rows_for_percentile=10,
    min_rows_per_group=5,
    overwrite=False,
    band_width=5,  # step size for cumulative thresholds: e.g., 5 -> [5,10,...,100]
    corr_method="pearson",  # "pearson", "spearman", or "kendall"
):
    """
    Computes groupwise correlations between disruption and citation percentiles for cumulative
    top-N% thresholds and writes tiny per-threshold CSVs to disk to save RAM.

    Cumulative selection for each threshold N:
        mask = disruption_percentile >= (100 - N)

    Thresholds on the x-axis are N = band_width, 2*band_width, ... , 100 (100 is always included).
    """
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    cols_needed = [
        "disruption_percentile",
        "citation_count_percentile",
        "first_time_author_ratio_group",
    ]
    missing = [c for c in cols_needed if c not in final_df.columns]
    if missing:
        raise ValueError(f"final_df is missing required columns: {missing}")

    # Keep only needed columns; coerce dtypes
    df = final_df[cols_needed].copy()
    df["disruption_percentile"] = pd.to_numeric(
        df["disruption_percentile"], errors="coerce"
    )
    df["citation_count_percentile"] = pd.to_numeric(
        df["citation_count_percentile"], errors="coerce"
    )
    df["first_time_author_ratio_group"] = df["first_time_author_ratio_group"].astype(
        "category"
    )

    # --- Cumulative thresholds controlled by band_width; always include 100 ---
    if band_width <= 0 or band_width > 100:
        raise ValueError("band_width must be in (0, 100].")
    x_ticks = list(range(band_width, 100, band_width))
    if 100 not in x_ticks:
        x_ticks.append(100)

    total = len(x_ticks)
    for idx, N in enumerate(x_ticks, 1):
        csv_path = Path(out_dir) / f"corr_p{N:03d}.csv"
        meta_path = Path(out_dir) / f"meta_p{N:03d}.json"

        if csv_path.exists() and meta_path.exists() and not overwrite:
            print(f"[{idx}/{total}] cumulative: N={N}%  — skip (already computed)")
            continue

        # --- Cumulative selection ---
        low = 100 - N
        mask = df["disruption_percentile"] >= low
        top_df = df[mask].copy()

        # Progress
        range_str = f"[{low:.3f}, 100.000]"
        print(
            f"[{idx}/{total}] cumulative: N={N}% → rows={len(top_df)}  range={range_str}"
        )

        # Skip small selections to avoid noisy correlations
        if len(top_df) < min_rows_for_percentile:
            pd.DataFrame(
                columns=["percentile_threshold", "group", "correlation", "sample_size"]
            ).to_csv(csv_path, index=False)
            meta = {
                "percentile_threshold": N,
                "selection": "cumulative",
                "cut_low": float(low),
                "cut_high": 100.0,
                "rows_in_top": int(len(top_df)),
                "skipped": True,
                "reason": f"rows_in_top < {min_rows_for_percentile}",
                "corr_method": corr_method,
                "min_rows_per_group": int(min_rows_per_group),
                "band_width": band_width,
            }
            meta_path.write_text(json.dumps(meta, indent=2))
            del top_df, mask
            gc.collect()
            continue

        # --- Groupwise correlation safely (no DeprecationWarning) ---
        def _safe_corr_vals(g):
            if len(g) >= min_rows_per_group:
                return g["disruption_percentile"].corr(
                    g["citation_count_percentile"], method=corr_method
                )
            return np.nan

        correlations = top_df.groupby("first_time_author_ratio_group", observed=True)[
            ["disruption_percentile", "citation_count_percentile"]
        ].apply(
            _safe_corr_vals
        )  # -> Series indexed by group

        # Precompute group sizes once
        group_sizes = top_df["first_time_author_ratio_group"].value_counts()

        # Collect rows (only non-NaN correlations)
        rows = []
        for group, corr in correlations.items():
            if pd.notna(corr):
                rows.append(
                    {
                        "percentile_threshold": N,
                        "group": str(group),
                        "correlation": float(corr),
                        "sample_size": int(group_sizes.get(group, 0)),
                    }
                )

        # Write out the tiny CSV
        pd.DataFrame(
            rows,
            columns=["percentile_threshold", "group", "correlation", "sample_size"],
        ).to_csv(csv_path, index=False)

        # Meta JSON
        meta = {
            "percentile_threshold": N,
            "selection": "cumulative",
            "cut_low": float(low),
            "cut_high": 100.0,
            "rows_in_top": int(len(top_df)),
            "groups_in_result": int(len(rows)),
            "skipped": len(rows) == 0,
            "corr_method": corr_method,
            "min_rows_per_group": int(min_rows_per_group),
            "band_width": band_width,
        }
        meta_path.write_text(json.dumps(meta, indent=2))

        # Clean up
        del top_df, mask, correlations, group_sizes, rows
        gc.collect()

    print("\nAll thresholds processed. Intermediate files are in:", out_dir)

    # Final cleanup
    del df
    gc.collect()


# --- Helper: load all tiny CSVs into one DataFrame ---
def _load_results_df(out_dir="intermediate_data"):
    files = sorted(glob.glob(os.path.join(out_dir, "corr_p*.csv")))
    if not files:
        return None
    parts = [pd.read_csv(f) for f in files]
    if not parts:
        return None
    res = pd.concat(parts, ignore_index=True)
    return res


# --- Plot + save PDF (labels reflect cumulative mode & band_width step) ---
@timer
def load_results_and_plot_raiyan(
    out_dir="intermediate_data", fig_dir="Figures", band_width=5, fig_name=None
):
    """
    Loads the per-threshold correlation snippets (cumulative top-N%) and plots one line per group.
    Saves a high-res PDF (dpi=300) to `fig_dir`.
    """
    setup_plotting_style()

    results_df = _load_results_df(out_dir)
    if results_df is None or results_df.empty:
        print(f"No valid correlations found under {out_dir}/.")
        return

    palette = sns.color_palette("rocket_r", n_colors=results_df["group"].nunique())
    plt.figure(figsize=(12, 8))

    for group, color in zip(sorted(results_df["group"].dropna().unique()), palette):
        gdf = results_df[results_df["group"] == group].sort_values(
            "percentile_threshold"
        )
        if not gdf.empty:
            plt.plot(
                gdf["percentile_threshold"],
                gdf["correlation"],
                marker="o",
                label=group,
                color=color,
                linewidth=2,
                markersize=6,
            )

    plt.xlabel(f"Top N% Most Disruptive Papers (Cumulative; step = {band_width}%)")
    plt.ylabel("Correlation Coefficient\n between disruption and citation ranks")
    plt.axhline(y=0, color="gray", linestyle="--", alpha=0.5, linewidth=1)
    plt.legend(
        title="Beginner Author Ratio Quartiles",
        title_fontsize=11,
        fontsize=10,
        bbox_to_anchor=(1.05, 1),
        loc="upper left",
    )

    # x ticks: use thresholds present; thin if many
    xvals = sorted(results_df["percentile_threshold"].unique())
    step = max(1, len(xvals) // 10)
    plt.xticks(xvals[::step])

    ax = plt.gca()
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()

    # --- Save as PDF (dpi=300) ---
    Path(fig_dir).mkdir(parents=True, exist_ok=True)
    if fig_name is None:
        fig_name = (
            f"Final_disruption_citation_correlation_cumulative_step{band_width}.pdf"
        )
    out_path = Path(fig_dir) / fig_name
    plt.savefig(out_path, dpi=300, bbox_inches="tight")
    print(f"\nFigure saved to {out_path} (dpi=300)")

    # Get figure before closing
    fig = plt.gcf()
    plt.close(fig)

    # Console summary
    print("\nSummary Statistics:")
    print(f"Number of data points (rows across thresholds): {len(results_df)}")
    print(f"Groups analyzed: {results_df['group'].nunique()}")
    print(
        f"Percentile thresholds covered: {int(np.nanmin(results_df['percentile_threshold']))}% to {int(np.nanmax(results_df['percentile_threshold']))}%"
    )

    correlation_summary = (
        results_df.groupby("group", dropna=True)["correlation"]
        .agg(["min", "max", "mean", "std"])
        .round(3)
    )
    print("\nCorrelation Summary by Group:")
    print(correlation_summary)

    # Clean up
    del fig, ax, results_df, palette, correlation_summary, xvals
    gc.collect()


# Load the tiny per-threshold CSVs
def _load_results_df(out_dir="intermediate_data"):
    files = sorted(glob.glob(os.path.join(out_dir, "corr_p*.csv")))
    if not files:
        return None
    parts = [pd.read_csv(f) for f in files]
    if not parts:
        return None
    return pd.concat(parts, ignore_index=True)


def _safe_atanh(r, eps=1e-12):
    if pd.isna(r):
        return np.nan
    # Guard against |r|=1.0
    r = float(np.clip(r, -1 + eps, 1 - eps))
    return atanh(r)


def _p_two_sided_from_z(z):
    if not isfinite(z):
        return np.nan
    # two-sided p = 2 * (1 - Phi(|z|)) = erfc(|z|/sqrt(2))
    return erfc(abs(z) / sqrt(2.0))


def _holm_stepdown(pvals):
    p = np.asarray(pvals, dtype=float)
    order = np.argsort(p)
    adj = np.empty_like(p)
    running_max = 0.0
    m = len(p)
    for i, idx in enumerate(order):
        raw_adj = (m - i) * p[idx]
        running_max = max(running_max, raw_adj)
        adj[idx] = min(1.0, running_max)
    return adj


def _fdr_bh(pvals):
    p = np.asarray(pvals, dtype=float)
    m = len(p)
    order = np.argsort(p)
    p_sorted = p[order]
    q = np.empty_like(p)
    running_min = 1.0
    for i in range(m - 1, -1, -1):
        q_i = p_sorted[i] * m / (i + 1)
        running_min = min(running_min, q_i)
        q[i] = min(1.0, running_min)
    out = np.empty_like(p)
    out[order] = q
    return out


def _star(p):
    if pd.isna(p):
        return "n/a"
    if p < 1e-3:
        return "***"
    if p < 1e-2:
        return "**"
    if p < 0.05:
        return "*"
    return "ns"


def run_quartile_line_tests_cumulative(
    out_dir="intermediate_data",
    results_dir="Results",
    table_name="PNAS_quartile_line_tests_step10",
    expected_quartiles=("0.00 - 0.25", "0.25 - 0.50", "0.50 - 0.75", "0.75 - 1.00"),
    step_filter=10,  # only use thresholds that are multiples of this (e.g., 10 → 10,20,...,100)
    p_floor=1e-300,
):
    """
    Pairwise Fisher r-to-z tests comparing quartile correlation lines at each cumulative threshold.
    Adjusts p-values per threshold across the 6 pairwise comparisons (Holm + BH/FDR).
    Prints PNAS-ready text and saves CSV + LaTeX tables.

    Returns
    -------
    per_threshold_df : long-form DataFrame of all tests across thresholds
    """
    Path(results_dir).mkdir(parents=True, exist_ok=True)
    res = _load_results_df(out_dir)
    if res is None or res.empty:
        print(f"[ERROR] No correlation files found under {out_dir}/.")
        return None

    # Keep only thresholds that match your cumulative step (e.g., 10,20,...,100)
    thresholds = sorted(
        t
        for t in res["percentile_threshold"].dropna().unique().tolist()
        if int(t) % int(step_filter) == 0
    )

    # Standardize quartile ordering if labels are present
    # Fallback: sort alphabetically
    if set(expected_quartiles).issubset(set(res["group"].unique())):
        q_order = list(expected_quartiles)
    else:
        q_order = sorted(res["group"].dropna().unique().tolist())

    rows = []
    for N in thresholds:
        sub = res[res["percentile_threshold"] == N].copy()

        # Map quartile -> (r, n)
        info = {}
        for _, row in sub.iterrows():
            q = str(row["group"])
            info[q] = (float(row["correlation"]), int(row.get("sample_size", np.nan)))

        # Generate all 6 pairwise comparisons among the 4 quartiles
        pairs = []
        for i in range(len(q_order)):
            for j in range(i + 1, len(q_order)):
                a, b = q_order[i], q_order[j]
                r1, n1 = info.get(a, (np.nan, np.nan))
                r2, n2 = info.get(b, (np.nan, np.nan))

                if (
                    not (isfinite(r1) and isfinite(r2))
                    or (n1 is None)
                    or (n2 is None)
                    or (n1 < 4)
                    or (n2 < 4)
                ):
                    z = np.nan
                    p = np.nan
                else:
                    z1 = _safe_atanh(r1)
                    z2 = _safe_atanh(r2)
                    denom = sqrt(1.0 / (n1 - 3) + 1.0 / (n2 - 3))
                    z = (z1 - z2) / denom if denom > 0 else np.nan
                    p = _p_two_sided_from_z(z)

                pairs.append((a, b, r1, n1, r2, n2, z, p))

        # Multiple-testing adjust per threshold (6 tests)
        pvals = [p for *_, p in pairs]
        holm = _holm_stepdown(pvals)
        fdr = _fdr_bh(pvals)

        # Collect rows
        for (a, b, r1, n1, r2, n2, z, p), p_holm, q_bh in zip(pairs, holm, fdr):
            rows.append(
                {
                    "percentile_threshold": int(N),
                    "quartile_a": a,
                    "quartile_b": b,
                    "r_a": r1,
                    "n_a": n1,
                    "r_b": r2,
                    "n_b": n2,
                    "z_diff": z,
                    "p_raw": p,
                    "p_holm": p_holm,
                    "q_bh": q_bh,
                }
            )

        # Clean up per-threshold variables
        del sub, info, pairs, pvals, holm, fdr
        gc.collect()

    per_threshold = pd.DataFrame.from_records(rows)

    # Clean up rows list
    del rows
    gc.collect()

    # Pretty rounding and p-floor for printing
    printable = per_threshold.copy()
    for col in ["r_a", "r_b", "z_diff", "p_raw", "p_holm", "q_bh"]:
        printable[col] = printable[col].astype(float)

    def _fmt_p(x):
        if not isfinite(x) or pd.isna(x):
            return "n/a"
        return f"<{p_floor:.0e}" if x < p_floor else f"{x:.3g}"

    # ---- PNAS-style console printout ----
    print(
        "\nPairwise Fisher r-to-z tests for quartile correlation lines (cumulative thresholds)"
    )
    print(f"Thresholds used (step={step_filter}%): {thresholds}\n")

    # For each threshold, print the 6 pairwise results (Holm-adjusted)
    for N in thresholds:
        sub = printable[printable["percentile_threshold"] == N].copy()
        if sub.empty:
            continue
        # Optional: show approximate per-quartile n at this N
        ns = {}
        for q in q_order:
            qsub = per_threshold[
                (per_threshold["percentile_threshold"] == N)
                & (
                    (per_threshold["quartile_a"] == q)
                    | (per_threshold["quartile_b"] == q)
                )
            ]
            # pick first available n for that quartile at this N
            n_vals = []
            if not qsub.empty:
                n_vals += qsub["n_a"].dropna().tolist()
                n_vals += qsub["n_b"].dropna().tolist()
            ns[q] = int(max(n_vals)) if n_vals else 0

        # Clean up qsub
        del qsub, n_vals
        gc.collect()

        print(
            f"— Cumulative threshold N={N}% (disruption ≥ {100-N:.0f}th percentile) —"
        )
        print(
            "Quartile sizes (n): " + " / ".join([f"{q}:{ns.get(q,0)}" for q in q_order])
        )

        # Order pairs in a nice fixed way
        sub = (
            sub.set_index(["quartile_a", "quartile_b"])
            .reindex(
                [
                    (q_order[0], q_order[1]),
                    (q_order[0], q_order[2]),
                    (q_order[0], q_order[3]),
                    (q_order[1], q_order[2]),
                    (q_order[1], q_order[3]),
                    (q_order[2], q_order[3]),
                ]
            )
            .reset_index()
        )

        for _, r in sub.iterrows():
            a, b = r["quartile_a"], r["quartile_b"]
            z = r["z_diff"]
            p = r["p_raw"]
            p_h = r["p_holm"]
            q = r["q_bh"]
            ra, rb = r["r_a"], r["r_b"]
            # stars by Holm-adjusted p
            stars = _star(p_h)
            print(
                f"  {a} vs {b}: Δz={z:.3f}, p={_fmt_p(p)}, Holm p={_fmt_p(p_h)} {stars}; "
                f"r_a={ra:.3f}, r_b={rb:.3f}, n_a={int(r['n_a'])}, n_b={int(r['n_b'])}"
            )
        print("")

        # Clean up sub after use
        del sub, ns
        gc.collect()

    # ---- Across-threshold summary (how often pairs are significant after Holm) ----
    print("Across-threshold summary (Holm-adjusted p<0.05):")
    pair_keys = sorted({(a, b) for a in q_order for b in q_order if a < b})
    for a, b in pair_keys:
        sub = per_threshold[
            (per_threshold["quartile_a"] == a) & (per_threshold["quartile_b"] == b)
        ]
        k = int((sub["p_holm"] < 0.05).sum())
        total = sub.shape[0]
        # median correlation difference (r_a - r_b) across thresholds (directional context)
        med_dr = (sub["r_a"] - sub["r_b"]).median()
        print(
            f"  {a} vs {b}: {k}/{total} thresholds significant; median Δr={med_dr:.3f}"
        )
        del sub
        gc.collect()

    # ---- Save tables ----
    csv_path = Path(results_dir) / f"{table_name}.csv"
    per_threshold.to_csv(csv_path, index=False)

    # LaTeX (compact, PNAS-SI style)
    # Limit columns and round for readability
    tex_cols = [
        "percentile_threshold",
        "quartile_a",
        "quartile_b",
        "r_a",
        "r_b",
        "n_a",
        "n_b",
        "z_diff",
        "p_raw",
        "p_holm",
        "q_bh",
    ]
    tex_df = per_threshold[tex_cols].copy()
    for c in ["r_a", "r_b", "z_diff", "p_raw", "p_holm", "q_bh"]:
        tex_df[c] = tex_df[c].astype(float).round(4)
    latex = tex_df.to_latex(
        index=False,
        escape=True,
        caption="Pairwise Fisher r-to-z tests comparing correlation coefficients across quartiles at each cumulative threshold (step = %d). Holm- and BH-adjusted p-values reported per threshold."
        % step_filter,
        label="tab:quartile_line_tests",
    )
    tex_path = Path(results_dir) / f"{table_name}.tex"
    Path(tex_path).write_text(latex)

    print(f"\nSaved CSV: {csv_path}")
    print(f"Saved LaTeX: {tex_path}")

    # Clean up all temporary objects
    del printable, tex_df, latex, pair_keys, res, q_order, thresholds
    gc.collect()

    return per_threshold


def plot_citation_share_by_disruption_quartiles(
    df,
    disruption_col="disruption_percentile",
    citation_col="citation_count",
    band_width=5,  # non-overlapping bands; must divide 100
    quartile_column="first_time_author_ratio_group",  # your 4-bin column (e.g., "0.00-0.25", ...)
    normalize="within_quartile",  # "within_quartile" (alias: "within_group") or "global"
    fig_dir="Figures",
    fig_name=None,
    dpi=300,
    y_pad_frac=0.08,  # ~8% dynamic padding around data range
):
    """
    Non-cumulative plot: x = non-overlapping disruption 'bands' (tail slices),
    y = proportion of citations captured in each band, stratified by author-ratio QUARTILES.

    Bands (for band_width=5):
      N=5  -> [95,100]
      N=10 -> [90,95)
      ...
      N=100-> [0,5]   (topmost includes 100; others are half-open on the right)

    Normalization:
      - "within_quartile" (alias: "within_group"):
          For each quartile q,
            share(q, band) = citations_in_band_from_q / total_citations_of_q.
          Each quartile’s line sums to 1 across all bands. Best for comparing how
          *each quartile* distributes its own citations across disruption levels.
      - "global":
          share(q, band) = citations_in_band_from_q / total_citations_in_dataset.
          Lines sum to the global citation fraction for each quartile; the “Overall”
          (if plotted alone) would sum to 1 across bands.

    Returns
    -------
    result_df : pd.DataFrame with columns
      [threshold, band_low, band_high, quartile, citations_in_band, denom, share]
    """
    setup_plotting_style()
    if 100 % band_width != 0:
        raise ValueError("band_width must divide 100 (e.g., 1,2,4,5,10,20,25,50).")

    # Validate / prepare columns
    if citation_col not in df.columns:
        if "citation_count_percentile" in df.columns:
            print(
                f"[WARN] '{citation_col}' not found; using 'citation_count_percentile' as a proxy."
            )
            citation_col = "citation_count_percentile"
        else:
            raise ValueError(
                f"'{citation_col}' not found and no 'citation_count_percentile' fallback available."
            )

    cols = [disruption_col, citation_col]
    if quartile_column is not None:
        cols.append(quartile_column)

    work = df[cols].copy()
    work[disruption_col] = pd.to_numeric(work[disruption_col], errors="coerce")
    work[citation_col] = pd.to_numeric(work[citation_col], errors="coerce")
    if quartile_column is not None:
        work[quartile_column] = work[quartile_column].astype("category")

    # Normalization setup
    norm_mode = normalize.lower()
    if norm_mode == "within_group":
        norm_mode = "within_quartile"

    if norm_mode == "global":
        denom_global = np.nansum(work[citation_col].to_numpy())
        denom_per_quartile = None
    elif norm_mode == "within_quartile":
        if quartile_column is not None:
            denom_per_quartile = work.groupby(quartile_column, observed=True)[
                citation_col
            ].sum()
        else:
            denom_per_quartile = pd.Series(
                {"Overall": np.nansum(work[citation_col].to_numpy())}
            )
        denom_global = None
    else:
        raise ValueError(
            "normalize must be 'within_quartile' (or 'within_group') or 'global'."
        )

    thresholds = list(range(band_width, 101, band_width))
    rows = []

    for idx, N in enumerate(thresholds, 1):
        low = 100 - N
        high = 100 - (N - band_width)

        # mask for non-overlapping band
        if high >= 100:
            mask = (work[disruption_col] >= low) & (work[disruption_col] <= 100)
        else:
            mask = (work[disruption_col] >= low) & (work[disruption_col] < high)

        band_df = work[mask]

        if quartile_column is not None:
            # per-quartile aggregation
            band_sum = band_df.groupby(quartile_column, observed=True)[
                citation_col
            ].sum()
            all_quarts = work[quartile_column].cat.categories
            band_sum = band_sum.reindex(all_quarts, fill_value=0.0)

            for q, num in band_sum.items():
                if norm_mode == "global":
                    denom = denom_global if denom_global > 0 else np.nan
                else:
                    denom = denom_per_quartile.get(q, np.nan)
                share = (num / denom) if (denom and denom > 0) else np.nan
                rows.append(
                    {
                        "threshold": N,
                        "band_low": low,
                        "band_high": high,
                        "quartile": str(q),
                        "citations_in_band": float(num),
                        "denom": float(denom) if pd.notna(denom) else np.nan,
                        "share": float(share) if pd.notna(share) else np.nan,
                    }
                )
        else:
            # overall only (no quartile stratification)
            num = float(np.nansum(band_df[citation_col].to_numpy()))
            denom = (
                denom_global
                if norm_mode == "global"
                else float(np.nansum(work[citation_col].to_numpy()))
            )
            share = (num / denom) if (denom and denom > 0) else np.nan
            rows.append(
                {
                    "threshold": N,
                    "band_low": low,
                    "band_high": high,
                    "quartile": "Overall",
                    "citations_in_band": num,
                    "denom": denom,
                    "share": float(share) if pd.notna(share) else np.nan,
                }
            )

        # progress
        rb = "]" if high >= 100 else ")"
        print(
            f"[{idx}/{len(thresholds)}] band({band_width}%) non-overlap: N={N}% → rows={len(band_df)}  "
            f"range=[{low:.3f}, {high:.3f}{rb}"
        )
        del band_df
        gc.collect()

    result_df = pd.DataFrame(rows)

    # --- Plot ---
    sns.set(context="talk", style="whitegrid")
    plt.figure(figsize=(12, 7), dpi=dpi)

    # keep category order if available
    if quartile_column is not None and pd.api.types.is_categorical_dtype(
        work[quartile_column]
    ):
        quartiles = [str(q) for q in work[quartile_column].cat.categories]
    else:
        quartiles = sorted(result_df["quartile"].dropna().unique().tolist())

    palette = sns.color_palette("rocket_r", n_colors=len(quartiles))

    for color, q in zip(palette, quartiles):
        qdf = result_df[result_df["quartile"] == q].sort_values("threshold")
        plt.plot(
            qdf["threshold"],
            qdf["share"],
            marker="o",
            linewidth=2,
            markersize=6,
            label=q,
            color=color,
        )

    xlabel = f"Top N% Most Disruptive (Band width = {band_width}%, non-overlap)"
    ylabel = (
        "Proportion of Citations (within quartile)"
        if norm_mode == "within_quartile"
        else "Proportion of Citations (global)"
    )
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title("Citation Share Across Non-overlapping Disruption Tail Bands by Quartile")

    # Auto-scale y-axis to data (light padding, no huge white space)
    yvals = result_df["share"].to_numpy(dtype=float)
    yvals = yvals[np.isfinite(yvals)]
    if yvals.size:
        y_min = max(0.0, yvals.min())
        y_max = yvals.max()
        if np.isfinite(y_min) and np.isfinite(y_max):
            span = max(1e-9, y_max - y_min)
            pad = y_pad_frac * span
            plt.ylim(y_min - 0.25 * pad if y_min > 0 else 0.0, y_max + pad)

    plt.axhline(y=0, color="gray", linestyle="--", alpha=0.4, linewidth=1)
    plt.legend(
        title="Quartile (first-time author ratio)",
        bbox_to_anchor=(1.05, 1),
        loc="upper left",
        fontsize=10,
        title_fontsize=11,
    )

    # x-ticks thinned for readability
    xvals = sorted(result_df["threshold"].unique())
    step = max(1, len(xvals) // 10)
    plt.xticks(xvals[::step])

    ax = plt.gca()
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()

    # save PDF
    Path(fig_dir).mkdir(parents=True, exist_ok=True)
    if fig_name is None:
        norm_tag = "withinQuartile" if norm_mode == "within_quartile" else "global"
        fig_name = f"Final_citation_share_by_disruption_quartiles_band{band_width}_{norm_tag}.pdf"
    out_path = Path(fig_dir) / fig_name
    plt.savefig(out_path, bbox_inches="tight", dpi=dpi)
    print(f"\nFigure saved to {out_path} (dpi={dpi})")

    return result_df


def main():
    if not os.path.exists("Final_Figures"):
        os.makedirs("Final_Figures")

    setup_plotting_style()

    final_df = load_full_disruption_data()

    ################### Teams with higher beginner-author ratios are more disruptive and innovative ###################
    print(
        "Section: Teams with higher beginner-author ratios are more disruptive and innovative"
    )
    print("Correlation between first_time_author_ratio and disruption:")
    corr_result = find_correlation_coefficient(
        final_df, "first_time_author_ratio", "disruption_percentile"
    )
    print(corr_result)
    del corr_result
    gc.collect()

    print("Correlation between early_career_author_ratio and disruption:")
    corr_result = find_correlation_coefficient(
        final_df, "early_career_author_ratio", "disruption_percentile"
    )
    print(corr_result)
    del corr_result
    gc.collect()

    print("Correlation between senior_author_ratio and disruption:")
    corr_result = find_correlation_coefficient(
        final_df, "senior_author_ratio", "disruption_percentile"
    )
    print(corr_result)
    del corr_result
    gc.collect()

    # All Career-Age Ratios vs Disruption Percentile in Grid:
    fig, axes = plot_team_size_by_career_ratios_grid(
        df=final_df,
        target_column="disruption_percentile",
        save_path="Final_Figures/Final_All_Career-Age_Ratio_And_Disruption.pdf",
    )
    plt.close(fig)
    del fig, axes
    gc.collect()

    # Kendall's Tau Table with CIs
    res_df, latex = kendall_tau_to_pnas_si_table_with_ci(
        df=final_df,
        target_column="disruption_percentile",
        min_group_size=5,
        min_sample_threshold=50,
        n_bins=10,
        binning_method="equal",  # or "quantile"
        correction="fdr_bh",  # "fdr_bh", "bonferroni", or None
        save_path="Final_Figures/Final_Kendall_Tau_Table_withCI.tex",
        add_ci=True,
        ci_level=95,
        n_boot=2000,
        random_state=123,
    )
    del res_df, latex
    gc.collect()

    ### Beginner Author Ratio vs Disruption Across Team Sizes ####
    corr_result = find_correlation_coefficient(
        final_df, "first_time_author_ratio", "disruption_percentile", "team_size"
    )
    print(corr_result)
    del corr_result
    gc.collect()

    fig, ax, stats_df = plot_disruption_by_team_size(
        final_df,
        target_column="disruption_percentile",
        team_size_column="team_size",
        min_group_size=100,
        save_path="Final_Figures/Final_First_Time_Author_and_Disruption_by_team_size.pdf",
    )
    plt.close(fig)
    del fig, ax, stats_df
    gc.collect()

    ### Beginner Author Ratio vs Disruption Across Decades ####
    corr_result = find_correlation_coefficient(
        final_df, "first_time_author_ratio", "disruption_percentile", "decade"
    )
    print(corr_result)
    del corr_result
    gc.collect()

    fig, axes = plot_firsttime_authors_by_decade_grid(
        df=final_df,
        ylim=(20, 95),
        target_column="disruption_percentile",
        decade_column="decade",
        save_path="Final_Figures/Final_Disruption_by_First_Time_Author_Ratio_decades.pdf",
    )
    plt.close(fig)
    del fig, axes
    gc.collect()

    ########### Atypical Combination ##################
    print("Section: Atypical Combination")
    final_df = bin_first_time_author_ratio(
        final_df,
        ratio_col="first_time_author_ratio",
        out_col="first_time_author_ratio_group",
        k=4,
    )

    final_df["Atyp_Median_Z"] = final_df["Atyp_Median_Z"].astype(float)
    print("Plotting Atypical Combination CDFs by First-Time Author Ratio Groups")
    fig, ax = plot_atyp_combination(
        final_df,
        group_column="first_time_author_ratio_group",
        save_path="Final_Figures/Final_atyp_combination_4bins.pdf",
    )
    plt.close(fig)
    del fig, ax
    gc.collect()

    print("Plotting correlation between first_time_author_ratio and Atyp_Median_Z")
    fig, axes = plot_team_size_by_career_ratios_grid(
        df=final_df,
        target_column="Atyp_Median_Z_percentile",
        ylim=(0, 100),
        save_path="Final_Figures/Final_All_Career-Age_Ratio_And_Atyp_Z.pdf",
    )
    plt.close(fig)
    del fig, axes
    gc.collect()

    ks_result = run_ks_tests_pairwise(
        final_df,
        group_col="first_time_author_ratio_group",
        value_col="Atyp_Median_Z",
        results_dir="Final_Figures",
        table_name="Final_KS_Atyp_Median_Z_k4",
    )
    del ks_result
    gc.collect()

    res_df, latex = kendall_tau_to_pnas_si_table_with_ci(
        df=final_df,
        target_column="avg_reference_popularity_percentile",
        min_group_size=5,
        min_sample_threshold=50,
        n_bins=10,
        binning_method="equal",  # or "quantile"
        correction="fdr_bh",  # "fdr_bh", "bonferroni", or None
        save_path="Final_Figures/Final_Kendall_Tau_Table_withCI_referencepop.tex",
        add_ci=True,
        ci_level=95,
        n_boot=2000,
        random_state=123,
    )
    del res_df, latex
    gc.collect()

    res_df, latex = kendall_tau_to_pnas_si_table_with_ci(
        df=final_df,
        target_column="avg_reference_age_percentile",
        min_group_size=5,
        min_sample_threshold=50,
        n_bins=10,
        binning_method="equal",  # or "quantile"
        correction="fdr_bh",  # "fdr_bh", "bonferroni", or None
        save_path="Final_Figures/Final_Kendall_Tau_Table_withCI_referenceage.tex",
        add_ci=True,
        ci_level=95,
        n_boot=2000,
        random_state=123,
    )
    del res_df, latex
    gc.collect()

    ######### Early-Career and disruptive collaborators are linked to greter disruption in beginner-heavy teams ######
    print(
        "Section: Early-Career and disruptive collaborators are linked to greater disruption in beginner-heavy teams"
    )
    fig, axes, pivot_dfs_disruption = plot_expanded_disruption_heatmaps(
        final_df,
        target_column="disruption_percentile",
        main_columns=[
            "first_time_author_ratio",
            "early_career_author_ratio",
            "senior_author_ratio",
        ],
        cmap="Purples",
        min_group_size=100,
        save_path="Final_Figures/Final_Disruption_heatmaps_by_author_ratios.pdf",
    )
    plt.close(fig)
    del fig, axes
    gc.collect()

    plot_configs = [
        {
            "x_column": "first_time_author_ratio",
            "y_column": "early_career_author_ratio",
        },  # Panel A
        {
            "x_column": "first_time_author_ratio",
            "y_column": "senior_author_ratio",
        },  # Panel B
        {
            "x_column": "early_career_author_ratio",
            "y_column": "senior_author_ratio",
        },  # Panel C
    ]

    matrix_tables = pnas_si_matrix_tables_from_heatmaps(
        pivot_dfs_disruption,
        plot_configs,
        method="median",
        target_column="disruption_percentile",
        caption_prefix="Disruption percentiles by author-ratio bins.",
        label_prefix="tab:si-disruption-matrix",
        value_fmt="{:.2f}",
        compress_counts=True,
        na_text="--",
        use_makecell=True,
        table_env="table",
        font_size_cmd="\\scriptsize",
    )

    for i, tex in enumerate(matrix_tables, 1):
        with open(f"Final_Figures/Final_disruption_matrix_panels_{i}.tex", "w") as f:
            f.write(tex)

    del pivot_dfs_disruption, plot_configs, matrix_tables
    gc.collect()

    fig, axes, pivot_dfs = plot_expanded_disruption_heatmaps(
        final_df,
        target_column="Atyp_Median_Z",
        main_columns=[
            "first_time_author_ratio",
            "early_career_author_ratio",
            "senior_author_ratio",
        ],
        cmap="Purples",
        min_group_size=100,
        save_path="Final_Figures/Final_Atyp_Median_Z_heatmaps_by_author_ratios.pdf",
    )
    plt.close(fig)
    del fig, axes, pivot_dfs
    gc.collect()

    coauthor_groups_to_plot = [
        "60-70 percentile",
        "70-80 percentile",
        "80-90 percentile",
        "90-100 percentile",
    ]

    fig, axes = plot_firsttime_authors_by_coauthor_disruption(
        df=final_df,
        target_column="disruption_percentile",
        coauthor_group_column="co_authors_disruption_group",
        coauthor_groups_to_plot=coauthor_groups_to_plot,
        ylim=(30, 95),
        save_path="Final_Figures/Final_Disruption_by_First_Time_Author_Ratio_accross_coauthor_Disruption.pdf",
    )
    plt.close(fig)
    del fig, axes, coauthor_groups_to_plot
    gc.collect()

    top_combination_based_on_median = analyze_author_count_combinations(
        final_df, 10000, method="median"
    )
    top_combination_based_on_mean = analyze_author_count_combinations(
        final_df, 10000, method="mean"
    )

    print("Total Number of Combinations: ", top_combination_based_on_mean.shape[0])

    create_team_disruption_svg_direct(
        top_combination_based_on_mean,
        output_filename="Final_Figures/Final_team_disruption_based_on_mean.svg",
        top_n=25,
        method="mean",
    )

    create_team_disruption_svg_direct(
        top_combination_based_on_median,
        output_filename="Final_Figures/Final_team_disruption_based_on_median.svg",
        top_n=25,
        method="median",
    )

    top_50_mean = top_combination_based_on_mean.head(50)
    top_50_mean["other_author_count"] = (
        top_50_mean["early_career_author_count"] + top_50_mean["senior_author_count"]
    )

    count = (
        top_50_mean["first_time_author_count"] >= top_50_mean["other_author_count"]
    ).sum()
    print("Count where beginner_author_count >= other_author_count:", count)

    count = (
        top_50_mean["first_time_author_count"] > top_50_mean["other_author_count"]
    ).sum()
    print("Count where beginner_author_count > other_author_count:", count)

    del (
        top_combination_based_on_median,
        top_combination_based_on_mean,
        top_50_mean,
        count,
    )
    gc.collect()

    ######## Highly disruptive papers by beginner-heavy teams are highly cited
    fig, axes = plot_team_size_by_career_ratios_grid(
        df=final_df,
        target_column="C10_percentile",
        ylim=(0, 100),
        save_path="Final_Figures/Final_All_Career-Age_Ratio_And_C10.pdf",
    )
    plt.close(fig)
    del fig, axes
    gc.collect()

    compute_correlations_to_disk(
        final_df,
        out_dir="intermediate_data",
        min_rows_for_percentile=10,
        min_rows_per_group=5,
        overwrite=True,
        band_width=10,
        corr_method="spearman",
    )
    gc.collect()

    load_results_and_plot_raiyan(
        out_dir="intermediate_data", fig_dir="Final_Figures", band_width=10
    )
    gc.collect()

    tests_df = run_quartile_line_tests_cumulative(
        out_dir="intermediate_data",
        results_dir="Final_Figures",
        table_name="Final_quartile_line_tests_step10",
        expected_quartiles=("0.00 - 0.25", "0.25 - 0.50", "0.50 - 0.75", "0.75 - 1.00"),
        step_filter=10,
        p_floor=1e-300,
    )
    del tests_df
    gc.collect()

    res_quartiles = plot_citation_share_by_disruption_quartiles(
        final_df,
        disruption_col="disruption_percentile",
        citation_col="citation_count",
        band_width=10,
        quartile_column=None,
        normalize="global",
        fig_dir="Final_Figures",
    )
    del res_quartiles
    gc.collect()

    # Final cleanup
    del final_df
    gc.collect()

    print("\n=== Analysis Complete ===")


if __name__ == "__main__":
    main()
