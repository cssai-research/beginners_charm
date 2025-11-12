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

        print(f"⏱️  {func.__name__}() took {time_str}")

        return result

    return wrapper


@timer
def load_full_disruption_data(
    filepath="data/disruption_analysis.csv", chunksize=100000
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
        "field_name",
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
    df = df[df["year"] >= 1901]

    df = df[
        ~((df["first_time_author_ratio"] == 1) & (df["avg_career_age"] > 0))
        & ~(
            (df["first_time_author_ratio"] == 1)
            & (df["senior_author_avg_disruption"] > 0)
        )
        & ~((df["first_time_author_ratio"] == 1) & (df["mid_career_author_ratio"] > 0))
        & ~(
            (df["first_time_author_ratio"] == 1)
            & (df["early_author_avg_disruption"] > 0)
        )
    ]

    df["year"] = df["year"].astype(int)
    df["decade_start"] = ((df["year"] - 1) // 10) * 10 + 1
    df["decade_end"] = df["decade_start"] + 9
    df["decade"] = df["decade_start"].astype(str) + "-" + df["decade_end"].astype(str)

    df["early_career_author_ratio"] = (
        df["early_career_author_ratio"] + df["mid_career_author_ratio"]
    )

    df["early_author_avg_disruption"] = np.where(
        df["early_author_avg_disruption"].isna()
        | df["mid_author_avg_disruption"].isna(),
        np.nan,
        (df["early_author_avg_disruption"] + df["mid_author_avg_disruption"]) / 2,
    )

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


def setup_plotting_style():
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
        f"Impact of Beginner Author Ratio on {ylabel_map.get(target_column, target_column)} Across Fields",
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


def bin_first_time_author_ratio(
    df,
    ratio_col="first_time_author_ratio",
    out_col="first_time_author_ratio_group",
    k=10,
):
    """
    Create K equal-width bins over [0,1] for the ratio column and write labels like '0.00 - 0.10'.
    Ensures 0 goes into the first bin and 1 goes into the last bin (include_lowest=True, right=True).
    """
    df = df.copy()
    df[ratio_col] = df[ratio_col].astype(float).clip(0.0, 1.0)

    # Equal-width bin edges
    bins = np.linspace(0.0, 1.0, k + 1)
    # Labels like '0.00 - 0.10'
    labels = [f"{bins[i]:.2f} - {bins[i+1]:.2f}" for i in range(k)]

    df[out_col] = pd.cut(
        df[ratio_col],
        bins=bins,
        labels=labels,
        include_lowest=True,  # includes 0 in first bin
        right=True,  # includes the right edge; 1.00 goes into last bin
    )
    return df


def plot_atyp_combination(
    df,
    group_column="first_time_author_ratio_group",
    save_path="Figures/Sup_4_atyp_combination.pdf",
):
    gc.collect()
    fig, ax = plt.subplots(figsize=(10, 6), dpi=300)
    gc.collect()

    # Use the categorical order (so legend/order matches the bins)
    if pd.api.types.is_categorical_dtype(df[group_column]):
        hue_order = list(df[group_column].cat.categories)
    else:
        hue_order = sorted(df[group_column].dropna().unique().tolist())

    sns.ecdfplot(
        data=df,
        x="atyp_median_z",
        hue=group_column,
        hue_order=hue_order,
        ax=ax,
        palette="flare",
    )
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

    # Annotations + shading
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
    ax.axvspan(xmin=df["atyp_median_z"].min(), xmax=0, facecolor="lightblue", alpha=0.1)
    ax.axvspan(xmin=0, xmax=df["atyp_median_z"].max(), facecolor="pink", alpha=0.05)

    # Save high-res PDF
    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, bbox_inches="tight", dpi=300)

    gc.collect()
    return fig, ax


def run_ks_tests_pairwise(
    df,
    group_col="first_time_author_ratio_group",
    value_col="atyp_median_z",
    results_dir="Results",
    table_name="PNAS_KS_atyp_median_z_k3",
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


if __name__ == "__main__":
    if not os.path.exists("Final_Figures"):
        os.makedirs("Final_Figures")

    setup_plotting_style()

    final_df = load_full_disruption_data()

    ################### Teams with higher beginner-author ratios are more disruptive and innovative ###################
    print(
        "Section: Teams with higher beginner-author ratios are more disruptive and innovative"
    )
    print("Correlation between first_time_author_ratio and disruption:")
    print(
        find_correlation_coefficient(
            final_df, "first_time_author_ratio", "disruption_percentile"
        )
    )

    print("Correlation between early_career_author_ratio and disruption:")
    print(
        find_correlation_coefficient(
            final_df, "early_career_author_ratio", "disruption_percentile"
        )
    )

    print("Correlation between senior_author_ratio and disruption:")
    print(
        find_correlation_coefficient(
            final_df, "senior_author_ratio", "disruption_percentile"
        )
    )

    # All Career-Age Ratios vs Disruption Percentile in Grid:
    fig, axes = plot_team_size_by_career_ratios_grid(
        df=final_df,
        target_column="disruption_percentile",
        save_path="Final_Figures/Final_All_Career-Age_Ratio_And_Disruption.pdf",
    )

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
    gc.collect()

    ### Beginner Author Ratio vs Disruption Across Team Sizes ####
    print(
        find_correlation_coefficient(
            final_df, "first_time_author_ratio", "disruption_percentile", "team_size"
        )
    )
    gc.collect()

    fig, ax, stats_df = plot_disruption_by_team_size(
        final_df,
        target_column="disruption_percentile",
        team_size_column="team_size",
        min_group_size=100,
        save_path="Final_Figures/Final_First_Time_Author_and_Disruption_by_team_size.pdf",
    )
    gc.collect()

    ### Beginner Author Ratio vs Disruption Across Decades ####
    print(
        find_correlation_coefficient(
            final_df, "first_time_author_ratio", "disruption_percentile", "decade"
        )
    )
    gc.collect()

    fig, axes = plot_firsttime_authors_by_decade_grid(
        df=final_df,
        ylim=(20, 95),
        target_column="disruption_percentile",
        decade_column="decade",
        save_path="Final_Figures/Final_Disruption_by_First_Time_Author_Ratio_decades.pdf",
    )
    del fig, axes
    gc.collect()

    #### Beginner Author Ratio vs Disruption Across Fields ####
    out_df = find_correlation_coefficient(
        final_df, "first_time_author_ratio", "disruption_percentile", "field_name"
    )
    print(out_df)

    # Baten Sir Addition
    num_cols = ["N", "Pearson_r", "Pearson_p"]
    out_df[num_cols] = out_df[num_cols].apply(pd.to_numeric, errors="coerce")
    alpha = 1  # significance threshold

    # 1) sort by N, 2) keep N>2k and significant Pearson p, 3) sort by Pearson r (desc)
    result = (
        out_df.sort_values("N", ascending=False)
        .loc[lambda d: (d["N"] > 2000) & (d["Pearson_p"] < alpha)]
        .sort_values("Pearson_r", ascending=False)
        .reset_index(drop=True)
    )
    result.to_csv("out_df_filtered.csv", index=False)

    fields_to_plot = [
        "Geology",
        "Finance",
        "Artificial intelligence",
        "Physics",
        "Advertising",
        "Data mining",
        "Oceanography",
        "Psychology",
        "Animal science",
        "Gynecology",
        "Biotechnology",
        "Virology",
    ]
    first_row = out_df.iloc[[0]]

    # 2) filter remaining rows to the desired groups
    rest = out_df.iloc[1:]
    rest = rest[rest["Group"].isin(fields_to_plot)].copy()

    # (optional) if there can be multiple rows per Group, keep first occurrence only
    # rest = rest.drop_duplicates(subset="Group", keep="first")

    # 3) order the filtered groups exactly as in fields_to_plot
    rest["Group"] = pd.Categorical(
        rest["Group"], categories=fields_to_plot, ordered=True
    )
    rest = rest.sort_values("Group")

    # 4) combine back
    filtered_df = pd.concat([first_row, rest], ignore_index=True)
    print(filtered_df)

    # Baten Sir Addition Ends

    fig, axes = plot_firsttime_authors_by_field_grid(
        df=final_df,
        target_column="disruption_percentile",
        field_column="field_name",
        fields_to_plot=fields_to_plot,
        save_path="Final_Figures/Final_Disruption_by_First_Time_Author_Ratio_fields_1-8.pdf",
    )
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

    final_df["atyp_median_z"] = final_df["Atyp_Median_Z"].astype(float)

    fig, ax = plot_atyp_combination(
        final_df,
        group_column="first_time_author_ratio_group",
        save_path="Final_Figures/Final_atyp_combination_4bins.pdf",
    )
    del fig, ax
    gc.collect()

    fig, axes = plot_team_size_by_career_ratios_grid(
        df=final_df,
        target_column="Atyp_Median_Z_percentile",
        ylim=(0, 100),
        save_path="Final_Figures/Final_All_Career-Age_Ratio_And_Atyp_Z.pdf",
    )
    plt.close(fig)
    del fig, axes
    gc.collect()

    _ = run_ks_tests_pairwise(
        final_df,
        group_col="first_time_author_ratio_group",
        value_col="atyp_median_z",
        results_dir="Final_Figures",
        table_name="Final_KS_atyp_median_z_k4",
    )
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
