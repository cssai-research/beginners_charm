import gc
import time
import pandas as pd
import numpy as np
import os
import warnings
import matplotlib.pyplot as plt
import seaborn as sns
import seaborn as sns
from matplotlib.colors import Normalize
from scipy.stats import pearsonr, spearmanr, kendalltau
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
        "mid_career_author_count",
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

    print(f"Starting year filtering... Keeping articles from {START_YEAR}")
    df = df[df["year"] >= START_YEAR]

    df.drop(
        columns=[
            "decade_start",
            "decade_end",
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

    df["mid_career_disruption_percentile"] = df["mid_author_avg_disruption"].apply(
        lambda x: find_percentile_in_reference(x, avg_disruption_sorted)
    )

    df["senior_author_disruption_bucket"] = df[
        "senior_author_disruption_percentile"
    ].apply(percentile_group)

    df["early_career_disruption_bucket"] = df[
        "early_career_disruption_percentile"
    ].apply(percentile_group)

    df["mid_career_disruption_bucket"] = df["mid_career_disruption_percentile"].apply(
        percentile_group
    )

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
            "mid_career_author_ratio",
            "senior_author_ratio",
        ]

    # Create a 2x4 grid of subplots
    fig, axes = plt.subplots(2, 4, figsize=(24, 12), sharex=True, sharey=True, dpi=300)
    axes_flat = axes.flatten()

    # Define color palette and markers for 4 groups
    full_palette = sns.color_palette("viridis_r", 8)
    color_indices = [0, 1, 3, 5]
    selected_colors = [full_palette[i] for i in color_indices]
    colors = {col: selected_colors[i] for i, col in enumerate(career_columns)}

    markers = {
        "first_time_author_ratio": "o",
        "early_career_author_ratio": "s",
        "mid_career_author_ratio": "^",
        "senior_author_ratio": "d",
    }

    labels = {
        "first_time_author_ratio": "Beginner Authors",
        "early_career_author_ratio": "Early-Career Authors",
        "mid_career_author_ratio": "Mid-Career Authors",
        "senior_author_ratio": "Senior Authors",
    }

    # Create plots for each team size - 2x4 grid (8 total plots)
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
                markersize=10,
                linewidth=2.5,
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
    caption="Kendall's $\\tau$ between author ratio and disruption percentile by team size and career stage.",
    label="tab:kendall_team_career",
    save_path="Figures/SI_Kendall_Tau_Table.tex",
    # CI controls:
    add_ci=True,
    ci_level=95,
    n_boot=2000,
    random_state=42,
):
    """
    Computes Kendall's τ per subplot (team size × career line) with the same binning
    used in your plotting function, applies multiple-testing correction to p-values,
    and outputs a PNAS SI-style LaTeX table where each cell shows:

        τ [CI_low, CI_high]***   (stars based on adjusted p-values)

    CI is a percentile bootstrap over the binned points (x = ratio midpoint, y = median target).
    """
    if career_columns is None:
        career_columns = [
            "first_time_author_ratio",
            "early_career_author_ratio",
            "mid_career_author_ratio",
            "senior_author_ratio",
        ]

    pretty_cols = {
        "first_time_author_ratio": "Beginner",
        "early_career_author_ratio": "Early-career",
        "mid_career_author_ratio": "Mid-career",
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

    # Pivot to wide: rows are team sizes; columns are the four careers
    order_rows = [str(i) for i in range(1, 8)] + ["8+"]
    order_cols = ["Beginner", "Early-career", "Mid-career", "Senior"]
    wide = res.pivot(index="team_size", columns="career", values="cell").reindex(
        index=order_rows, columns=order_cols
    )
    wide = wide.fillna("--")

    # Build LaTeX using the provided PNAS SI template
    lines = []
    lines.append("\\begin{table}\\centering")
    lines.append(f"\\caption{{{caption}}}")
    lines.append("\\begin{tabular}{lrrrr}")
    lines.append("Team size & Beginner & Early-career & Mid-career & Senior \\\\")
    lines.append("\\midrule")
    for ts in order_rows:
        if ts not in wide.index:
            lines.append(f"{ts} & -- & -- & -- & -- \\\\")
        else:
            row = wide.loc[ts]
            lines.append(
                f"{ts} & {row['Beginner']} & {row['Early-career']} & {row['Mid-career']} & {row['Senior']} \\\\"
            )
    lines.append("\\bottomrule")
    lines.append("\\end{tabular}")
    lines.append("\\vspace{0.25em}")
    lines.append(
        "\\footnotesize{Entries are Kendall's $\\tau$ with bootstrap "
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


def plot_expanded_disruption_heatmaps(
    df,
    target_column="disruption_percentile",
    main_columns=[
        "first_time_author_ratio",
        "early_career_author_ratio",
        "mid_career_author_ratio",
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
    Create a figure with six subplots showing disruption values for different combinations of author ratios.
    Two rows: First-time authors vs career stages (row 1), and career stages compared against each other (row 2)
    """
    setup_plotting_style_mahdee()

    if method not in ["mean", "median"]:
        raise ValueError("Method must be either 'mean' or 'median'")

    # Close any existing figures
    plt.close("all")

    # Create figure and subplots (2 rows, 3 columns for 6 combinations)
    fig, axes = plt.subplots(2, 3, figsize=(36, 24), dpi=300)

    # Extract columns for easier reference
    fresh_ratio = main_columns[0]
    career_stages = main_columns[1:4]  # early, mid, senior

    # Define all axis pairs we want to plot (6 combinations total)
    plot_configs = [
        # Row 1: first_time_author_ratio (x-axis) vs each career stage (y-axis)
        {"x_column": fresh_ratio, "y_column": career_stages[0]},  # Fresh vs Early
        {"x_column": fresh_ratio, "y_column": career_stages[1]},  # Fresh vs Mid
        {"x_column": fresh_ratio, "y_column": career_stages[2]},  # Fresh vs Senior
        # Row 2: Career stages compared to each other
        {"x_column": career_stages[0], "y_column": career_stages[1]},  # Early vs Mid
        {"x_column": career_stages[0], "y_column": career_stages[2]},  # Early vs Senior
        {"x_column": career_stages[1], "y_column": career_stages[2]},  # Mid vs Senior
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
        row = i // 3
        col = i % 3
        ax = axes[row, col]

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
            norm=norm,
        )

        # Set tick positions and labels
        ax.set_xticks(np.arange(len(x_centers)))
        ax.set_yticks(np.arange(len(y_centers)))
        ax.set_xticklabels(x_labels)
        ax.set_yticklabels(y_labels)

        # Rotate the tick labels and set their alignment
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")

        # Add labels
        x_label_display = (
            "Beginner Author Ratio"
            if x_column == "first_time_author_ratio"
            else x_column.replace("_", " ").title()
        )
        y_label_display = (
            "Beginner Author Ratio"
            if y_column == "first_time_author_ratio"
            else y_column.replace("_", " ").title()
        )

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
                            j + 0.15,
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
                                j - 0.15,
                                f"n={count_text}",
                                ha="center",
                                va="center",
                                color="black",
                                fontsize=6,
                                alpha=0.8,
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
    cbar_ax = fig.add_axes([0.92, 0.25, 0.015, 0.5])
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

    # Adjust spacing
    fig.subplots_adjust(
        left=0.05, right=0.90, bottom=0.08, top=0.92, wspace=0.3, hspace=0.3
    )

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
    use_makecell=True,
    table_env="table",
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


def analyze_author_count_combinations(df, min_count=10, method="median"):
    """
    Analyze combinations of author counts and their relationship to disruption percentiles.

    This function:
    1. Groups by all 4 author count columns (no binning)
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

    # Define the author count columns (now 4 groups)
    count_columns = [
        "first_time_author_count",
        "early_career_author_count",
        "mid_career_author_count",
        "senior_author_count",
    ]

    # Step 1: Group by all 4 count columns
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
        "mid_career_author_count": stats["mid_career_author_count"],
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

    # Add team_size column (now includes mid-career)
    combination_df["team_size"] = (
        combination_df["first_time_author_count"]
        + combination_df["early_career_author_count"]
        + combination_df["mid_career_author_count"]
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
        "mid_career_author_count",
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
    output_filename="MidCareer_Figures/Final_team_disruption.svg",
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

    # Add team_size column if not present (now includes mid-career)
    if "team_size" not in plot_data.columns:
        plot_data["team_size"] = (
            plot_data["first_time_author_count"]
            + plot_data["early_career_author_count"]
            + plot_data["mid_career_author_count"]
            + plot_data["senior_author_count"]
        )

    # Set up dimensions and margins
    width = 1000
    height = 800
    margin = {"top": 40, "right": 120, "bottom": 60, "left": 200}
    graph_width = width - margin["left"] - margin["right"]
    graph_height = height - margin["top"] - margin["bottom"]

    # Define career columns and map colors using viridis_r palette (now 4 groups)
    career_columns = [
        "first_time_author_count",
        "early_career_author_count",
        "mid_career_author_count",
        "senior_author_count",
    ]

    full_palette = sns.color_palette("viridis_r", 8)
    color_indices = [0, 1, 3, 5]
    selected_colors = [mcolors.to_hex(full_palette[i]) for i in color_indices]
    colors = {col: selected_colors[i] for i, col in enumerate(career_columns)}

    # Emoji mapping (now 4 groups)
    emoji_map = {
        "first_time_author_count": "🌱",
        "early_career_author_count": "🚀",
        "mid_career_author_count": "⭐",
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
                start_x += segment_width

        svg_content += f"""
        <text class="bar-label" x="{margin['left'] + bar_width + 5}" y="{y_position}" dy="0.35em">
            {row[disruption_col]:.2f} <tspan fill="#888">n = {row['N_Records']:,}</tspan>
        </text>
        """

    # Legend (now 4 groups)
    legend_y = height / 2 - margin["bottom"]
    legend_x = width - margin["right"] + 10

    svg_content += f"""
    <rect x="{legend_x - 10}" y="{legend_y - 10}" width="130" height="120" fill="white" stroke="#ddd" />
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


def main():
    if not os.path.exists("MidCareer_Figures"):
        os.makedirs("MidCareer_Figures")

    setup_plotting_style()

    final_df = load_full_disruption_data()

    # Keep only necessary columns to reduce memory usage
    necessary_columns = [
        # Career ratios
        "first_time_author_ratio",
        "early_career_author_ratio",
        "mid_career_author_ratio",
        "senior_author_ratio",
        # Career counts
        "first_time_author_count",
        "early_career_author_count",
        "mid_career_author_count",
        "senior_author_count",
        # Team info
        "team_size",
        # Target variables
        "disruption",
        "disruption_percentile",
        "C10",
        "c10_percentile",
        "Atyp_Median_Z",
        # Disruption percentiles by career stage
        "senior_author_disruption_percentile",
        "early_career_disruption_percentile",
        "mid_career_disruption_percentile",
        # Additional analysis columns (if needed)
        "avg_disruption",
        "avg_disruption_percentile",
        "avg_citation_count",
        "avg_citation_count_percentile",
    ]

    # Filter to only necessary columns
    final_df = final_df[necessary_columns]
    gc.collect()

    print(f"Memory optimized. Columns kept: {len(final_df.columns)}")
    print(f"DataFrame shape: {final_df.shape}")

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

    print("Correlation between mid_career_author_ratio and disruption:")
    corr_result = find_correlation_coefficient(
        final_df, "mid_career_author_ratio", "disruption_percentile"
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
        save_path="MidCareer_Figures/MidCareer_All_Career-Age_Ratio_And_Disruption.pdf",
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
        binning_method="equal",
        correction="fdr_bh",
        save_path="MidCareer_Figures/MidCareer_Kendall_Tau_Table_withCI.tex",
        add_ci=True,
        ci_level=95,
        n_boot=2000,
        random_state=123,
    )
    del res_df, latex
    gc.collect()

    ######### Early-Career and disruptive collaborators are linked to greater disruption in beginner-heavy teams ######
    print(
        "Section: Early-Career and disruptive collaborators are linked to greater disruption in beginner-heavy teams"
    )
    fig, axes, pivot_dfs_disruption = plot_expanded_disruption_heatmaps(
        final_df,
        target_column="disruption_percentile",
        main_columns=[
            "first_time_author_ratio",
            "early_career_author_ratio",
            "mid_career_author_ratio",
            "senior_author_ratio",
        ],
        cmap="Purples",
        min_group_size=100,
        save_path="MidCareer_Figures/Final_Disruption_heatmaps_by_author_ratios.pdf",
    )
    plt.close(fig)
    del fig, axes
    gc.collect()

    plot_configs = [
        {
            "x_column": "first_time_author_ratio",
            "y_column": "early_career_author_ratio",
        },
        {
            "x_column": "first_time_author_ratio",
            "y_column": "mid_career_author_ratio",
        },
        {
            "x_column": "first_time_author_ratio",
            "y_column": "senior_author_ratio",
        },
        {
            "x_column": "early_career_author_ratio",
            "y_column": "mid_career_author_ratio",
        },
        {
            "x_column": "early_career_author_ratio",
            "y_column": "senior_author_ratio",
        },
        {
            "x_column": "mid_career_author_ratio",
            "y_column": "senior_author_ratio",
        },
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
        with open(
            f"MidCareer_Figures/Final_disruption_matrix_panels_{i}.tex", "w"
        ) as f:
            f.write(tex)

    del pivot_dfs_disruption, plot_configs, matrix_tables
    gc.collect()

    fig, axes, pivot_dfs = plot_expanded_disruption_heatmaps(
        final_df,
        target_column="Atyp_Median_Z",
        main_columns=[
            "first_time_author_ratio",
            "early_career_author_ratio",
            "mid_career_author_ratio",
            "senior_author_ratio",
        ],
        cmap="Purples",
        min_group_size=100,
        save_path="MidCareer_Figures/Final_Atyp_Median_Z_heatmaps_by_author_ratios.pdf",
    )
    plt.close(fig)
    del fig, axes, pivot_dfs
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
        output_filename="MidCareer_Figures/Final_team_disruption_based_on_mean.svg",
        top_n=25,
        method="mean",
    )

    create_team_disruption_svg_direct(
        top_combination_based_on_median,
        output_filename="MidCareer_Figures/Final_team_disruption_based_on_median.svg",
        top_n=25,
        method="median",
    )

    top_50_mean = top_combination_based_on_mean.head(50)
    top_50_mean["other_author_count"] = (
        top_50_mean["early_career_author_count"]
        + top_50_mean["mid_career_author_count"]
        + top_50_mean["senior_author_count"]
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

    # Final cleanup
    del final_df
    gc.collect()

    print("\n=== Analysis Complete ===")


if __name__ == "__main__":
    main()
