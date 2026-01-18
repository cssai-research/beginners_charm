import gc
import os
import warnings
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from perform_statistical_analysis import (
    load_full_disruption_data,
    find_correlation_coefficient,
    setup_plotting_style,
    timer,
)


warnings.filterwarnings("ignore", category=RuntimeWarning)

# Set pandas options for memory efficiency
pd.options.mode.chained_assignment = None
pd.set_option("mode.copy_on_write", True)


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
    Create a 2x4 grid of subplots, each showing the relationship between First-Time Author Ratio
    and disruption score across different fields for different team sizes.
    Using raw data points for team sizes 1-7, binning only for team size 8+
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

    colors = sns.color_palette("tab20", n_colors=len(fields))
    field_colors = {field: colors[i] for i, field in enumerate(fields)}

    markers = {field: "o" for field in fields}

    # Modified for 2x4 grid: team sizes 1-7 individually, then "8+"
    if team_sizes is None:
        team_sizes = list(range(1, 8)) + ["8+"]

    needed_columns = [author_ratio_column, target_column, "team_size", field_column]
    df_slim = df[needed_columns].copy()
    df_slim[author_ratio_column] = df_slim[author_ratio_column].round(3)
    gc.collect()

    for idx, team_size in enumerate(team_sizes):
        ax = axes_flat[idx]

        # Handle the special case for "8+" (last cell)
        if team_size == "8+":
            subset = df_slim[df_slim["team_size"] >= 8].copy()
            title_text = "Team Size: 8+"
            use_binning = True
        else:
            subset = df_slim[df_slim["team_size"] == team_size].copy()
            title_text = f"Team Size: {team_size}"
            use_binning = False  # NO BINNING for team sizes 1-7

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

            if use_binning:
                # Handle zero values separately
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

                # Handle values between 0 and 1
                values = field_subset[
                    (field_subset[author_ratio_column] > 0)
                    & (field_subset[author_ratio_column] < 1)
                ][[author_ratio_column, target_column]].dropna()
                if len(values) == 0:
                    del values
                    gc.collect()
                else:
                    # Binning
                    if binning_method == "equal":
                        bin_edges = np.linspace(0, 1, n_bins + 1)
                        values["bin"] = pd.cut(
                            values[author_ratio_column],
                            bins=bin_edges,
                            include_lowest=True,
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

                # Handle one values separately
                one_mask = field_subset[author_ratio_column] == 1
                one_values = field_subset.loc[one_mask, target_column]
                if len(one_values) >= min_group_size:
                    one_median = one_values.median()
                    one_std = one_values.std()
                    one_sem = one_std / np.sqrt(len(one_values))

                    if ci_method == "bootstrap":
                        boot = np.random.choice(
                            one_values, (n_bootstrap, len(one_values))
                        )
                        boot_meds = np.median(boot, axis=1)
                        lower = np.percentile(boot_meds, (100 - ci) / 2)
                        upper = np.percentile(boot_meds, 100 - (100 - ci) / 2)
                        del boot, boot_meds
                    else:
                        z = 1.96
                        lower = one_median - z * one_sem
                        upper = one_median + z * one_sem

                    results.append(
                        {
                            "x": 1,
                            "median": one_median,
                            "count": len(one_values),
                            "median_ci_lower": lower,
                            "median_ci_upper": upper,
                        }
                    )

                del one_mask, one_values
                gc.collect()

            else:
                # NEW LOGIC FOR TEAM SIZES 1-7: Use raw ratio values without binning
                values = field_subset[[author_ratio_column, target_column]].dropna()

                if len(values) > 0:
                    grouped = values.groupby(author_ratio_column, observed=False)

                    for ratio_val, group in grouped:
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

                        results.append(
                            {
                                "x": ratio_val,
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
        ax.set_title(title_text, fontweight="bold", fontsize=14)
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
        loc="upper center",
        bbox_to_anchor=(0.5, -0.02),  # Move legend below the figure
        ncol=min(len(fields), 5),  # Increase columns for better fit
        frameon=True,
        fontsize=12,  # Slightly smaller font
        columnspacing=1.0,  # Add some spacing between columns
    )

    # Remove individual legends
    for ax in axes_flat:
        if ax.get_legend() is not None:
            ax.get_legend().remove()

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


def main(skip_level_1_analysis=True):
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
    df_level_0.rename(columns={"level_0_field_names": "level_0_field"}, inplace=True)
    df_level_0 = df_level_0[df_level_0["level_0_field"].notna()].copy()

    # Drop level_1_field_names from level_0 dataframe
    df_level_0.drop(columns=["level_1_field_names"], inplace=True, errors="ignore")

    print(f"Level 0 dataframe shape: {df_level_0.shape}")
    print(f"Number of unique level 0 fields: {df_level_0['level_0_field'].nunique()}")
    print(
        f"Level 0 field value counts:\n{df_level_0['level_0_field'].value_counts()}\n"
    )
    gc.collect()

    # Calculate correlation coefficients for Level 0 fields
    print("Calculating correlation coefficients for Level 0 fields...")
    out_df_level_0 = find_correlation_coefficient(
        df_level_0, "first_time_author_ratio", "disruption_percentile", "level_0_field"
    )
    print("Level 0 Correlations:")
    print(out_df_level_0)
    print()

    # Plot Level 0 fields
    print("Plotting Level 0 fields...")
    fig, axes = plot_firsttime_authors_by_field_grid(
        df=df_level_0,
        target_column="disruption_percentile",
        field_column="level_0_field",
        fields_to_plot=None,
        save_path="Final_Figures/Final_Disruption_by_First_Time_Author_Ratio_level_0_fields_1-8.pdf",
    )
    plt.close(fig)
    del fig, axes

    # Clean up Level 0 dataframe
    print("Cleaning up Level 0 data from memory...")
    del df_level_0, out_df_level_0
    gc.collect()

    if skip_level_1_analysis:
        return

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
    df_level_1.rename(columns={"level_1_field_names": "level_1_field"}, inplace=True)
    df_level_1 = df_level_1[df_level_1["level_1_field"].notna()].copy()

    print(f"Level 1 dataframe shape: {df_level_1.shape}")
    print(f"Number of unique level 1 fields: {df_level_1['level_1_field'].nunique()}")
    print(
        f"Level 1 field value counts:\n{df_level_1['level_1_field'].value_counts()}\n"
    )

    # Clean up final_df as we don't need it anymore
    del final_df
    gc.collect()

    # Calculate correlation coefficients for Level 1 fields
    print("Calculating correlation coefficients for Level 1 fields...")
    out_df_level_1 = find_correlation_coefficient(
        df_level_1, "first_time_author_ratio", "disruption_percentile", "level_1_field"
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
