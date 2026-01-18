import gc
import os
import warnings
import pandas as pd
import matplotlib.pyplot as plt
from perform_statistical_analysis import (
    load_full_disruption_data,
    find_correlation_coefficient,
    setup_plotting_style,
    plot_firsttime_authors_by_field_grid,
)


warnings.filterwarnings("ignore", category=RuntimeWarning)

# Set pandas options for memory efficiency
pd.options.mode.chained_assignment = None
pd.set_option("mode.copy_on_write", True)


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
