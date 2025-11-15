import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np
import scipy.stats as stats
import numpy as np
import pandas as pd
from scipy.stats import ttest_ind, mannwhitneyu
from google.cloud import bigquery


def get_productivity_pattern_data():
    BIGQUERY_PROJECT = "scisci-cssai-usf"  # replace this with your GCP project name
    DISRUPTION_DATASET = "Disruption"

    bq_client = bigquery.Client(project=BIGQUERY_PROJECT)

    BQ_SQL = f""" 
    SELECT Career_Age, 
        AVG(Paper_Count_In_Year) as AVG_Num_Of_Paper_In_Year,
        STDDEV(Paper_Count_In_Year) as STD_Paper_Count,
        COUNT(Paper_Count_In_Year) as COUNT_Paper_Count
    FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.All_Yearly_Author_Profiles` 
    WHERE Paper_Count_In_Year > 0
    GROUP BY Career_Age
    HAVING Career_Age < 80
    ORDER BY Career_Age ASC
    """

    job = bq_client.query(BQ_SQL)
    temp_df = job.to_dataframe()
    temp_df["STD_ERR"] = temp_df["STD_Paper_Count"] / np.sqrt(temp_df["COUNT_Paper_Count"])
    temp_df.to_csv("data/average_num_of_papers_by_career_age.csv", index=False)

def setup_plotting_style():
    sns.set(context="talk", style="whitegrid")

def get_career_stage(career_age):
    if career_age == 0:
        return "First-Time"
    elif career_age <= 10:
        return "Early-Career"
    else:
        return "Senior"
    
def plot_career_age_vs_yearly_paper(df, show_avg=True, save_path=None):
    setup_plotting_style()

    # Create the color palette as specified
    full_palette = sns.color_palette("viridis_r", 8)
    color_indices = [0, 1, 3, 5]
    selected_colors = [full_palette[i] for i in color_indices]

    # Create a dictionary to map career stages to colors
    career_stages = ["First-Time", "Early-Career", "Mid-Career", "Senior"]
    color_dict = dict(zip(career_stages, selected_colors))

    plt.figure(figsize=(12, 5), dpi=300)

    # Plot the scatter points with error bars
    sns.scatterplot(
        data=df,
        x="Career_Age",
        y="AVG_Num_Of_Paper_In_Year",
        hue="Career Stage",
        style="Career Stage",
        palette=color_dict,
        s=100,
        alpha=0.8,
    )

    # Add error bars
    for stage in career_stages:
        stage_data = df[df["Career Stage"] == stage]
        plt.errorbar(
            stage_data["Career_Age"],
            stage_data["AVG_Num_Of_Paper_In_Year"],
            yerr=stage_data["STD_ERR"],
            fmt="none",
            ecolor=color_dict[stage],
            alpha=0.6,
            capsize=3,
            capthick=1,
        )

    # Calculate and plot the average for each career stage with y limits
    if show_avg:
        for stage in career_stages:
            stage_data = df[df["Career Stage"] == stage]
            avg_papers = stage_data["AVG_Num_Of_Paper_In_Year"].mean()

            # Define x-range for each career stage
            if stage == "First-Time":
                x_min, x_max = 0, 0
                x_pos = 0
            elif stage == "Early-Career":
                x_min, x_max = 1, 4
                x_pos = 2.5
            elif stage == "Mid-Career":
                x_min, x_max = 5, 9
                x_pos = 7.5
            else:  # Senior
                x_min, x_max = 10, df["Career_Age"].max()
                x_pos = 15

            # Add the average value as text
            # plt.text(x_pos - 0.2, avg_papers - 0.2, f"Avg: {avg_papers:.2f}",
            #          color=color_dict[stage], fontweight='bold', ha='center')

            # Draw a horizontal line for the average ONLY within the career stage range
            plt.hlines(
                y=avg_papers,
                xmin=x_min,
                xmax=x_max,
                colors=color_dict[stage],
                linestyles="--",
                alpha=0.7,
                linewidth=2,
            )

    plt.title(
        f"Average Yearly Productivity Over Career Stage", fontweight="bold", fontsize=18
    )
    plt.xlabel("Career Age (Years)", fontweight="bold", fontsize=12)
    plt.ylabel("Avg Yearly Productivity", fontweight="bold", fontsize=12)
    plt.ylim(0, df["AVG_Num_Of_Paper_In_Year"].max() + 1)
    plt.legend(title="Career Stage", loc="lower right")

    if save_path:
        plt.savefig(save_path, bbox_inches="tight", dpi = 300)
    plt.show()


def test_first_time_difference(df):
    # Get data for each group
    first_time_data = df[df["Career Stage"] == "First-Time"]["AVG_Num_Of_Paper_In_Year"]
    early_career_data = df[df["Career Stage"] == "Early-Career"][
        "AVG_Num_Of_Paper_In_Year"
    ]
    mid_career_data = df[df["Career Stage"] == "Mid-Career"]["AVG_Num_Of_Paper_In_Year"]
    senior_data = df[df["Career Stage"] == "Senior"]["AVG_Num_Of_Paper_In_Year"]

    # Check if first_time_data has more than one value (needed for statistical tests)
    if len(first_time_data) <= 1:
        print("First-Time data has only one value, which limits statistical testing.")
        print(f"First-Time mean: {first_time_data.mean()}")

    # Descriptive statistics
    print("\nDescriptive statistics by career stage:")
    stats_df = df.groupby("Career Stage")["AVG_Num_Of_Paper_In_Year"].agg(
        ["count", "mean", "std", "min", "max"]
    )
    print(stats_df)

    # Since Senior data is not normal, we'll use non-parametric tests for all comparisons
    print("\nComparing First-Time with other stages (Mann-Whitney U test):")

    # Function to run Mann-Whitney U test (non-parametric)
    def compare_groups_nonparametric(group1, group2, name1, name2):
        if len(group1) <= 1 or len(group2) <= 1:
            print(f"{name1} vs {name2}: Cannot perform test - insufficient data")
            return

        try:
            stat, p = mannwhitneyu(group1, group2)
            print(
                f"{name1} vs {name2}: p-value = {p:.4f}, {'Significant' if p < 0.05 else 'Not significant'}"
            )

            # Calculate effect size (r = Z / sqrt(N))
            n1, n2 = len(group1), len(group2)
            N = n1 + n2

            # Calculate Z-score from p-value (approximation)
            from scipy.stats import norm

            z = norm.ppf(1 - p / 2)  # Two-tailed p-value to z-score
            r = z / np.sqrt(N)

            print(
                f"Effect size (r): {abs(r):.2f}, "
                + f"({'Small' if abs(r) < 0.3 else 'Medium' if abs(r) < 0.5 else 'Large'} effect)"
            )

            # Also calculate the percentage difference
            mean1, mean2 = group1.mean(), group2.mean()
            percent_diff = ((mean2 - mean1) / mean1) * 100
            print(f"Percentage difference: {percent_diff:.1f}%")

        except Exception as e:
            print(f"{name1} vs {name2}: Error in test - {e}")

    # Compare First-Time with each other group using non-parametric tests
    if len(first_time_data) > 1:
        compare_groups_nonparametric(
            first_time_data, early_career_data, "First-Time", "Early-Career"
        )
        compare_groups_nonparametric(
            first_time_data, mid_career_data, "First-Time", "Mid-Career"
        )
        compare_groups_nonparametric(
            first_time_data, senior_data, "First-Time", "Senior"
        )

    # For cases where First-Time has only one data point, we can still show the magnitude of differences
    else:
        ft_mean = first_time_data.mean()
        print(
            "\nSince First-Time has only one data point, showing percentage differences:"
        )
        print(
            f"First-Time vs Early-Career: {((early_career_data.mean() - ft_mean) / ft_mean * 100):.1f}% difference"
        )
        print(
            f"First-Time vs Mid-Career: {((mid_career_data.mean() - ft_mean) / ft_mean * 100):.1f}% difference"
        )
        print(
            f"First-Time vs Senior: {((senior_data.mean() - ft_mean) / ft_mean * 100):.1f}% difference"
        )

    # Kruskal-Wallis test (non-parametric alternative to ANOVA)
    print("\nKruskal-Wallis test across all career stages:")
    try:
        all_data = [first_time_data, early_career_data, mid_career_data, senior_data]
        # Filter out any empty groups
        all_data = [group for group in all_data if len(group) > 0]
        if len(all_data) >= 2:  # Need at least 2 groups for the test
            stat, p = stats.kruskal(*all_data)
            print(
                f"p-value = {p:.4f}, {'Significant' if p < 0.05 else 'Not significant'}"
            )
        else:
            print("Cannot perform Kruskal-Wallis test - insufficient data")
    except Exception as e:
        print(f"Error in Kruskal-Wallis test: {e}")




def main():
    setup_plotting_style()
    get_productivity_pattern_data()
    print("Data retrieval and saving completed.")

    df = pd.read_csv("data/average_num_of_papers_by_career_age.csv")

    df.loc[:, "Career Stage"] = df["Career_Age"].apply(get_career_stage)

    print("\nAverage number of papers by Career Stage:")
    print(df.groupby("Career Stage")["AVG_Num_Of_Paper_In_Year"].mean())

    subset = df[(df["Career_Age"] >= 0) & (df["Career_Age"] <= 50)].copy()
    subset["Increase"] = subset["AVG_Num_Of_Paper_In_Year"].diff().fillna(0)
    print("\nCareer Age vs Average Papers with Increase:")
    print(subset[["Career_Age", "AVG_Num_Of_Paper_In_Year", "Increase"]])

    plot_career_age_vs_yearly_paper(df, False, "Final_Figures/Sup_2_Career_Age_Vs_Avg_Paper.pdf")

    test_first_time_difference(df)


if __name__ == "__main__":
    main()