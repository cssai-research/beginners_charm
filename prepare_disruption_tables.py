import os
import time
from google.cloud import bigquery

## Constants
BIGQUERY_PROJECT = "scisci-cssai-usf"  # replace this with your GCP project name
SCISCINET_DATASET = "SciSciNet"
DISRUPTION_DATASET = "Disruption"

MIN_YEAR = 1961
MAX_YEAR = 2020

os.environ["GOOGLE_CLOUD_PROJECT"] = BIGQUERY_PROJECT
client = bigquery.Client()


def create_author_profiles(year):
    """
    This function creates a new table called temp_author_profile_{year} which stores each authors
    previous track record.
    """
    print(f"Creating author profiles for papers before {year}...")

    temp_table_name = (
        f"{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.temp_author_profile_{year}"
    )

    client = bigquery.Client(project=BIGQUERY_PROJECT)
    try:
        client.get_table(temp_table_name)
        print(f"Temporary table {temp_table_name} already exists. Deleting it...")
        client.delete_table(temp_table_name)
        print(f"Temporary table {temp_table_name} deleted.")
    except Exception:
        print(f"Temporary table {temp_table_name} does not exist. Creating it...")

    query = f"""
    CREATE OR REPLACE TABLE `{temp_table_name}` AS
    WITH AuthorPaperCount AS (
        SELECT 
            a.authorid,
            COUNT(DISTINCT CASE WHEN p.year = {year} THEN p.paperid ELSE NULL END) AS paper_count_in_year
        FROM `{BIGQUERY_PROJECT}.{SCISCINET_DATASET}.SciSciNet_Authors` a
        LEFT JOIN `{BIGQUERY_PROJECT}.{SCISCINET_DATASET}.SciSciNet_PaperAuthorAffiliations` pa
            ON a.authorid = pa.authorid 
        LEFT JOIN `{BIGQUERY_PROJECT}.{SCISCINET_DATASET}.SciSciNet_Papers` p 
            ON p.paperid = pa.paperid
        GROUP BY a.authorid
    )
    SELECT 
        a.authorid,
        COUNT(DISTINCT p.paperid) AS paper_count_in_prev_years,
        COALESCE(apc.paper_count_in_year, 0) AS paper_count_in_year,
        GREATEST({year} - a.debut_year, 0) AS career_age,
        COALESCE(AVG(p.citation_count), 0) AS avg_citation_count,
        COALESCE(AVG(p.C5), 0) AS avg_c5,
        AVG(p.disruption) AS avg_disruption,
        CASE 
            WHEN a.debut_year = {year} THEN TRUE 
            ELSE FALSE 
        END AS is_new_author,
        CASE 
            WHEN ({year} - a.debut_year) BETWEEN 1 AND 5 THEN TRUE 
            ELSE FALSE 
        END AS is_early_career_author,
        CASE 
            WHEN ({year} - a.debut_year) BETWEEN 6 AND 10 THEN TRUE 
            ELSE FALSE 
        END AS is_mid_career_author,
        CASE 
            WHEN ({year} - a.debut_year) >= 11 THEN TRUE 
            ELSE FALSE 
        END AS is_senior_author
    FROM `{BIGQUERY_PROJECT}.{SCISCINET_DATASET}.SciSciNet_Authors` a
    LEFT JOIN `{BIGQUERY_PROJECT}.{SCISCINET_DATASET}.SciSciNet_PaperAuthorAffiliations` pa
        ON a.authorid = pa.authorid 
    LEFT JOIN `{BIGQUERY_PROJECT}.{SCISCINET_DATASET}.SciSciNet_Papers` p 
        ON p.paperid = pa.paperid AND p.year < {year} -- Note: Only considering previous papers for career calculations
    LEFT JOIN AuthorPaperCount apc
        ON a.authorid = apc.authorid
    GROUP BY a.authorid, a.debut_year, apc.paper_count_in_year
    """

    print("Executing BigQuery query for creating author profiles...")
    query_job = client.query(query)
    return query_job, temp_table_name


def create_all_yearly_author_profiles():
    """
    Create a table that combines all yearly author profiles into a single table
    with AuthorID and Year as a composite key.
    """
    print("Creating All_Yearly_Author_Profiles table...")

    # Start building the query
    query = f"""
    CREATE OR REPLACE TABLE `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.All_Yearly_Author_Profiles` AS
    """

    # Add UNION ALL for each year
    for i, year in enumerate(range(MIN_YEAR, MAX_YEAR + 1)):
        if i > 0:
            query += "\nUNION ALL\n"

        query += f"""
        SELECT 
            authorid,
            {year} AS year,
            paper_count_in_prev_years,
            paper_count_in_year,
            career_age,
            avg_citation_count,
            avg_c5,
            avg_disruption,
            is_new_author,
            is_early_career_author,
            is_mid_career_author,
            is_senior_author
        FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.temp_author_profile_{year}`
        """

    # Execute the query
    print("Executing BigQuery query for creating All_Yearly_Author_Profiles...")
    query_job = client.query(query)
    query_job.result()

    print(
        f"Table {BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.All_Yearly_Author_Profiles created successfully."
    )

    # Get row count for validation
    count_query = f"""
    SELECT COUNT(*) as row_count 
    FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.All_Yearly_Author_Profiles`
    """
    count_job = client.query(count_query)
    results = count_job.result()

    for row in results:
        print(f"Total rows in All_Yearly_Author_Profiles: {row.row_count}")


def create_paper_author_details(year):
    """This function creates a table with details about paper's author level metrics for a specific year."""
    temp_author_profile_table = (
        f"{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.temp_author_profile_{year}"
    )

    create_query = f"""
    CREATE OR REPLACE TABLE `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.paper_author_details_{year}` AS
    WITH AuthorDetails AS (
        SELECT 
            pa.paperid,
            pa.authorid,
            COALESCE(ap.paper_count_in_prev_years, 0) AS paper_count,
            COALESCE(ap.career_age, 0) AS career_age,
            COALESCE(ap.avg_citation_count, 0) AS avg_citation_count,
            COALESCE(ap.avg_c5, 0) AS avg_c5,
            COALESCE(ap.avg_disruption, 0) AS avg_disruption,
            COALESCE(ap.is_new_author, FALSE) AS is_new_author,
            COALESCE(ap.is_early_career_author, FALSE) AS is_early_career_author,
            COALESCE(ap.is_mid_career_author, FALSE) AS is_mid_career_author,
            COALESCE(ap.is_senior_author, FALSE) AS is_senior_author,
            pa.institutionid
        FROM `{BIGQUERY_PROJECT}.{SCISCINET_DATASET}.SciSciNet_PaperAuthorAffiliations` pa
        JOIN `{BIGQUERY_PROJECT}.{SCISCINET_DATASET}.SciSciNet_Papers` p 
            ON p.paperid = pa.paperid AND p.year = {year}
        LEFT JOIN `{temp_author_profile_table}` ap
            ON pa.authorid = ap.authorid
    )
    SELECT 
        paperid,
        AVG(career_age) AS avg_career_age,
        STDDEV(career_age) AS std_career_age,
        MAX(career_age) AS max_career_age,
        AVG(paper_count) AS avg_paper_count,
        AVG(avg_citation_count) AS avg_citation_count,
        AVG(avg_c5) AS avg_c5,
        AVG(avg_disruption) AS avg_disruption,
        
        -- Calculate metrics for early career authors
        SAFE_DIVIDE(
            SUM(CASE WHEN is_early_career_author THEN paper_count ELSE 0 END),
            COUNTIF(is_early_career_author)
        ) AS early_author_avg_paper_count,
        SAFE_DIVIDE(
            SUM(CASE WHEN is_early_career_author THEN avg_citation_count ELSE 0 END),
            COUNTIF(is_early_career_author)
        ) AS early_author_avg_citation_count,
        SAFE_DIVIDE(
            SUM(CASE WHEN is_early_career_author THEN avg_c5 ELSE 0 END),
            COUNTIF(is_early_career_author)
        ) AS early_author_avg_c5,
        SAFE_DIVIDE(
            SUM(CASE WHEN is_early_career_author THEN avg_disruption ELSE 0 END),
            COUNTIF(is_early_career_author)
        ) AS early_author_avg_disruption,
        
        -- Calculate metrics for mid career authors
        SAFE_DIVIDE(
            SUM(CASE WHEN is_mid_career_author THEN paper_count ELSE 0 END),
            COUNTIF(is_mid_career_author)
        ) AS mid_author_avg_paper_count,
        SAFE_DIVIDE(
            SUM(CASE WHEN is_mid_career_author THEN avg_citation_count ELSE 0 END),
            COUNTIF(is_mid_career_author)
        ) AS mid_author_avg_citation_count,
        SAFE_DIVIDE(
            SUM(CASE WHEN is_mid_career_author THEN avg_c5 ELSE 0 END),
            COUNTIF(is_mid_career_author)
        ) AS mid_author_avg_c5,
        SAFE_DIVIDE(
            SUM(CASE WHEN is_mid_career_author THEN avg_disruption ELSE 0 END),
            COUNTIF(is_mid_career_author)
        ) AS mid_author_avg_disruption,
        
        -- Calculate metrics for senior authors
        SAFE_DIVIDE(
            SUM(CASE WHEN is_senior_author THEN paper_count ELSE 0 END),
            COUNTIF(is_senior_author)
        ) AS senior_author_avg_paper_count,
        SAFE_DIVIDE(
            SUM(CASE WHEN is_senior_author THEN avg_citation_count ELSE 0 END),
            COUNTIF(is_senior_author)
        ) AS senior_author_avg_citation_count,
        SAFE_DIVIDE(
            SUM(CASE WHEN is_senior_author THEN avg_c5 ELSE 0 END),
            COUNTIF(is_senior_author)
        ) AS senior_author_avg_c5,
        SAFE_DIVIDE(
            SUM(CASE WHEN is_senior_author THEN avg_disruption ELSE 0 END),
            COUNTIF(is_senior_author)
        ) AS senior_author_avg_disruption,
        
        COUNTIF(is_new_author) AS first_time_author_count,
        COUNTIF(is_early_career_author) AS early_career_author_count,
        COUNTIF(is_mid_career_author) AS mid_career_author_count,
        COUNTIF(is_senior_author) AS senior_author_count,
        -- Calculate author ratios
        CASE 
            WHEN COUNT(DISTINCT authorid) > 0 THEN 
                COUNTIF(is_new_author) / COUNT(DISTINCT authorid)
            ELSE 0
        END AS first_time_author_ratio,
        CASE 
            WHEN COUNT(DISTINCT authorid) > 0 THEN 
                COUNTIF(is_early_career_author) / COUNT(DISTINCT authorid)
            ELSE 0
        END AS early_career_author_ratio,
        CASE 
            WHEN COUNT(DISTINCT authorid) > 0 THEN 
                COUNTIF(is_mid_career_author) / COUNT(DISTINCT authorid)
            ELSE 0
        END AS mid_career_author_ratio,
        CASE 
            WHEN COUNT(DISTINCT authorid) > 0 THEN 
                COUNTIF(is_senior_author) / COUNT(DISTINCT authorid)
            ELSE 0
        END AS senior_author_ratio,
        COUNT(DISTINCT institutionid) / NULLIF(COUNT(DISTINCT authorid), 0) AS affiliation_author_ratio
    FROM AuthorDetails
    GROUP BY paperid
    """

    print(f"Creating paper_author_details_{year} table...")
    create_job = client.query(create_query)
    return create_job


def create_combined_data_table():
    combine_author_details_query = f"""
    CREATE OR REPLACE TABLE `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.paper_author_details` AS
    """

    for year in range(MIN_YEAR, MAX_YEAR + 1):
        if year == MIN_YEAR:
            combine_author_details_query += f"""
            SELECT * FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.paper_author_details_{year}`
            """
        else:
            combine_author_details_query += f"""
            UNION ALL
            SELECT * FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.paper_author_details_{year}`
            """

    print("Combining author details tables...")
    combine_job = client.query(combine_author_details_query)
    combine_job.result()

    final_table_query = f"""
    CREATE OR REPLACE TABLE `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.disruption_analysis` AS
    SELECT
        p.paperid,
        p.doi,
        p.year,
        p.doctype,
        p.citation_count, 
        p.C10,
        p.disruption, 
        p.Atyp_Median_Z,
        p.Atyp_10pct_Z, 
        p.Atyp_Pairs,
        p.team_size,
        p.institution_count, 
        p.nct_count + p.nih_count + p.nsf_count as funding_count,
        a.avg_career_age,
        a.std_career_age,
        a.max_career_age,
        a.first_time_author_count,
        a.early_career_author_count,
        a.mid_career_author_count,
        a.senior_author_count,
        a.first_time_author_ratio,
        a.early_career_author_ratio,
        a.mid_career_author_ratio,
        a.senior_author_ratio,
        a.affiliation_author_ratio,
        a.avg_paper_count,
        a.avg_citation_count,
        a.avg_c5,
        a.avg_disruption,
        -- Add new columns for career stage-specific metrics
        a.early_author_avg_paper_count,
        a.early_author_avg_citation_count,
        a.early_author_avg_c5,
        a.early_author_avg_disruption,
        a.mid_author_avg_paper_count,
        a.mid_author_avg_citation_count,
        a.mid_author_avg_c5,
        a.mid_author_avg_disruption,
        a.senior_author_avg_paper_count,
        a.senior_author_avg_citation_count,
        a.senior_author_avg_c5,
        a.senior_author_avg_disruption,
    FROM 
        `{BIGQUERY_PROJECT}.{SCISCINET_DATASET}.SciSciNet_Papers` p
    LEFT JOIN 
        `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.paper_author_details` a
    ON 
        p.paperid = a.paperid
    WHERE is_retracted is False
    AND year between {MIN_YEAR} AND {MAX_YEAR}
    """

    print("Creating final disruption_analysis table...")
    final_job = client.query(final_table_query)
    final_job.result()

    print(
        f"Final table {BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.disruption_analysis created successfully."
    )


def add_reference_metrics():
    """
    Add comprehensive reference metrics to the disruption_analysis table:
    - avg_reference_age: Average age of references cited by a paper
    - median_reference_age: Median age of references cited by a paper
    - std_reference_age: Standard deviation of reference ages
    - avg_reference_popularity: Average citations of references cited by a paper
    - median_reference_popularity: Median citations of references cited by a paper
    - std_reference_popularity: Standard deviation of reference citation counts
    """
    print("Adding comprehensive reference metrics to disruption_analysis table...")

    # Create temporary table with reference metrics
    reference_metrics_query = f"""
    CREATE OR REPLACE TABLE `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.paper_reference_metrics` AS
    WITH CitationData AS (
      SELECT
        pr.citing_paperid,
        pr.year_diff AS reference_age,
        cited.citation_count
      FROM 
        `{BIGQUERY_PROJECT}.{SCISCINET_DATASET}.SciSciNet_PaperReferences` pr
      JOIN 
        `{BIGQUERY_PROJECT}.{SCISCINET_DATASET}.SciSciNet_Papers` cited
      ON 
        pr.cited_paperid = cited.paperid
      WHERE
        pr.year_diff IS NOT NULL 
        AND pr.year_diff >= 0  -- Ensure positive reference age
        AND cited.citation_count IS NOT NULL
    ),
    AggregatedMetrics AS (
      SELECT
        citing_paperid,
        AVG(reference_age) AS avg_reference_age,
        STDDEV(reference_age) AS std_reference_age,
        AVG(citation_count) AS avg_reference_popularity,
        STDDEV(citation_count) AS std_reference_popularity
      FROM CitationData
      GROUP BY citing_paperid
    ),
    MedianMetrics AS (
      SELECT DISTINCT
        citing_paperid,
        PERCENTILE_CONT(reference_age, 0.5) OVER (PARTITION BY citing_paperid) AS median_reference_age,
        PERCENTILE_CONT(citation_count, 0.5) OVER (PARTITION BY citing_paperid) AS median_reference_popularity
      FROM CitationData
    )
    SELECT
      a.citing_paperid,
      a.avg_reference_age,
      m.median_reference_age,
      a.std_reference_age,
      a.avg_reference_popularity,
      m.median_reference_popularity,
      a.std_reference_popularity
    FROM AggregatedMetrics a
    JOIN MedianMetrics m
    ON a.citing_paperid = m.citing_paperid;
    """

    print("Creating paper_reference_metrics table...")
    reference_metrics_job = client.query(reference_metrics_query)
    reference_metrics_job.result()
    print("paper_reference_metrics table created successfully.")

    # Now update the disruption_analysis table to include these metrics
    update_query = f"""
    CREATE OR REPLACE TABLE `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.disruption_analysis` AS
    SELECT
      da.*,
      COALESCE(rm.avg_reference_age, 0) AS avg_reference_age,
      COALESCE(rm.median_reference_age, 0) AS median_reference_age,
      COALESCE(rm.std_reference_age, 0) AS std_reference_age,
      COALESCE(rm.avg_reference_popularity, 0) AS avg_reference_popularity,
      COALESCE(rm.median_reference_popularity, 0) AS median_reference_popularity,
      COALESCE(rm.std_reference_popularity, 0) AS std_reference_popularity
    FROM 
      `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.disruption_analysis` da
    LEFT JOIN
      `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.paper_reference_metrics` rm
    ON
      da.paperid = rm.citing_paperid;
    """

    print("Updating disruption_analysis table with comprehensive reference metrics...")
    update_job = client.query(update_query)
    update_job.result()
    print(
        "Added comprehensive reference metrics to disruption_analysis table successfully."
    )


def add_field_name():
    """
    Add field_name column to the disruption_analysis table.
    If a paper has multiple fields, only the first one (by fieldid) is kept.
    """
    print("Adding field_name column to disruption_analysis table...")

    # Create a temporary table with the first field for each paper
    temp_paper_fields_query = f"""
    CREATE OR REPLACE TABLE `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.temp_paper_first_field` AS
    WITH RankedFields AS (
        SELECT 
            pf.paperid,
            f.display_name as field_name,
            ROW_NUMBER() OVER (PARTITION BY pf.paperid ORDER BY pf.fieldid) as rn
        FROM `{BIGQUERY_PROJECT}.{SCISCINET_DATASET}.SciSciNet_PaperFields` pf
        JOIN `{BIGQUERY_PROJECT}.{SCISCINET_DATASET}.SciSciNet_Fields` f
        ON pf.fieldid = f.fieldid
    )
    SELECT 
        paperid,
        field_name
    FROM RankedFields
    WHERE rn = 1
    """

    print("Creating temporary table with first field for each paper...")
    temp_job = client.query(temp_paper_fields_query)
    temp_job.result()
    print("Temporary paper fields table created successfully.")

    # Update the disruption_analysis table to include field_name
    update_query = f"""
    CREATE OR REPLACE TABLE `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.disruption_analysis` AS
    SELECT
        da.*,
        COALESCE(tpf.field_name, 'Unknown') AS field_name
    FROM 
        `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.disruption_analysis` da
    LEFT JOIN
        `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.temp_paper_first_field` tpf
    ON
        da.paperid = tpf.paperid
    """

    print("Updating disruption_analysis table with field_name column...")
    update_job = client.query(update_query)
    update_job.result()
    print("Added field_name column to disruption_analysis table successfully.")

    # Clean up the temporary table
    print("Cleaning up temporary table...")
    client.delete_table(
        f"{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.temp_paper_first_field"
    )
    print("Temporary table deleted successfully.")

    # Get a count of papers with and without field names for validation
    validation_query = f"""
    SELECT 
        COUNT(*) as total_papers,
        COUNTIF(field_name != 'Unknown') as papers_with_fields,
        COUNTIF(field_name = 'Unknown') as papers_without_fields
    FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.disruption_analysis`
    """

    validation_job = client.query(validation_query)
    results = validation_job.result()

    for row in results:
        print(f"Validation results:")
        print(f"  Total papers: {row.total_papers}")
        print(f"  Papers with fields: {row.papers_with_fields}")
        print(f"  Papers without fields: {row.papers_without_fields}")


def clean_data():
    print("Starting comprehensive data cleaning...")

    # GET INITIAL COUNT
    initial_count_query = f"""
    SELECT COUNT(*) as initial_count 
    FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.disruption_analysis`
    """
    initial_count_job = client.query(initial_count_query)
    initial_results = initial_count_job.result()
    initial_count = 0
    for row in initial_results:
        initial_count = row.initial_count
        print(f"INITIAL COUNT: {initial_count:,} entries in disruption_analysis table")

    # 1. Quality Filter
    quality_filter_query = f"""
    DELETE FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.disruption_analysis`
    WHERE 
        disruption IS NULL
        OR doi IS NULL
        OR team_size IS NULL
        OR team_size = 0
    """

    print("Removing papers with missing critical data...")
    quality_job = client.query(quality_filter_query)
    quality_job.result()

    # Count after quality filter
    after_quality_query = f"""
    SELECT COUNT(*) as count FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.disruption_analysis`
    """
    after_quality_job = client.query(after_quality_query)
    after_quality_results = after_quality_job.result()
    for row in after_quality_results:
        quality_removed = initial_count - row.count
        print(
            f"After quality filter: {row.count:,} entries remaining ({quality_removed:,} removed)"
        )
        current_count = row.count

    # 2. Author Count Validation
    # This is needed because there are 182275783 records in the Paper Table where team_size is 0.
    counts_query = f"""
    DELETE FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.disruption_analysis`
    WHERE first_time_author_count + early_career_author_count + mid_career_author_count + senior_author_count != team_size
    """

    print("Deleting entries where author category counts don't sum to team_size...")
    counts_job = client.query(counts_query)
    counts_job.result()

    # Count after author count validation
    after_counts_job = client.query(after_quality_query)
    after_counts_results = after_counts_job.result()
    for row in after_counts_results:
        count_validation_removed = current_count - row.count
        print(
            f"After count validation: {row.count:,} entries remaining ({count_validation_removed:,} removed)"
        )
        current_count = row.count

    # 3. Author Ratio Validation
    ratios_query = f"""
    DELETE FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.disruption_analysis`
    WHERE ABS((first_time_author_ratio + early_career_author_ratio + mid_career_author_ratio + senior_author_ratio) - 1.0) >= 0.001
    """

    print(
        "Deleting entries where author category ratios don't sum to approximately 1..."
    )
    ratios_job = client.query(ratios_query)
    ratios_job.result()

    # Count after ratio validation
    after_ratios_job = client.query(after_quality_query)
    after_ratios_results = after_ratios_job.result()
    for row in after_ratios_results:
        ratio_validation_removed = current_count - row.count
        print(
            f"After ratio validation: {row.count:,} entries remaining ({ratio_validation_removed:,} removed)"
        )
        current_count = row.count

    # 4. Create problematic authors table and remove their papers
    print("Creating temporary table for problematic authors...")
    temp_problematic_authors_table = (
        f"{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.temp_problematic_authors"
    )

    # Drop table if exists
    try:
        client.get_table(temp_problematic_authors_table)
        print(
            f"Temporary table {temp_problematic_authors_table} already exists. Deleting it..."
        )
        client.delete_table(temp_problematic_authors_table)
    except Exception:
        print(
            f"Temporary table {temp_problematic_authors_table} does not exist. Creating it..."
        )

    # Create problematic authors table
    union_parts = []
    for year in range(MIN_YEAR, MAX_YEAR + 1):
        union_parts.extend(
            [
                f"""SELECT authorid FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.temp_author_profile_{year}`
                WHERE is_new_author = TRUE AND paper_count_in_year > 10""",
                f"""SELECT authorid FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.temp_author_profile_{year}`
                WHERE paper_count_in_year > 50""",
                f"""SELECT authorid FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.temp_author_profile_{year}`
                WHERE career_age > 80""",
                f"""SELECT authorid FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.temp_author_profile_{year}`
                WHERE paper_count_in_prev_years > 1000""",
            ]
        )

    create_problematic_authors_query = f"""
    CREATE OR REPLACE TABLE `{temp_problematic_authors_table}` AS
    SELECT DISTINCT authorid FROM ({' UNION ALL '.join(union_parts)})
    """

    create_job = client.query(create_problematic_authors_query)
    create_job.result()

    # Count problematic authors
    count_problematic_query = f"""
    SELECT COUNT(*) as count FROM `{temp_problematic_authors_table}`
    """
    count_job = client.query(count_problematic_query)
    count_result = count_job.result()
    for row in count_result:
        print(f"Found {row.count:,} unique problematic author IDs")

    # Get initial count of All_Yearly_Author_Profiles for tracking
    initial_profiles_count_query = f"""
    SELECT COUNT(*) as count FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.All_Yearly_Author_Profiles`
    """
    initial_profiles_job = client.query(initial_profiles_count_query)
    initial_profiles_results = initial_profiles_job.result()
    for row in initial_profiles_results:
        initial_profiles_count = row.count
        print(f"Initial All_Yearly_Author_Profiles count: {initial_profiles_count:,}")

    # Remove problematic authors from All_Yearly_Author_Profiles table
    delete_profiles_query = f"""
    DELETE FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.All_Yearly_Author_Profiles`
    WHERE authorid IN (
        SELECT authorid FROM `{temp_problematic_authors_table}`
    )
    """

    print("Removing problematic authors from All_Yearly_Author_Profiles table...")
    delete_profiles_job = client.query(delete_profiles_query)
    delete_profiles_job.result()

    # Count remaining profiles after cleanup
    after_profiles_cleanup_job = client.query(initial_profiles_count_query)
    after_profiles_results = after_profiles_cleanup_job.result()
    for row in after_profiles_results:
        profiles_removed = initial_profiles_count - row.count
        print(
            f"All_Yearly_Author_Profiles after cleanup: {row.count:,} entries remaining ({profiles_removed:,} removed)"
        )

    # Remove papers by problematic authors from disruption_analysis
    delete_papers_query = f"""
    DELETE FROM `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.disruption_analysis`
    WHERE paperid IN (
        SELECT DISTINCT pa.paperid
        FROM `{BIGQUERY_PROJECT}.{SCISCINET_DATASET}.SciSciNet_PaperAuthorAffiliations` pa
        INNER JOIN `{temp_problematic_authors_table}` prob
        ON pa.authorid = prob.authorid
    )
    """

    print("Deleting papers by problematic authors...")
    delete_job = client.query(delete_papers_query)
    delete_job.result()

    # FINAL COUNT
    final_count_job = client.query(after_quality_query)
    final_results = final_count_job.result()
    for row in final_results:
        problematic_authors_removed = current_count - row.count
        total_removed = initial_count - row.count
        print(
            f"After problematic authors removal: {row.count:,} entries remaining ({problematic_authors_removed:,} removed)"
        )
        print(f"\n" + "=" * 60)
        print(f"FINAL CLEANUP SUMMARY:")
        print(f"Initial entries: {initial_count:,}")
        print(f"Final entries: {row.count:,}")
        print(
            f"disruption_analysis - Total removed: {total_removed:,} ({(total_removed/initial_count)*100:.2f}%)"
        )
        print(f"All_Yearly_Author_Profiles - Problematic authors removed: {profiles_removed:,}")
        print(f"=" * 60)

    # Clean up temporary table
    client.delete_table(temp_problematic_authors_table)
    print("Temporary problematic authors table deleted.")


def delete_temp_tables():
    delete_query = ""
    for year in range(MIN_YEAR, MAX_YEAR + 1):
        delete_query += f"""
        DROP TABLE IF EXISTS `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.temp_author_profile_{year}`;
        """
        delete_query += f"DROP TABLE IF EXISTS `{BIGQUERY_PROJECT}.{DISRUPTION_DATASET}.paper_author_details_{year}`;\n"

    delete_job = client.query(delete_query)
    delete_job.result()
    print("Temporary tables deleted successfully.")


if __name__ == "__main__":
    start_time = time.time()

    # Start all author profile creation jobs concurrently
    print("Starting all author profile creation jobs...")
    author_profile_jobs = {}
    for year in range(MIN_YEAR, MAX_YEAR + 1):
        job, temp_table_name = create_author_profiles(year)
        author_profile_jobs[year] = (job, temp_table_name)
        print(f"Started author profile job for year {year}")

    # Wait for all author profile jobs to complete
    print("Waiting for all author profile jobs to complete...")
    for year, (job, temp_table_name) in author_profile_jobs.items():
        job.result()  # Wait for completion
        print(f"Author profile job for year {year} completed: {temp_table_name}")

    create_all_yearly_author_profiles()

    # Start all paper author details jobs concurrently
    print("Starting all paper author details jobs...")
    paper_author_detail_jobs = {}
    for year in range(MIN_YEAR, MAX_YEAR + 1):
        job = create_paper_author_details(year)
        paper_author_detail_jobs[year] = job
        print(f"Started paper author details job for year {year}")

    # Wait for all paper author details jobs to complete
    print("Waiting for all paper author details jobs to complete...")
    for year, job in paper_author_detail_jobs.items():
        job.result()  # Wait for completion
        print(f"Paper author details job for year {year} completed")

    create_combined_data_table()
    add_reference_metrics()
    add_field_name()

    print("Cleaning Data")
    clean_data()

    print("Deleting temporary tables...")
    delete_temp_tables()

    print(
        f"All tasks completed successfully in { round((time.time() - start_time) / 60, 2)} minutes."
    )
