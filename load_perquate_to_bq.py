from google.cloud import bigquery

GCP_PROJECT_NAME = "scisci-cssai-usf"  # replace this with your GCP project name
DATASET_NAME = "SciSciNet"
BUCKET_PATH = "gs://sciscinet-neo/v2"

tables = {
    "sciscinet_authors.parquet": "SciSciNet_Authors",
    "sciscinet_papers.parquet": "SciSciNet_Papers",
    "sciscinet_paperfields.parquet": "SciSciNet_PaperFields",
    "sciscinet_fields.parquet": "SciSciNet_Fields",
    "sciscinet_paper_author_affiliation.parquet": "SciSciNet_PaperAuthorAffiliations",
    "sciscinet_paperrefs.parquet": "SciSciNet_PaperReferences",
}

print(f"BigQuery version: {bigquery.__version__}")
client = bigquery.Client(project=GCP_PROJECT_NAME)

for parquet_file, bq_table_name in tables.items():
    uri = f"{BUCKET_PATH}/{parquet_file}"
    print(f"Submitting load job for {parquet_file} into {bq_table_name}...")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        column_name_character_map="V2",
    )

    try:
        load_job = client.load_table_from_uri(
            uri,
            f"{GCP_PROJECT_NAME}.{DATASET_NAME}.{bq_table_name}",
            job_config=job_config,
        )
        load_job.result()
        print(f"Loaded {parquet_file} into {bq_table_name}")
    except Exception as e:
        print(f"Error loading {parquet_file}: {e}")

print("All processing completed!")

print("\nRenaming column P_gf_ to P_gf in SciSciNet_Authors table...")

try:
    # Create a new table with the renamed column
    rename_query = f"""
    CREATE OR REPLACE TABLE `{GCP_PROJECT_NAME}.{DATASET_NAME}.SciSciNet_Authors` AS
    SELECT 
        authorid,
        avg_c10,
        avg_logc10,
        productivity,
        h_index,
        display_name,
        inference_sources,
        inference_counts,
        P_gf_ AS P_gf
    FROM `{GCP_PROJECT_NAME}.{DATASET_NAME}.SciSciNet_Authors`
    """

    query_job = client.query(rename_query)
    query_job.result()
    print("Successfully renamed column P_gf_ to P_gf")

except Exception as e:
    print(f"Error renaming column: {e}")


# add debut year on author table
try:
    alter_table_query = f"""
    ALTER TABLE `{GCP_PROJECT_NAME}.{DATASET_NAME}.SciSciNet_Authors`
    ADD COLUMN debut_year INT64
    """
    query_job = client.query(alter_table_query)
    query_job.result()
    print("Successfully added debut_year column to SciSciNet_Authors table")

    update_debut_year_query = f"""
    UPDATE `{GCP_PROJECT_NAME}.{DATASET_NAME}.SciSciNet_Authors` AS authors
    SET debut_year = (
        SELECT MIN(papers.year) 
        FROM `{GCP_PROJECT_NAME}.{DATASET_NAME}.SciSciNet_PaperAuthorAffiliations` AS affiliations
        JOIN `{GCP_PROJECT_NAME}.{DATASET_NAME}.SciSciNet_Papers` AS papers
        ON affiliations.paperid = papers.paperid
        WHERE affiliations.authorid = authors.authorid
        AND papers.year IS NOT NULL
    )
    WHERE TRUE
    """
    query_job = client.query(update_debut_year_query)
    query_job.result()
    print("Updating debut_year values...")
    query_job = client.query(update_debut_year_query)
    query_job.result()
    print(f"Successfully updated debut_year for all authors")

except Exception as e:
    print(f"Error updating debut_year: {e}")

# Schema display section
print("\n" + "=" * 80)
print("TABLE SCHEMAS")
print("=" * 80)

for parquet_file, bq_table_name in tables.items():
    print(f"\n{'-'*60}")
    print(f"SCHEMA FOR TABLE: {bq_table_name}")

    try:
        table_ref = client.dataset(DATASET_NAME).table(bq_table_name)
        table = client.get_table(table_ref)

        print(f"Number of columns: {len(table.schema)}")
        print("\nColumns:")

        for field in table.schema:
            print(f"{field.name},{field.field_type}")

    except Exception as e:
        print(f"Error retrieving schema for {bq_table_name}: {e}")
