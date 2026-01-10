from google.cloud import bigquery, storage
import tempfile
import os


def export_bq_table_to_csv(
    bq_table_name,
    project_id="sciscinet-mahdee-483915",
    dataset_id="Disruption",
    bucket_name="sciscinet-data-mahdee",
    output_folder="exported_data",
    cleanup_intermediate=False,
):
    """
    Export a BigQuery table to a CSV file in GCS (memory-efficient version).
    Converts ARRAY columns to pipe-delimited strings for CSV compatibility.
    """

    # Initialize clients
    bq_client = bigquery.Client(project=project_id)
    gcs_client = storage.Client()

    # Define URIs and paths
    intermediate_prefix = f"intermediate/{bq_table_name}/"
    intermediate_uri = f"gs://{bucket_name}/{intermediate_prefix}*.csv"
    full_table_id = f"{project_id}.{dataset_id}.{bq_table_name}"
    output_filename = f"{bq_table_name}.csv"
    output_path = f"{output_folder}/{output_filename}"
    output_uri = f"gs://{bucket_name}/{output_path}"

    print(f"Starting export of table: {full_table_id}")

    # Create a temporary table with arrays converted to strings
    temp_table_id = f"{project_id}.{dataset_id}.temp_{bq_table_name}_export"
    
    # Query to convert arrays to pipe-delimited strings
    convert_query = f"""
    CREATE OR REPLACE TABLE `{temp_table_id}` AS
    SELECT
        paperid,
        doi,
        year,
        doctype,
        citation_count,
        C10,
        disruption,
        Atyp_Median_Z,
        Atyp_10pct_Z,
        Atyp_Pairs,
        team_size,
        institution_count,
        funding_count,
        avg_career_age,
        std_career_age,
        max_career_age,
        first_time_author_count,
        early_career_author_count,
        mid_career_author_count,
        senior_author_count,
        first_time_author_ratio,
        early_career_author_ratio,
        mid_career_author_ratio,
        senior_author_ratio,
        affiliation_author_ratio,
        avg_paper_count,
        avg_citation_count,
        avg_c5,
        avg_disruption,
        early_author_avg_paper_count,
        early_author_avg_citation_count,
        early_author_avg_c5,
        early_author_avg_disruption,
        mid_author_avg_paper_count,
        mid_author_avg_citation_count,
        mid_author_avg_c5,
        mid_author_avg_disruption,
        senior_author_avg_paper_count,
        senior_author_avg_citation_count,
        senior_author_avg_c5,
        senior_author_avg_disruption,
        avg_reference_age,
        median_reference_age,
        std_reference_age,
        avg_reference_popularity,
        median_reference_popularity,
        std_reference_popularity,
        ARRAY_TO_STRING(level_0_field_names, '|') AS level_0_field_names,
        ARRAY_TO_STRING(level_1_field_names, '|') AS level_1_field_names
    FROM `{full_table_id}`
    """
    
    print("Converting array columns to delimited strings...")
    convert_job = bq_client.query(convert_query)
    convert_job.result()
    print("Conversion completed.")

    # Extract the temporary table to GCS
    extract_job = bq_client.extract_table(
        temp_table_id,
        intermediate_uri,
        location="US",
    )
    extract_job.result()
    print("Export to GCS intermediate location completed.")

    # Get list of intermediate files
    bucket = gcs_client.bucket(bucket_name)
    intermediate_blobs = sorted(
        list(bucket.list_blobs(prefix=intermediate_prefix)),
        key=lambda x: x.name,
    )

    if not intermediate_blobs:
        raise ValueError("No files found in intermediate directory")

    print(f"Found {len(intermediate_blobs)} intermediate files to combine")

    # Use a temporary file instead of StringIO to avoid memory explosion
    with tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8") as tmp_file:
        for i, blob in enumerate(intermediate_blobs):
            print(f"Processing file {i+1}/{len(intermediate_blobs)}: {blob.name}")

            # Download blob in chunks
            content = blob.download_as_text(encoding="utf-8")
            lines = content.splitlines()

            if not lines:
                continue

            # Write header for the first file, skip for the rest
            if i == 0:
                tmp_file.write("\n".join(lines))
            else:
                tmp_file.write("\n" + "\n".join(lines[1:]))

        tmp_file.flush()

        print(f"Uploading combined file to GCS: {output_uri}")
        output_blob = bucket.blob(output_path)
        output_blob.upload_from_filename(tmp_file.name, content_type="text/csv")

    print(f"Successfully created combined CSV at: {output_uri}")

    # Clean up temporary table
    print("Cleaning up temporary table...")
    bq_client.delete_table(temp_table_id)
    print(f"Deleted temporary table: {temp_table_id}")

    # Clean up intermediate files if requested
    if cleanup_intermediate:
        print("Cleaning up intermediate files...")
        for blob in intermediate_blobs:
            try:
                blob.delete()
                print(f"Deleted: {blob.name}")
            except Exception as e:
                print(f"Warning: Could not delete {blob.name}: {e}")

    # Delete temporary local file
    os.remove(tmp_file.name)

    return output_uri


gcs_uri = export_bq_table_to_csv("disruption_analysis", cleanup_intermediate=True)
print(f"Table exported to: {gcs_uri}")
