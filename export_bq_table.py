from google.cloud import bigquery, storage
import tempfile
import os


def export_bq_table_to_csv(
    bq_table_name,
    project_id="scisciresearch-mahdee",
    dataset_id="Disruption",
    bucket_name="scisciresearch-mahdee",
    output_folder="exported_data",
    cleanup_intermediate=False,
):
    """
    Export a BigQuery table to a CSV file in GCS (memory-efficient version).

    Args:
        bq_table_name (str): Name of the BigQuery table to export
        project_id (str): GCP project ID
        dataset_id (str): BigQuery dataset ID
        bucket_name (str): GCS bucket name for storage
        output_folder (str): Folder in GCS bucket to store the final CSV
        cleanup_intermediate (bool): Whether to delete intermediate CSV files after combining

    Returns:
        str: GCS URI of the combined CSV file
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

    # Extract table to GCS intermediate location
    extract_job = bq_client.extract_table(
        full_table_id,
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

    print(f"✅ Successfully created combined CSV at: {output_uri}")

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


gcs_uri = export_bq_table_to_csv("disruption_analysis")
print(f"Table exported to: {gcs_uri}")
