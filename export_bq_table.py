from google.cloud import bigquery, storage
import io


def export_bq_table_to_csv(
    bq_table_name,
    project_id="scisci-cssai-usf",
    dataset_id="Disruption",
    bucket_name="sciscinet-data",
    output_folder="exported_data",
    cleanup_intermediate=False,
):
    """
    Export a BigQuery table to a CSV file in GCS.

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

    extract_job.result()  # Wait for the job to complete
    print("Export to GCS intermediate location completed.")

    # Get list of intermediate files
    bucket = gcs_client.bucket(bucket_name)
    intermediate_blobs = list(bucket.list_blobs(prefix=intermediate_prefix))

    if not intermediate_blobs:
        raise ValueError("No files found in intermediate directory")

    # Sort blobs by name to ensure consistent ordering
    intermediate_blobs = sorted(intermediate_blobs, key=lambda x: x.name)

    print(f"Found {len(intermediate_blobs)} intermediate files to combine")

    # Combine files directly in memory and upload to GCS
    combined_content = io.StringIO()

    for i, blob in enumerate(intermediate_blobs):
        print(f"Processing file {i+1}/{len(intermediate_blobs)}: {blob.name}")

        # Download blob content as text
        content = blob.download_as_text(encoding="utf-8")

        if i == 0:
            # Include header for first file
            combined_content.write(content)
        else:
            # Skip header line for subsequent files
            lines = content.split("\n")
            if len(lines) > 1:  # Ensure there's more than just a header
                combined_content.write("\n".join(lines[1:]))

    # Upload combined content to GCS
    output_blob = bucket.blob(output_path)
    combined_content.seek(0)  # Reset to beginning of StringIO
    output_blob.upload_from_string(combined_content.getvalue(), content_type="text/csv")

    print(f"Successfully created combined CSV at: {output_uri}")

    # Clean up intermediate files
    if cleanup_intermediate:
        print("Cleaning up intermediate files...")
        for blob in intermediate_blobs:
            try:
                blob.delete()
                print(f"Deleted intermediate file: {blob.name}")
            except Exception as e:
                print(f"Warning: Could not delete {blob.name}: {e}")

    combined_content.close()
    return output_uri


gcs_uri = export_bq_table_to_csv("disruption_analysis")
print(f"Table exported to: {gcs_uri}")
