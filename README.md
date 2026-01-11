## Beginner Author Codes

This repository contains the code accompanying the paper "Beginner's Charm: Beginner Heavy Teams Are Associated With High Scientific Disruption."
[https://arxiv.org/abs/2509.10389](https://arxiv.org/abs/2509.10389)

### Prerequisites

The preprocessing pipeline requires substantial computational resources and is implemented using Google Cloud Platform's BigQuery. To proceed:

1. Create a new GCP project. New accounts receive $300 in free credits upon registration.
2. Note: The preprocessing operations are estimated to cost less than $50.

Install the required Python packages:
```bash
pip install -r requirements.txt
```

### Data Loading and Preprocessing

Navigate to [BigQuery](https://console.cloud.google.com/bigquery) and create two datasets:
- `SciSciNet`
- `Disruption`

#### Configuration and Execution

Update the `GCP_PROJECT_ID` or `GCP_PROJECT` variable in the following files:

1. **`load_perquate_to_bq.py`**  
   This script creates BigQuery tables within the `SciSciNet` dataset by loading data directly from the SciSciNet V2 public bucket's Parquet files.

2. **`prepare_disruption_tables.py`**  
   This script generates the `disruption_analysis` and `All_Yearly_Author_Profiles` tables required for the paper's analysis.

### Exporting Data

1. Create a storage bucket on GCP at https://console.cloud.google.com/storage
2. Update the `GCP_PROJECT` and `bucket_name` variables in the following file:

**`export_bq_table.py`**  
This script exports the BigQuery table into multiple blobs, combines them into a single CSV file, and stores it at `bucket_name/exported_data/disruption_analysis.csv`.

3. Download `disruption_analysis.csv` from the bucket and place it in the `/data` folder.

### Statistical Analysis

Once the data is available in the `/data` folder, proceed with the statistical analysis using the following scripts:

1. **`perform_statistical_analysis.py`**  
   Generates the majority of plots and tables presented in the article.

2. **`perform_field_based_analysis.py`**  
   Generates plots illustrating the relationship between beginner author ratio and disruption percentile across level 0 and level 1 fields.

3. **`perform_productivity_pattern_analysis.py`**  
   Generates plots depicting career age versus average number of papers.

4. **`perform_midcareer_analysis.py`**  
   Generates mid-career related analyses as detailed in the supplementary materials.

### FAQ

**Q: I do not have gcloud-sdk installed and encounter an error with the line `bq_client = bigquery.Client(project=project_id)`. How can I resolve this?**

**A:** You can use Google Colab as an alternative. Follow these steps:

1. Open Google Colab and authenticate your account:
```python
from google.colab import auth
auth.authenticate_user()
```

2. In the next cell, copy the contents of the Python file and execute it.

3. Ensure your Colab account has owner access to the GCP project.