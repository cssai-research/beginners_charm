# Scientific teams with more debutants are disproportionately disruptive

This repository contains the data-preparation and analysis code accompanying the manuscript:

**Scientific teams with more debutants are disproportionately disruptive**

Mahdee Mushfique Kamal and Raiyan Abdul Baten

The study analyzes 29,054,261 research articles published from 1941 through 2020 using SciSciNet V2. We define a **scientific debutant** as an author in the year of their first observed publication and examine how the share of debutants within a research team relates to scientific disruption, knowledge recombination, reference use, citation impact, team size, and collaborator publication histories.

The repository is organized as an end-to-end reproducibility pipeline:

```text
SciSciNet V2
    ↓
BigQuery preprocessing
    ↓
data/disruption_analysis.csv
    ↓
01_prepare_sciscinet_fields.ipynb
    ↓
02_prepare_exact_career_age.ipynb
    ↓
03_manuscript_analysis.ipynb
    ↓
Manuscript analyses, figures, tables, and robustness checks
```

## Repository structure

The main files are:

```text
.
├── README.md
├── Schema.md
├── requirements.txt
├── .gitignore
│
├── load_perquate_to_bq.py
├── prepare_disruption_tables.py
├── export_bq_table.py
│
├── 01_prepare_sciscinet_fields.ipynb
├── 02_prepare_exact_career_age.ipynb
├── 03_manuscript_analysis.ipynb
│
└── data/
    └── README.md
```

Large source data, intermediate files, and generated analysis files are not stored in the repository.

---

## 1. Requirements

The upstream preprocessing uses Google Cloud Platform and BigQuery. The local preparation and analysis stages use Python and Jupyter.

Install the repository dependencies with:

```bash
pip install -r requirements.txt
```

The notebook workflow uses packages including:

- Polars
- pandas
- NumPy
- SciPy
- statsmodels
- PyArrow
- DuckDB
- matplotlib
- psutil
- huggingface_hub
- hf_xet

Access to SciSciNet V2 is also required for the two local preparation notebooks.

---

## 2. BigQuery preprocessing

The large-scale preprocessing stage constructs the main paper-level analysis dataset.

### 2.1 Create the Google Cloud datasets

Create a Google Cloud project and, in BigQuery, create two datasets:

```text
SciSciNet
Disruption
```

Then configure the Google Cloud project identifier in the preprocessing scripts as described below.

### 2.2 Load SciSciNet V2 into BigQuery

Edit the project configuration in:

```text
load_perquate_to_bq.py
```

and run:

```bash
python load_perquate_to_bq.py
```

This script creates the required BigQuery tables in the `SciSciNet` dataset from the SciSciNet V2 source data.

### 2.3 Construct the analysis tables

Edit the project configuration in:

```text
prepare_disruption_tables.py
```

and run:

```bash
python prepare_disruption_tables.py
```

This stage constructs the paper-level `disruption_analysis` table and the author-year information used by the analysis pipeline.

### 2.4 Export the main analysis CSV

Create or select a Google Cloud Storage bucket, then configure the project and bucket settings in:

```text
export_bq_table.py
```

Run:

```bash
python export_bq_table.py
```

The script exports the BigQuery `disruption_analysis` table to Google Cloud Storage and combines the exported parts into:

```text
disruption_analysis.csv
```

Download this file and place it at:

```text
data/disruption_analysis.csv
```

The subsequent notebooks assume this exact location.

---

## 3. Prepare canonical SciSciNet V2 field memberships

Run:

```text
01_prepare_sciscinet_fields.ipynb
```

This notebook reconstructs the canonical SciSciNet V2 field memberships used for the manuscript's field-specific analyses.

The analysis uses the 19 canonical Level-0 fields and preserves SciSciNet's multi-membership convention: a multidisciplinary paper can belong to more than one broad field.

The notebook downloads the required SciSciNet V2 field files and writes the primary field-analysis handoff to:

```text
data/sciscinet_field_reconstruction_v2/
    analysis_level0_field_memberships.parquet
```

It also generates supporting field lookups, diagnostics, and a provenance manifest.

### Hugging Face access

SciSciNet V2 is accessed through Hugging Face.

Before running the notebook:

1. obtain access to the SciSciNet V2 repository;
2. create a Hugging Face read token if required;
3. either set the `HF_TOKEN` environment variable or enter the token securely when prompted by the notebook.

Do not save access tokens directly in the notebook.

---

## 4. Prepare exact career-age composition

After completing the field-preparation notebook, run:

```text
02_prepare_exact_career_age.ipynb
```

For every author linked to a focal paper, this notebook reconstructs the author's earliest observed publication year in SciSciNet V2 and calculates career age on the focal paper.

The manuscript distinguishes:

- career age 0: publication debut;
- career ages 1–10: subsequent early career;
- career age >10: senior career stage.

For the exact-age analyses, career ages 0 through 10 are represented separately.

The main output is:

```text
data/sciscinet_exact_career_age_v2/
    v9_exact_career_age_handoff.parquet
```

The full paper-level handoff contains all 29,054,261 focal papers.

The SciSciNet V2 author-paper mapping provides complete exact-career-age reconstruction for 29,054,201 papers. For 60 papers, the mapping contains one fewer linked author than the team size recorded in the analysis dataset. These papers remain in the handoff to preserve row alignment but are excluded from analyses requiring complete exact-career-age composition.

The notebook validates that the reconstructed exact-age composition collapses exactly to the existing broad career-stage variables for all 29,054,201 completely reconstructed papers.

Additional intermediate author-history files are retained because they are used by the one-publication-debutant robustness analysis in the manuscript notebook.

---

## 5. Reproduce the manuscript analyses

After the two preparation notebooks have completed, run:

```text
03_manuscript_analysis.ipynb
```

This is the main statistical-analysis notebook for the manuscript.

It reproduces the analyses underlying the reported results, including:

- the association between debutant share and disruption;
- comparisons among debutant, early-career, and senior author shares;
- exact career-age profiles from publication debut through career age 10;
- direct debut-versus-post-debut contrasts;
- post-debut trajectory extrapolation diagnostics;
- debutant share × team-size interactions;
- temporal robustness analyses;
- field-specific analyses across the 19 SciSciNet V2 Level-0 fields;
- atypical knowledge-combination analyses;
- reference-popularity analyses;
- 10-year citation-impact analyses;
- disruption × debutant-share citation models;
- collaborator prior-disruption models;
- collaborator prior-disruption × debutant-share interactions;
- exclusion of papers containing one-publication debutants;
- structure-preserving randomization tests;
- family-wise tests of adjacent career-age transitions;
- manuscript and supplementary figures and tables.

The notebook is committed to this repository with its executed outputs so that the reported figures, tables, model estimates, and diagnostics can be inspected without rerunning the complete analysis.

Rerunning the notebook requires the main CSV and the outputs of both preparation notebooks described above.

---

## 6. Main analysis samples

The primary paper-level analysis sample contains:

```text
29,054,261 papers
```

published from 1941 through 2020.

The complete exact-career-age sample contains:

```text
29,054,201 papers
```

Citation-impact analyses use papers published through 2010 so that every included paper has a complete 10-year citation window.

Atypical knowledge-combination analyses exclude unavailable or non-finite atypicality values before ranking and statistical analysis.

---

## 7. Main measures

### Debutant share

For each paper, debutant share is the fraction of authors whose focal-paper publication occurs in the same year as their first observed publication in SciSciNet V2.

### Career age

For an author on a focal paper:

```text
career age = focal publication year − first observed publication year
```

Career age 0 therefore corresponds to publication debut.

### Disruption

The primary outcome is disruption percentile, derived from the citation-network disruption measure and ranked from 0 to 100 across the analysis sample.

Higher values indicate greater disruption.

### Atypical knowledge combination

Atypicality is based on the historical co-occurrence of publication venues in a paper's reference list.

Regression analyses use a percentile-ranked version of the paper-level median venue-pair z score.

Lower percentile values indicate more atypical knowledge combinations.

### Reference popularity

Reference popularity measures how highly cited a paper's references were before the focal paper appeared.

The primary analysis uses average reference-popularity percentile.

Lower values indicate reliance on less-popular prior work.

### Citation impact

Citation impact is measured using the number of citations accumulated within 10 years of publication, denoted \(C_{10}\).

Citation-impact analyses rank \(C_{10}\) within publication year and include papers published through 2010.

---

## 8. Statistical analysis

Unless otherwise noted, paper-level regression models include:

- publication-year fixed effects;
- exact team-size fixed effects for team sizes 1–40.

Models are estimated using ordinary least squares after exact residualization with respect to these fixed effects.

Inference uses HC1 heteroskedasticity-robust standard errors and two-sided 95% confidence intervals.

When multiple career-stage shares enter jointly, senior-author share is the reference category.

In exact-career-age models, shares at career ages 0 through 10 enter jointly and authors more than 10 years past debut form the reference category.

Because of the very large sample size, interpretation emphasizes effect sizes and confidence intervals rather than statistical significance alone.

---

## 9. Structure-preserving randomization tests

The manuscript includes two matched randomization tests designed to preserve the observed constraints of team career-age composition.

### Null 1

The first null preserves:

- publication year;
- exact team size;
- each paper's combined age-0 and age-1 representation;
- exact career-age composition at ages 2–10.

Within matched strata, the fixed age-0/age-1 total is randomly divided between debutants and age-1 authors.

### Null 2

The second null preserves:

- publication year;
- exact team size;
- the total number of authors at career ages 0–10;
- the empirical multivariate distribution of complete age-0–10 composition vectors within each matched stratum.

Complete career-age composition vectors are reassigned across papers within matched strata.

A family-wise Monte Carlo procedure additionally tests whether the observed age-0-to-age-1 transition is larger than the maximum adjacent career-age transition expected anywhere from ages 0–1 through 9–10.

---

## 10. Data

The study uses SciSciNet V2, an integrated science-of-science dataset containing publication, authorship, citation, and field information.

SciSciNet V2 is available from the Center for Science of Science and Innovation at Northwestern University:

https://northwestern-cssi.github.io/sciscinet/

Large source files and derived analysis datasets are not committed to this GitHub repository.

See:

```text
data/README.md
```

for the expected local data structure.

---

## 11. Reproduction order

For a complete reproduction beginning from SciSciNet V2, run the components in this order:

```text
1. load_perquate_to_bq.py

2. prepare_disruption_tables.py

3. export_bq_table.py

4. Download:
   data/disruption_analysis.csv

5. 01_prepare_sciscinet_fields.ipynb

6. 02_prepare_exact_career_age.ipynb

7. 03_manuscript_analysis.ipynb
```

The first three stages perform the large-scale BigQuery preprocessing. The two preparation notebooks generate the additional local SciSciNet V2-derived files required by the final manuscript analysis notebook.

---

## 12. Notes on computational requirements

The preprocessing and analysis operate on tens of millions of papers and large SciSciNet V2 author-publication mappings.

The BigQuery stages require a Google Cloud environment capable of processing the source tables.

The local notebooks are designed for large-data processing using Polars and DuckDB. DuckDB is configured to spill large joins to disk when necessary.

Runtime and storage requirements will depend on the machine, available memory, local disk performance, and whether intermediate files have already been cached.

---

## 13. Reproducibility and provenance

The preparation notebooks record source-file and processing information in local provenance manifests.

The exact-career-age notebook additionally performs explicit validation against the paper-level career-stage variables before producing the final handoff.

The final manuscript notebook contains the executed outputs corresponding to the reported analyses. This allows the numerical results and figures associated with the manuscript to be inspected directly from GitHub without requiring readers to reproduce the complete computational pipeline first.

---

## Citation

If you use this code or analysis workflow, please cite the accompanying manuscript:

> Mahdee Mushfique Kamal and Raiyan Abdul Baten.  
> **Scientific teams with more debutants are disproportionately disruptive.**

Citation information will be updated upon publication.

---

## Authors

**Mahdee Mushfique Kamal**  
Department of Computer Science and Engineering  
Bangladesh University of Engineering and Technology  
Dhaka, Bangladesh

**Raiyan Abdul Baten**  
Bellini College of Artificial Intelligence, Cybersecurity, and Computing  
University of South Florida  
Tampa, Florida, USA

Corresponding author: **rbaten@usf.edu**

---

## License

Please refer to the repository license, if present, for terms governing reuse of the code.

SciSciNet V2 remains subject to the terms and conditions of its original data provider.