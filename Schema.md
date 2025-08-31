# SciSciNet Database Schema

You have access to a database with 6 tables. Here are the table schemas:

## SciSciNet_Authors
- **Columns:** 9
- **Schema:**
  - `authorid` (STRING)
  - `avg_c10` (FLOAT)
  - `avg_logc10` (FLOAT)
  - `productivity` (INTEGER)
  - `h_index` (INTEGER)
  - `display_name` (STRING)
  - `inference_sources` (INTEGER)
  - `inference_counts` (INTEGER)
  - `P_gf` (FLOAT)
  - `debut_year` (INTEGER)

## SciSciNet_Papers
- **Columns:** 28
- **Schema:**
  - `paperid` (STRING)
  - `doi` (STRING)
  - `year` (INTEGER)
  - `date` (STRING)
  - `doctype` (STRING)
  - `cited_by_count` (INTEGER)
  - `is_retracted` (BOOLEAN)
  - `reference_count` (INTEGER)
  - `citation_count` (INTEGER)
  - `C3` (INTEGER)
  - `C5` (INTEGER)
  - `C10` (INTEGER)
  - `disruption` (FLOAT)
  - `Atyp_Median_Z` (FLOAT)
  - `Atyp_10pct_Z` (FLOAT)
  - `Atyp_Pairs` (INTEGER)
  - `WSB_mu` (FLOAT)
  - `WSB_sigma` (FLOAT)
  - `WSB_Cinf` (FLOAT)
  - `SB_B` (FLOAT)
  - `SB_T` (INTEGER)
  - `team_size` (INTEGER)
  - `institution_count` (INTEGER)
  - `patent_count` (INTEGER)
  - `newsfeed_count` (INTEGER)
  - `nct_count` (INTEGER)
  - `nih_count` (INTEGER)
  - `nsf_count` (INTEGER)

## SciSciNet_PaperFields
- **Columns:** 3
- **Schema:**
  - `paperid` (STRING)
  - `fieldid` (STRING)
  - `score_openalex` (FLOAT)

## SciSciNet_Fields
- **Columns:** 12
- **Schema:**
  - `id` (STRING)
  - `wikidata` (STRING)
  - `display_name` (STRING)
  - `level` (INTEGER)
  - `description` (STRING)
  - `works_count` (INTEGER)
  - `cited_by_count` (INTEGER)
  - `image_url` (STRING)
  - `image_thumbnail_url` (STRING)
  - `works_api_url` (STRING)
  - `updated_date` (STRING)
  - `fieldid` (STRING)

## SciSciNet_PaperAuthorAffiliations
- **Columns:** 5
- **Schema:**
  - `paperid` (STRING)
  - `author_position` (STRING)
  - `authorid` (STRING)
  - `institutionid` (STRING)
  - `raw_affiliation_string` (STRING)

## SciSciNet_PaperReferences
- **Columns:** 5
- **Schema:**
  - `citing_paperid` (STRING)
  - `cited_paperid` (STRING)
  - `year` (INTEGER)
  - `ref_year` (INTEGER)
  - `year_diff` (INTEGER)