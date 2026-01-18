!/bin/bash

# This script runs the analysis for beginner authors in research publications.
time python3 perform_statistical_analysis.py
time python3 perform_field_based_analysis.py
time python3 perform_mid_career_analysis.py