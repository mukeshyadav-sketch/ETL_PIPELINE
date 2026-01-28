# User Data ETL Pipeline

## Overview
This project implements a robust ETL (Extract, Transform, Load) pipeline using Python. It fetches user data from an external API, cleans and verifies the data, provides insights, and creates both CSV and SQLite database outputs.

## Features
- **Extract**: Retreives user data from `https://jsonplaceholder.typicode.com/users`.
- **Transform**: Flattens nested JSON structures (address, company details) into a tabular format.
- **Validate**: Performs data quality checks:
  - Detects duplicate User IDs.
  - Validates email formats.
  - Checks for missing critical fields (e.g., City).
  - Validates Zipcode formatting.
- **Insights**: Generates summary statistics on the processed data (total users, unique locations, etc.).
- **Load**:
  - Saves valid records to `valid_users.csv`.
  - Saves rejected records with error details to `rejected_users.csv`.
  - Loads valid records into a SQLite database (`users.db`).
- **Logging**: Tracks pipeline execution and errors in `pipeline.log`.

## Prerequisites
- Python 3.x
- pip

## Installation
1. Clone the repository.
2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage
Run the pipeline using Python:
```bash
python pipeline.py
```

## Output Files
- **valid_users.csv**: CSV file containing all records that passed validation.
- **rejected_users.csv**: CSV file containing records that failed validation, with an 'errors' column explaining the reason.
- **users.db**: SQLite database containing the `users` table with valid data.
- **pipeline.log**: Log file containing execution details and any API errors.

## Database Schema (users table)
- `user_id` (Primary Key)
- `name`
- `username`
- `email`
- `phone`
- `website`
- `city`
- `zipcode`
- `latitude`
- `longitude`
- `company_name`
- `company_phrase`
- `company_bs`
