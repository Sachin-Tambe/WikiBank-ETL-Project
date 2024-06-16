# Bank Data ETL Project

This Python project automates the Extraction, Transformation, and Loading (ETL) process for bank data from a specific Wikipedia page into a SQLite database. It utilizes web scraping, data transformation with exchange rates, and database management to facilitate easy analysis and reporting of global bank rankings and assets.

## Features

- **Data Extraction**: Retrieves bank data from a specified Wikipedia URL using BeautifulSoup and requests.
- **Data Transformation**: Converts total assets into multiple currencies using exchange rates from a CSV file.
- **Data Loading**: Stores the transformed data into a SQLite database for easy querying and analysis.
- **Logging**: Records timestamps and activities to monitor the progress and status of the ETL process.

## Prerequisites

- Python 3.x installed
- Libraries: pandas, requests, BeautifulSoup4
- SQLite3

Install the required libraries using pip:

```bash
pip install pandas requests beautifulsoup4
```

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/bank-data-etl.git
   cd bank-data-etl
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. **Configure Parameters**:
   - Open `main.py` and adjust the following variables:
     - `url`: Set this to the desired Wikipedia page URL containing the bank data.
     - `exchange_rate_path`: Path to the `exchange_rate.csv` file with currency conversion rates.

2. **Run the ETL Process**:
   - Execute the main script `main.py` to start the ETL process:

     ```bash
     python main.py
     ```

3. **Output**:
   - Transformed data will be displayed in the console.
   - `transformed_path.csv` will contain CSV-formatted transformed data.
   - `world_bank.db` will store the SQLite database with the transformed bank data.

## Project Structure

- **main.py**: Contains the main logic for ETL operations including extraction, transformation, loading, and logging.
- **exchange_rate.csv**: CSV file with exchange rates for converting bank assets into different currencies.
- **transformed_path.csv**: Output CSV file containing the transformed bank data.
- **world_bank.db**: SQLite database storing the transformed data in structured tables.
