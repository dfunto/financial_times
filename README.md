# Financial Times Pipeline

Financial Times Pipeline is a tool to ingest currency rates from [Fixer API](https://fixer.io/) and store historical data in a local sqlite database. It's also possible to query simple reports from the stored data.

# Prerequisites

Before you begin ensure you have met the following requirements:

* You have Python 3.8 or higher installed
* You have a valid Fixer API account and access Token. Please check: [Fixer Sign Up](https://fixer.io/signup/free)

# Set Up

To set up the pipeline, follow these steps:

1. Install pip dependencies (run this command on root folder) 
    ```bash
    pip install -r requirements.txt
    ```
2. Create .env file in root folder with the following environment variables:

    ```bash
    FIXER_API_TOKEN=ADD_YOU_TOKEN_HERE
    ```

# Usage

Use main.py script as an example, importing one of the following functions from the pipeline_fixer module.

## Function load_hist_rates: 

Runs data pipeline and loads database with selected date range, old data (if any) will be replaced.
    
params:
    
* begin_date: Date range start (format: yyyy-mm-dd)
* end_date: Date range end (format: yyyy-mm-dd)
* *base_currency: Optional base currency as string (default: EUR)*

---

## Function disp_hist_rates: 
    
Read historical data for the selected period if already loaded to database.

params:

* begin_date: Date range start (format: datetime.date)
* end_date: Date range end (format: datetime.date)
* *base_currency: Optional base currency as string (default: EUR)*

---

## Function disp_avg_rates: 
    
Returns average rate for the selected period and currency.

params:

* begin_date: Date range start (format: datetime.date)
* end_date: Date range end (format: datetime.date)
* currency: Currency as string (example: "USD")
* *base_currency: Optional base currency as string (default: EUR)*