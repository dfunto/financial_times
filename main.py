from datetime import date

from pipeline_fixer import load_hist_rates, disp_hist_rates, disp_avg_rates

def main():

    # Parameters
    begin_date = date(2021,3,1)
    end_date = date(2021,3,4)
    base_currency = "EUR"

    # First load some data to database
    load_hist_rates(begin_date, end_date, base_currency)
    print()

    # Display loaded historical rates
    print("Historical Rates")
    disp_hist_rates(begin_date, end_date, base_currency)
    print()
    
    # Display average for selected currency
    print("Average Rates")
    disp_avg_rates(begin_date, end_date, currency="USD", base_currency=base_currency)

if __name__ == "__main__":
    main()