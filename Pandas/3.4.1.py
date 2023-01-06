import pandas as pd
from statistics import mean
import math

pd.set_option("expand_frame_repr", False)
df = pd.read_csv("vacancies_dif_currencies.csv")
df_dates = pd.read_csv("cb_currencies.csv")


def handle_salary(date, salary_from, salary_to, salary_currency):
    currency_exchange = 0
    if salary_currency in ["BYN", "BYR", "EUR", "KZT", "UAH", "USD"]:
        salary_currency.replace("BYN", "BYR")
        date = f"{date[1]}-{date[0]}"
        df_date_row = df_dates.loc[df_dates["date"] == date]
        currency_exchange = df_date_row[salary_currency].values[0]
    elif salary_currency == "RUR":
        currency_exchange = 1

    if not (math.isnan(salary_from)) and not (math.isnan(salary_to)):
        mean([salary_from, salary_to]) * currency_exchange
    elif not (math.isnan(salary_from)):
        return salary_from * currency_exchange
    else:
        return salary_to * currency_exchange


df["salary"] = df.apply(lambda row:
                        handle_salary(row["published_at"][:7].split("-"),
                                      row["salary_from"],
                                      row["salary_to"],
                                      row["salary_currency"]),
                        axis=1)
df[:100].to_csv("processed_vacancies.csv", index=False)
