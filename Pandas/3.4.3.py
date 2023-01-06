import pandas as pd
from statistics import mean
import math
import concurrent.futures
import os
import matplotlib.pyplot as plt
from matplotlib.ticker import IndexLocator
from jinja2 import Environment, FileSystemLoader
import pdfkit
import re

pd.set_option("expand_frame_repr", False)


"""
Метод для обработки заработной платы: 
-возврат необходимого значения в зависимости от того, какие значения принимают поля salary_from, salary_to; 
-преобразование в рубли при необходимости
"""
def handle_salary(dates_currencies, date, salary_from, salary_to, salary_currency):
    currency_exchange = 0
    if salary_currency in ["BYN", "BYR", "EUR", "KZT", "UAH", "USD"]:
        salary_currency.replace("BYN", "BYR")
        date = f"{date[1]}-{date[0]}"
        df_date_row = dates_currencies.loc[df_dates_currencies["date"] == date]
        currency_exchange = df_date_row[salary_currency].values[0]
    elif salary_currency == "RUR":
        currency_exchange = 1

    if not (math.isnan(salary_from)) and not (math.isnan(salary_to)):
        mean([salary_from, salary_to]) * currency_exchange
    elif not (math.isnan(salary_from)):
        return salary_from * currency_exchange
    else:
        return salary_to * currency_exchange


"""
Метод для получения статистики за отдельно взятый год
"""
def get_year_statistics(file_name, job_name, dates):
    year = file_name[-8:-4]
    df = pd.read_csv(file_name)
    df["salary"] = df.apply(lambda row:
                            handle_salary(dates,
                                          row["published_at"][:7].split("-"),
                                          row["salary_from"],
                                          row["salary_to"],
                                          row["salary_currency"]),
                            axis=1)

    df = df[df["salary"].notnull()]
    year_salaries = int(df["salary"].mean())
    year_vacancies_count = df.shape[0]
    job_dataframe = df[df["name"].str.contains(job_name)]
    year_job_salaries = int(job_dataframe["salary"].mean())
    year_job_vacancies_count = job_dataframe.shape[0]
    return [year, year_salaries, year_vacancies_count, year_job_salaries, year_job_vacancies_count]

"""
Метод для разделения исходного файла на более мелкие по годам
"""
def separate_csv(file_name):
    df = pd.read_csv(file_name)
    df["years"] = df["published_at"].apply(lambda s: s[0:4])
    years = df["years"].unique()

    if not os.path.exists("csv_files"):
        os.mkdir("csv_files")
    for year in years:
        data = df[df["years"] == year]
        data.iloc[:, :6].to_csv(rf"csv_files\part_{year}.csv", index=False)

"""
Метод для однопроцессной обработки данных о зарплатах по городам
"""
def get_singleprocess_statistics(file_name, job_name, area_name, dates_currencies):
    df = pd.read_csv(file_name)
    df["year"] = df.apply(lambda row: row["published_at"][0:4], axis=1)
    years = df["year"].unique()
    df["salary"] = df.apply(lambda row:
                            handle_salary(dates_currencies,
                                          row["published_at"][:7].split("-"),
                                          row["salary_from"],
                                          row["salary_to"],
                                          row["salary_currency"]),
                            axis=1)
    df = df[df["salary"].notnull()]
    df["count"] = df.groupby("area_name")["area_name"].transform("count")
    total_vacancies_count = df.shape[0]
    cities_salaries = {}
    cities_vacancies_ratios = {}
    years_job_salaries = {}
    years_job_vacancies_count = {}


    # Уровень зарплат по городам (в порядке убывания) - только первые 10 значений
    # Доля вакансий по городам (в порядке убывания) - только первые 10 значений
    proper_cities_df = df[df["count"] >= total_vacancies_count * 0.01]

    cities = proper_cities_df["area_name"].unique()

    for city in cities:
        city_df = proper_cities_df[proper_cities_df["area_name"] == city]
        cities_salaries[city] = int(city_df["salary"].mean())
        cities_vacancies_ratios[city] = round(city_df.shape[0] / total_vacancies_count, 4)

    # Динамика уровня зарплат по годам для выбранной профессии и региона
    # Динамика количества вакансий по годам для выбранной профессии и региона
    job_df = df[df["name"].str.contains(job_name)]
    for year in years:
        year_df = job_df[(job_df["year"] == year) & (job_df["area_name"] == area_name)]
        if year_df.shape[0] > 0:
            years_job_salaries[year] = int(year_df["salary"].mean())
            years_job_vacancies_count[year] = year_df.shape[0]

    slice_end = 10 if len(cities_salaries.items()) > 10 else len(cities_salaries.items())
    cities_salaries = dict(
        sorted(cities_salaries.items(), key=lambda x: x[1], reverse=True)[:slice_end])
    slice_end = 10 if len(cities_vacancies_ratios.items()) > 10 else len(cities_vacancies_ratios.items())

    temp_len = len(cities_vacancies_ratios)
    cities_vacancies_ratios = dict(
        sorted(cities_vacancies_ratios.items(), key=lambda x: x[1], reverse=True)[:slice_end])

    if temp_len > 10:
        cities_vacancies_ratios.update({"Другие": round(1 - sum(cities_vacancies_ratios.values()), 4)})

    return [
        cities_salaries,
        cities_vacancies_ratios,
        years_job_salaries,
        years_job_vacancies_count,
    ]


"""
Метод для многопроцессной обработки данных по годам
"""
def get_multiprocess_statistics(job_name, dates_currencies):
    files_count = len([x for x in os.listdir("csv_files")])
    with concurrent.futures.ThreadPoolExecutor(max_workers=files_count) as executor:
        futures = [executor.submit(get_year_statistics, os.path.join("csv_files", file_name), job_name, dates_currencies) for
                   file_name in
                   os.listdir("csv_files")]
    output = [future.result() for future in concurrent.futures.as_completed(futures)]
    result = [{} for _ in range(4)]
    for year_data in output:
        for i in range(4):
            result[i][year_data[0]] = year_data[i + 1]

    for i in range(len(result)):
        result[i] = dict(sorted(result[i].items(), key=lambda x: x[0]))
    return result


"""
Класс для генерации графиков в формате .png и отчета в формате .png
"""
class Report:
    def __init__(self,
                 job_name,
                 area_name,
                 years_salaries,
                 years_vacancies_counts,
                 cities_salaries,
                 job_years_salaries,
                 job_years_vacancies_count,
                 cities_vacancies_ratios,
                 years_job_city_salaries,
                 years_job_city_vacancies_count
                 ):

        self.job_name = job_name
        self.area_name = area_name
        self.years_salaries = years_salaries
        self.job_years_salaries = job_years_salaries
        self.years_vacancies_counts = years_vacancies_counts
        self.job_years_vacancies = job_years_vacancies_count
        self.cities_salaries = cities_salaries
        self.cities_vacancies_ratios = cities_vacancies_ratios
        self.years_job_city_salaries = years_job_city_salaries
        self.years_job_city_vacancies_count = years_job_city_vacancies_count

    """
    Метод, отвечающий за отрисовку всего полотна с графиками
    """
    def render_graph(self):
        fig, ax = plt.subplots(nrows=2, ncols=2)
        self.__render_years_salaries_graph(ax[0, 0])
        self.__render_years_vacancies_graph(ax[0, 1])
        self.__render_cities_salaries_graph(ax[1, 0])
        self.__render_cities_vacancies_ratios_graph(ax[1, 1])
        plt.tight_layout()
        plt.savefig("graph.png")

    """
    Метод, отвечающий за отрисовку графика, включающего в себя данные об уровне зарплат по годам в целом и для выбранной профессии 
    """
    def __render_years_salaries_graph(self, ax):
        ax.set_title("Уровень зарплат по годам")
        width = 0.4
        years = self.years_salaries.keys()
        salaries = self.years_salaries.values()
        ax.bar([i - width / 2 for i in range(len(years))],
               salaries,
               width=width,
               label="средняя з/п")

        job_salaries = self.job_years_salaries.values()
        ax.bar([i + width / 2 for i in range(len(years))],
               job_salaries,
               width=width,
               label=f"з/п {self.job_name}")
        ax.set_xticks(range(len(years)), years, rotation="vertical")
        ax.tick_params(axis="both", labelsize=8)
        ax.legend(fontsize=8)
        ax.yaxis.set_major_locator(IndexLocator(base=100000, offset=0))

    """
    Метод, отвечающий за отрисовку графика, включающего в себя данные о количестве вакансий по годам в целом и для выбранной профессии
    """
    def __render_years_vacancies_graph(self, ax):
        ax.set_title("Количество вакансий по годам")
        width = 0.4
        years = self.years_vacancies_counts.keys()
        vacancies = self.years_vacancies_counts.values()
        ax.bar([i - width / 2 for i in range(len(years))],
               vacancies,
               width=width,
               label="Количество вакансий")
        job_vacancies = self.job_years_vacancies.values()
        ax.bar([i + width / 2 for i in range(len(years))],
               job_vacancies,
               width=width,
               label=f"Количество вакансий\n{self.job_name}")
        ax.set_xticks(range(len(years)),
                      years,
                      rotation="vertical")
        ax.tick_params(axis="both", labelsize=8)
        ax.legend(fontsize=8, loc='upper left')

    """
    Метод, отвечающий за отрисовку графика с уровнями зарплат по годам
    """
    def __render_cities_salaries_graph(self, ax):
        ax.set_title("Уровень зарплат по городам")
        cities_salaries = self.cities_salaries
        cities = cities_salaries.keys()
        salaries = cities_salaries.values()
        y_pos = range(len(self.cities_salaries))
        cities = [re.sub(r"[- ]", "\n", city) for city in cities]
        ax.barh(y_pos, salaries)
        ax.set_yticks(y_pos, cities)
        ax.invert_yaxis()
        ax.tick_params(axis="x", labelsize=8)
        ax.tick_params(axis="y", labelsize=6)

    """
    Метод, отвечающий за отрисовку графика с долями 
    """
    def __render_cities_vacancies_ratios_graph(self, ax):
        ax.set_title("Доля вакансий по городам")
        reversed_cities_vacancies_ratios = dict(sorted(self.cities_vacancies_ratios.items(), key=lambda item: item[1], reverse=True))
        cities = reversed_cities_vacancies_ratios.keys()
        ratios = reversed_cities_vacancies_ratios.values()
        ax.pie(
            ratios,
            labels=cities,
            textprops={'fontsize': 6},
            colors=["#ff8006", "#28a128", "#1978b5", "#0fbfd0", "#bdbe1c", "#808080",
                    "#e478c3", "#8d554a", "#9567be", "#d72223", "#1978b5", "#ff8006"])

    """
    Метод, отвечающий за генерацию отчета в формате .pdf
    """
    def generate_pdf(self):
        env = Environment(loader=FileSystemLoader("."))
        template = env.get_template("pdf_template.html")

        years_headers = ["Год",
                         f"Средняя зарплата - {self.job_name}, регион - {self.area_name}",
                         f"Количество вакансий - {self.job_name}, регион - {self.area_name}"]

        pdf_template = template.render(
            {"job_name": self.job_name,
             "job_years_salaries": self.job_years_salaries,
             "job_years_vacancies": self.job_years_vacancies,
             "years_job_city_salaries": self.years_job_city_salaries,
             "years_job_city_vacancies_count": self.years_job_city_vacancies_count,
             "years_headers": years_headers})
        config = pdfkit.configuration(wkhtmltopdf=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe')
        pdfkit.from_string(pdf_template, 'report.pdf', configuration=config, options={"enable-local-file-access": None})


if __name__ == "__main__":
    file_name = input("Введите название файла: ")
    job_name = input("Введите название профессии: ")
    area_name = input("Введите название региона: ")

    separate_csv(file_name)
    df_dates_currencies = pd.read_csv("cb_currencies.csv")

    output_multiprocess_data = get_multiprocess_statistics(job_name, df_dates_currencies)
    output_singleprocess_data = get_singleprocess_statistics(file_name, job_name, area_name, df_dates_currencies)
    report = Report(job_name, area_name, output_multiprocess_data[0], output_multiprocess_data[1], output_singleprocess_data[0], output_multiprocess_data[2], output_multiprocess_data[3], output_singleprocess_data[1], output_singleprocess_data[2], output_singleprocess_data[3])
    report.render_graph()
    report.generate_pdf()