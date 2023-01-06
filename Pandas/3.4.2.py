import pandas as pd
from statistics import mean
import math
import concurrent.futures
import os
import matplotlib.pyplot as plt
from matplotlib.ticker import IndexLocator
from jinja2 import Environment, FileSystemLoader
import pdfkit

pd.set_option("expand_frame_repr", False)


"""
Метод для обработки заработной платы: 
-возврат необходимого значения в зависимости от того, какие значения принимают поля salary_from, salary_to; 
-преобразование в рубли при необходимости
"""
def handle_salary(dates, date, salary_from, salary_to, salary_currency):
    currency_exchange = 0
    if salary_currency in ["BYN", "BYR", "EUR", "KZT", "UAH", "USD"]:
        salary_currency.replace("BYN", "BYR")
        date = f"{date[1]}-{date[0]}"
        df_date_row = dates.loc[df_dates["date"] == date]
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
    salaries_year = int(df["salary"].mean())
    vacancies_count_year = df.shape[0]
    job_dataframe = df[df["name"].str.contains(job_name)]
    job_salary_year = int(job_dataframe["salary"].mean())
    job_vacancies_count_year = job_dataframe.shape[0]
    return [year, salaries_year, vacancies_count_year, job_salary_year, job_vacancies_count_year]

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
Метод для многопроцессорной обработки данных по годам
"""
def get_multiprocess_statistics(job_name, dates):
    files_count = len([x for x in os.listdir("csv_files")])
    with concurrent.futures.ThreadPoolExecutor(max_workers=files_count) as executor:
        futures = [executor.submit(get_year_statistics, os.path.join("csv_files", file_name), job_name, dates) for
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
            years_salaries,
            job_years_salaries,
            years_vacancies_counts,
            job_years_vacancies):

        self.job_name = job_name
        self.years_salaries = years_salaries
        self.job_years_salaries = job_years_salaries
        self.years_vacancies_counts = years_vacancies_counts
        self.job_years_vacancies = job_years_vacancies

    """
    Метод, отвечающий за отрисовку всего полотна с графиками
    """
    def render_graph(self):
        fig, ax = plt.subplots(nrows=2, ncols=2)
        self.__render_years_salaries_graph(ax[0, 0])
        self.__render_years_vacancies_graph(ax[0, 1])
        ax[1, 0].axis('off')
        ax[1, 1].axis('off')
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
    Метод, отвечающий за генерацию отчета в формате .pdf
    """
    def generate_pdf(self):
        env = Environment(loader=FileSystemLoader("."))
        template = env.get_template("pdf_template.html")

        years_headers = ["Год", "Средняя зарплата", f"Средняя зарплата - {self.job_name}", "Количество вакансий",
                       f"Количество вакансий - {self.job_name}"]

        pdf_template = template.render(
            {"job_name": self.job_name,
             "years_salaries": self.years_salaries,
             "years_vacancies_counts": self.years_vacancies_counts,
             "job_years_salaries": self.job_years_salaries,
             "job_years_vacancies": self.job_years_vacancies,
             "years_headers": years_headers})
        config = pdfkit.configuration(wkhtmltopdf=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe')
        pdfkit.from_string(pdf_template, 'report.pdf', configuration=config, options={"enable-local-file-access": None})


if __name__ == "__main__":
    file_name = input("Введите название файла: ")
    job_name = input("Введите название профессии: ")

    separate_csv(file_name)
    df_dates = pd.read_csv("cb_currencies.csv")

    output_data = get_multiprocess_statistics(job_name, df_dates)

    print(f"Динамика уровня зарплат по годам: {output_data[0]}")
    print(f"Динамика количества вакансий по годам: {output_data[2]}")
    print(f"Динамика уровня зарплат по годам для выбранной профессии: {output_data[1]}")
    print(f"Динамика количества вакансий по годам для выбранной профессии: {output_data[3]}")

    report = Report(job_name, output_data[0], output_data[2], output_data[1], output_data[3])
    report.render_graph()
    report.generate_pdf()