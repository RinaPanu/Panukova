import csv
from datetime import datetime
import math
import re
import matplotlib.pyplot as plt
from matplotlib.ticker import IndexLocator
from jinja2 import Environment, FileSystemLoader
import pdfkit

class FileHandler:
    @staticmethod
    def get_user_input():
        rows = [
            "Введите название файла",
            "Введите название профессии"
        ]
        return [input(f"{rows[i]}: ") for i in range(len(rows))]

    @staticmethod
    def csv_reader(file_name):
        reader = csv.reader(open(file_name, encoding="utf_8_sig"))
        data = [Vacancy(row) for row in list(filter(lambda x: "" not in x, reader))[1:]]
        return data


class Vacancy:
    def __init__(self, vacancy_info):
        self.__currencies_exchanges = {
            "AZN": 35.68,
            "BYR": 23.91,
            "EUR": 59.90,
            "GEL": 21.74,
            "KGS": 0.76,
            "KZT": 0.13,
            "RUR": 1,
            "UAH": 1.64,
            "USD": 60.66,
            "UZS": 0.0055
        }
        if len(vacancy_info) > 6:
            vacancy_info = [vacancy_info[0], vacancy_info[6], vacancy_info[7], vacancy_info[9], vacancy_info[10],
                            vacancy_info[11]]
        self.name = vacancy_info[0]
        self.salary = int(0.5 * self.__currencies_exchanges[vacancy_info[3]] * (
                float(vacancy_info[1]) + float(vacancy_info[2])))
        self.city = vacancy_info[4]
        self.year = int(datetime.strptime(vacancy_info[5], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y'))

class Statistics:
    def __init__(self):
        self.years_salaries = None
        self.years_vacancies_counts = None
        self.job_years_salaries = None
        self.job_years_vacancies = None
        self.cities_salaries = None
        self.cities_vacancies_ratios = None

    def __get_empty_dict_with_keys(self, keys):
        return {x: y for x in keys for y in [0]}

    def prepare(self, vacancies_info, name):
        years = set()
        cities = list()
        for vacancy_info in vacancies_info:
            years.add(vacancy_info.year)
            if vacancy_info.city not in cities:
                cities.append(vacancy_info.city)
        years = sorted(years)
        years_salaries = self.__get_empty_dict_with_keys(years)
        job_years_salaries = self.__get_empty_dict_with_keys(years)
        years_vacancies_counts = self.__get_empty_dict_with_keys(years)
        job_years_vacancies = self.__get_empty_dict_with_keys(years)
        cities_salaries = self.__get_empty_dict_with_keys(cities)
        cities_vacancies = self.__get_empty_dict_with_keys(cities)

        for vacancy_info in vacancies_info:
            years_salaries[vacancy_info.year] += vacancy_info.salary
            years_vacancies_counts[vacancy_info.year] += 1
            cities_salaries[vacancy_info.city] += vacancy_info.salary
            cities_vacancies[vacancy_info.city] += 1
            if name in vacancy_info.name:
                job_years_vacancies[vacancy_info.year] += 1
                job_years_salaries[vacancy_info.year] += vacancy_info.salary

        for year in years:
            if years_vacancies_counts[year] > 0:
                years_salaries[year] = years_salaries[year] // years_vacancies_counts[year]
            if job_years_vacancies[year] > 0:
                job_years_salaries[year] = job_years_salaries[year] // job_years_vacancies[year]
        for city in cities:
            cities_salaries[city] = cities_salaries[city] // cities_vacancies[city]

        vacancies_count = len(vacancies_info)

        proper_cities_salaries = dict()
        for city_salary in cities_salaries.items():
            if math.floor(100 * cities_vacancies[city_salary[0]] / vacancies_count) >= 1:
                proper_cities_salaries.update({city_salary[0]: city_salary[1]})

        cities_vacancies_ratios = dict()
        for city_vacancy in cities_vacancies.items():
            if math.floor(100 * city_vacancy[1] / vacancies_count) >= 1:
                cities_vacancies_ratios.update(
                    {city_vacancy[0]: round(city_vacancy[1] / vacancies_count, 4)})

        self.years_salaries = years_salaries
        self.years_vacancies_counts = years_vacancies_counts
        self.job_years_salaries = job_years_salaries
        self.job_years_vacancies = job_years_vacancies
        slice_end = 10 if len(proper_cities_salaries.items()) > 10 else len(proper_cities_salaries.items())
        self.cities_salaries = dict(
            sorted(proper_cities_salaries.items(), key=lambda x: x[1], reverse=True)[:slice_end])
        slice_end = 10 if len(cities_vacancies_ratios.items()) > 10 else len(cities_vacancies_ratios.items())
        self.cities_vacancies_ratios = dict(
            sorted(cities_vacancies_ratios.items(), key=lambda x: x[1], reverse=True)[:slice_end])
        if len(cities_vacancies_ratios.items()) > 10:
            self.cities_vacancies_ratios.update({"Другие": round(1 - sum(self.cities_vacancies_ratios.values()), 4)})

    def print(self):
        print(f"Динамика уровня зарплат по годам: {self.years_salaries}")
        print(f"Динамика количества вакансий по годам: {self.years_vacancies_counts}")
        print(f"Динамика уровня зарплат по годам для выбранной профессии: {self.job_years_salaries}")
        print(f"Динамика количества вакансий по годам для выбранной профессии: {self.job_years_vacancies}")
        print(f"Уровень зарплат по городам (в порядке убывания): {self.cities_salaries}")
        print(f"Доля вакансий по городам (в порядке убывания): {self.cities_vacancies_ratios}")

    def get_prepared_statistics(self):
        return self.years_salaries, \
               self.job_years_salaries, \
               self.years_vacancies_counts, \
               self.job_years_vacancies, \
               self.cities_salaries, \
               self.cities_vacancies_ratios

class Report:
    def __init__(self,
            job_name,
            years_salaries,
            job_years_salaries,
            years_vacancies_counts,
            job_years_vacancies,
            cities_salaries,
            cities_vacancies_ratios):

        self.job_name = job_name
        self.years_salaries = years_salaries
        self.job_years_salaries = job_years_salaries
        self.years_vacancies_counts = years_vacancies_counts
        self.job_years_vacancies = job_years_vacancies
        self.cities_salaries = cities_salaries
        self.cities_vacancies_ratios = cities_vacancies_ratios

    def render_graph(self):
        fig, ax = plt.subplots(2, 2)
        self.__render_years_salaries_graph(ax[0, 0])
        self.__render_years_vacancies_graph(ax[0, 1])
        self.__render_cities_salaries_graph(ax[1, 0])
        self.__render_cities_vacancies_ratios_graph(ax[1, 1])
        plt.tight_layout()
        plt.savefig("graph.png")
        plt.show()

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
        ax.yaxis.set_major_locator(IndexLocator(base=10000, offset=0))

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

    def __render_cities_vacancies_ratios_graph(self, ax):
        ax.set_title("Доля вакансий по городам")
        reversed_cities_vacancies_ratios = dict(sorted(self.cities_vacancies_ratios.items(), key=lambda item: item[1], reverse=True))
        cities = reversed_cities_vacancies_ratios.keys()
        ratios = reversed_cities_vacancies_ratios.values()
        ax.pie(ratios, labels=cities, textprops={'fontsize': 6})

    def generate_pdf(self):
        env = Environment(loader=FileSystemLoader("."))
        template = env.get_template("pdf_template.html")

        years_headers = ["Год", "Средняя зарплата", f"Средняя зарплата - {self.job_name}", "Количество вакансий",
                       f"Количество вакансий - {self.job_name}"]
        cities_headers = ["Город", "Уровень зарплат", "Город", "Доля вакансий"]

        cities_vacancies_ratios = dict([(k, f"{v:.2%}") for k, v in list(self.cities_vacancies_ratios.items())[:10]])

        pdf_template = template.render(
            {"job_name": self.job_name, "years_salaries": self.years_salaries, "years_vacancies_counts": self.years_vacancies_counts,
             "job_years_salaries": self.job_years_salaries,
             "job_years_vacancies": self.job_years_vacancies,
             "cities_salaries": self.cities_salaries,
             "cities_vacancies_ratios": cities_vacancies_ratios,
             "years_headers": years_headers,
             "cities_headers": cities_headers})
        config = pdfkit.configuration(wkhtmltopdf=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe')
        pdfkit.from_string(pdf_template, 'report.pdf', configuration=config, options={"enable-local-file-access": None})


user_input = FileHandler.get_user_input()
file_name = user_input[0]
vacancy_name = user_input[1]

vacancies_info = FileHandler.csv_reader(file_name)
statistics = Statistics()
statistics.prepare(vacancies_info, vacancy_name)
statistics.print()

report = Report(vacancy_name, *statistics.get_prepared_statistics())
report.render_graph()
report.generate_pdf()
