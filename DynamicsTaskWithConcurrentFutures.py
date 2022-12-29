from concurrent.futures import ThreadPoolExecutor
from Splitter import Splitter
from DynamicsCalculator import Calculator
import os

if __name__ == "__main__":
    fileName = input("Введите название файла: ")
    vacancyName = input("Введите название профессии: ")
    splitter = Splitter(fileName)
    dynamicsCalculator = Calculator(vacancyName)
    with ThreadPoolExecutor(os.cpu_count()*3) as ex:
        res = ex.map(dynamicsCalculator.GetDynamicsByYear, [f'CsvFilesByYear\\DataByYear{year}.csv' for year in splitter.years], splitter.years)
    dynamicsCalculator.HandleResults(res)
    CitiesSalaryData, CitiesRatioData = dynamicsCalculator.GetDynamicsByCity(fileName)
    print("Уровень зарплат по городам (в порядке убывания):", CitiesSalaryData)
    print("Доля вакансий по городам (в порядке убывания):", CitiesRatioData)
