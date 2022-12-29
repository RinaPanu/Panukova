import multiprocessing
from Splitter import Splitter
from DynamicsCalculator import Calculator

if __name__ == "__main__":
    fileName = input("Введите название файла: ")
    vacancyName = input("Введите название профессии: ")
    splitter = Splitter(fileName)
    dynamicsCalculator = Calculator(vacancyName)
    files = [(f'CsvFilesByYear\\DataByYear{year}.csv', year) for year in splitter.years]
    res = []
    for name, year in files:
        res.append(dynamicsCalculator.GetDynamicsByYear(name, year))
    dynamicsCalculator.HandleResults(res)
    CitiesSalaryData, CitiesRatioData = dynamicsCalculator.GetDynamicsByCity(fileName)
    print("Уровень зарплат по городам (в порядке убывания):", CitiesSalaryData)
    print("Доля вакансий по городам (в порядке убывания):", CitiesRatioData)
