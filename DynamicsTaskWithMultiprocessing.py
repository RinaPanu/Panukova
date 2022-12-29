import multiprocessing
from Splitter import Splitter
from DynamicsCalculator import Calculator

if __name__ == "__main__":
    fileName = input("Введите название файла: ")
    vacancyName = input("Введите название профессии: ")
    splitter = Splitter(fileName)
    dynamicsCalculator = Calculator(vacancyName)
    with multiprocessing.Pool(multiprocessing.cpu_count() * 3) as p:
        p.starmap_async(dynamicsCalculator.GetDynamicsByYear, [(f'CsvFilesByYear\\DataByYear{year}.csv', year) for year in splitter.years], callback=dynamicsCalculator.HandleResults)
        p.close()
        p.join()
    CitiesSalaryData, CitiesRatioData = dynamicsCalculator.GetDynamicsByCity(fileName)
    print("Уровень зарплат по городам (в порядке убывания):", CitiesSalaryData)
    print("Доля вакансий по городам (в порядке убывания):", CitiesRatioData)
