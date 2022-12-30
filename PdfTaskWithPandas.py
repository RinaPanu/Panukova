from concurrent.futures import ThreadPoolExecutor
from Splitter import Splitter
from DynamicsCalculator import Calculator
from PdfReport import Report
from currenciesParser import CurrenciesParser
import os

if __name__ == "__main__":
    fileName = input("Введите название файла: ")
    vacancyName = input("Введите название профессии: ")
    areaName = input("Введите название региона: ")
    currenciesParser = CurrenciesParser(fileName)
    convertedCurrencies, convertedCurrenciesFile = currenciesParser.ConvertToRub()
    splitter = Splitter(convertedCurrenciesFile, "CsvFilesByYear", "DataByYear")
    dynamicsCalculator = Calculator(vacancyName, areaName)
    with ThreadPoolExecutor(os.cpu_count() * 3) as ex:
        res = ex.map(dynamicsCalculator.GetDynamicsByYear,
                     [f'CsvFilesByYear\\DataByYear{year}.csv' for year in splitter.years], splitter.years)
    generalSalaries, generalCount, vacancySalaries, vacancyCount = dynamicsCalculator.HandleResults(res)
    citiesSalaryData, citiesRatioData = dynamicsCalculator.GetDynamicsByCity(convertedCurrenciesFile)
    data = [generalSalaries, generalCount, vacancySalaries, vacancyCount, citiesSalaryData, citiesRatioData]
    report = Report(vacancyName, areaName)
    report.GeneratePDF(data)