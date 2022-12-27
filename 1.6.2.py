import sys
from datetime import datetime
from prettytable import *
import csv
import re
import os


class InputConnect:
    __formatFuncs = {"name": lambda vacancy: vacancy.name,
                     "description": lambda vacancy: vacancy.description,
                     "key_skills": lambda vacancy: vacancy.keySkills,
                     "experience_id": lambda vacancy: DataSet._experience[vacancy.experienceId],
                     "premium": lambda vacancy: DataSet._boolFields[vacancy.premium],
                     "employer_name": lambda vacancy: vacancy.employerName,
                     "salary": lambda vacancy: vacancy.salary.Format(),
                     "area_name": lambda vacancy: vacancy.areaName,
                     "published_at": lambda vacancy: datetime.strptime(vacancy.publishedAt,
                                                                       '%Y-%m-%dT%H:%M:%S%z').strftime(
                         '%d.%m.%Y')}
    __requests = {"Введите название файла: ": lambda fileName: fileName,
                  "Введите параметр фильтрации: ": lambda filterParameter: InputConnect._SetFilterParameter(
                      filterParameter),
                  "Введите параметр сортировки: ": lambda sortParameter: InputConnect._SetSortParameter(sortParameter),
                  "Обратный порядок сортировки (Да / Нет): ": lambda isReverseSort: InputConnect._SetReverseSortStatus(
                      isReverseSort),
                  "Введите диапазон вывода: ": lambda filterParameter: filterParameter.split(),
                  "Введите требуемые столбцы: ": lambda filterParameter: filterParameter.split(", ")}

    fieldNames = {"name": "Название",
                  "description": "Описание",
                  "key_skills": "Навыки",
                  "experience_id": "Опыт работы",
                  "premium": "Премиум-вакансия",
                  "employer_name": "Компания",
                  "salary_from": "Нижняя граница вилки оклада",
                  "salary_to": "Верхняя граница вилки оклада",
                  "salary_gross": "Оклад указан до вычета налогов",
                  "salary_currency": "Идентификатор валюты оклада",
                  "area_name": "Название региона",
                  "published_at": "Дата публикации вакансии",
                  "salary": "Оклад"}

    correctFields = ["name", "description", "key_skills", "experience_id", "premium", "employer_name", "salary",
                     "area_name", "published_at"]

    def __init__(self):
        self.fileName, self.filterParameter, self.sortParameter, self.isReverseSort, self.outputRange, self.outputColumns = self.__GetData()

    def Initialize(self, vacancies):
        self.table = PrettyTable(hrules=ALL, align='l')
        self.table.field_names = [self.fieldNames[name] for name in self.correctFields]
        self.table.max_width = 20

        self.start, self.end = self.__SetRange(vacancies, self.outputRange)
        self.outputColumns = list(set(self.table.field_names) & set(self.outputColumns)) + ["№"] if any(
            self.outputColumns) else self.table.field_names

    def PrintDataSet(self, dataSet):
        outputVacancies = [self.__Formatter(vacancy)
                           for vacancy in dataSet.vacanciesObjects]
        self.table.add_rows(outputVacancies)
        self.table.add_autoindex("№")
        outputTable = self.table.get_string(start=self.start, end=self.end, fields=self.outputColumns)
        if self.start < len(dataSet.vacanciesObjects) > 0:
            print(outputTable)
        else:
            print("Ничего не найдено")
            sys.exit()

    def __Formatter(self, vacancy):
        newVacancy = {nameFunc: self.__formatFuncs[nameFunc](vacancy) for nameFunc in self.correctFields}
        newVacancy = self.__SetMaxChars(newVacancy).values()
        return newVacancy

    def __SetRange(self, vacancies, rangeRows):
        countRows = len(vacancies)
        start = int(rangeRows[0]) - 1 if any(rangeRows) else 0
        end = int(rangeRows[1]) - 1 if len(rangeRows) == 2 else countRows
        return start, end

    def __GetData(self):
        data = {}
        for request in InputConnect.__requests.keys():
            data[request] = input(request)
        data = [InputConnect.__requests[item[0]](item[1]) for item in data.items()]
        return data

    def __SetMaxChars(self, vacancy):
        newVacancy = vacancy.copy()
        for key in vacancy.keys():
            if len(vacancy[key]) > 100:
                newVacancy[key] = vacancy[key][:100] + "..."
        return newVacancy

    @staticmethod
    def _SetFilterParameter(filterParameter):
        if filterParameter == "":
            return filterParameter
        elif ": " not in filterParameter:
            print("Формат ввода некорректен")
            sys.exit()
        elif filterParameter.split(": ", 1)[0] not in DataSet._filterFuncs:
            print("Параметр поиска некорректен")
            sys.exit()
        else:
            filterParameter = filterParameter.split(": ", 1)
        return filterParameter

    @staticmethod
    def _SetSortParameter(sortParameter):
        if sortParameter not in DataSet._sortFuncs and sortParameter != "":
            print("Параметр сортировки некорректен")
            sys.exit()
        return sortParameter

    @staticmethod
    def _SetReverseSortStatus(isReverseSort):
        if isReverseSort == "":
            return False
        elif isReverseSort in ["Да", "Нет"]:
            isReverseSort = isReverseSort == "Да"
            return isReverseSort
        else:
            print("Порядок сортировки задан некорректно")
            sys.exit()


class DataSet:
    _sortFuncs = {"Название": lambda vacancy: vacancy.name,
                  "Описание": lambda vacancy: vacancy.description,
                  "Навыки": lambda vacancy: len(vacancy.keySkills.split("\n")),
                  "Опыт работы": lambda vacancy: DataSet._experienceSort[vacancy.experienceId],
                  "Премиум-вакансия": lambda vacancy: vacancy.premium,
                  "Компания": lambda vacancy: vacancy.employerName,
                  "Оклад": lambda vacancy: vacancy.salary.Sort(),
                  "Идентификатор валюты оклада": lambda vacancy: vacancy.salary.salaryCurrency,
                  "Название региона": lambda vacancy: vacancy.areaName,
                  "Дата публикации вакансии": lambda vacancy: datetime.strptime(vacancy.publishedAt,
                                                                                '%Y-%m-%dT%H:%M:%S%z')}
    _filterFuncs = {"Название": lambda expectedName, vacancy: expectedName == vacancy.name,
                    "Описание": lambda expectedDesc, vacancy: expectedDesc == vacancy.description,
                    "Навыки": lambda expectedSkills, vacancy: set(expectedSkills.split(", ")) <= set(
                        vacancy.keySkills.split("\n")),
                    "Опыт работы": lambda expectedExp, vacancy: expectedExp == DataSet._experience[
                        vacancy.experienceId],
                    "Премиум-вакансия": lambda expectedPrem, vacancy: expectedPrem == DataSet._boolFields[
                        vacancy.premium],
                    "Компания": lambda expectedEmployer, vacancy: expectedEmployer == vacancy.employerName,
                    "Оклад": lambda expectedSalary, vacancy: vacancy.salary.SumFilter(expectedSalary),
                    "Идентификатор валюты оклада": lambda expectedCurrency, vacancy: vacancy.salary.CurrencyFilter(
                        expectedCurrency),
                    "Название региона": lambda expectedArea, vacancy: expectedArea == vacancy.areaName,
                    "Дата публикации вакансии": lambda expectedDate, vacancy: expectedDate == datetime.strptime(
                        vacancy.publishedAt,
                        '%Y-%m-%dT%H:%M:%S%z').strftime(
                        '%d.%m.%Y')}
    _experience = {"noExperience": "Нет опыта",
                   "between1And3": "От 1 года до 3 лет",
                   "between3And6": "От 3 до 6 лет",
                   "moreThan6": "Более 6 лет"}

    _experienceSort = {"noExperience": 0,
                       "between1And3": 1,
                       "between3And6": 2,
                       "moreThan6": 3}

    _boolFields = {"True": "Да",
                   "False": "Нет"}

    _reverseFieldNames = {v: k for k, v in InputConnect.fieldNames.items()}

    def __init__(self, inputData):
        self.fileName = inputData.fileName
        self.__UniversalParserCSV(inputData)



    def __UniversalParserCSV(self, inputData):
        filterParameter = inputData.filterParameter
        sortParameter = inputData.sortParameter
        isReverseSort = inputData.isReverseSort

        fileReader, columnNames = self.__CsvReader(inputData.fileName)
        self.vacanciesObjects = self.__CsvFilter(fileReader, columnNames)

        inputData.Initialize(self.vacanciesObjects)
        self.vacanciesObjects = self.__SortVacancies(sortParameter, self.vacanciesObjects, isReverseSort)
        self.vacanciesObjects = self.__FilterVacancies(filterParameter, self.vacanciesObjects)

    def __CleanRow(self, row):
        cleaner = re.compile('<.*?>')
        clearedRow = re.sub(cleaner, '', row)
        clearedRow = "; ".join(clearedRow.split('\n'))
        clearedRow = "".join(clearedRow.split('\r'))
        clearedRow = " ".join(clearedRow.split())
        return clearedRow

    def __CsvReader(self, fileName):
        file = open(fileName, encoding='utf-8-sig', newline='')
        if os.stat(fileName).st_size == 0:
            print("Пустой файл")
            sys.exit()
        fileReader = csv.DictReader(file)
        columnNames = fileReader.fieldnames
        return fileReader, columnNames

    def __CsvFilter(self, fileReader, columnNames):
        vacancies = []
        columnsCount = len(columnNames)

        for row in fileReader:
            if all(row.values()) and columnsCount == len(row):
                tempRow = {columnNames[i]: self.__CleanRow(row[columnNames[i]]) for i in range(columnsCount)}
                tempRow['salary_from'] = Salary(tempRow['salary_from'], tempRow.pop('salary_to'),
                                                tempRow.pop("salary_currency"), tempRow.pop("salary_gross"))
                tempRow['key_skills'] = "\n".join(tempRow['key_skills'].split("; "))
                vacancies.append(Vacancy(*tempRow.values()))

        if len(vacancies) == 0:
            print("Нет данных")
            sys.exit()
        return vacancies

    def __FilterVacancies(self, filterParameter, vacancies):
        newVacancies = []
        for vacancy in vacancies:
            if not filterParameter:
                newVacancies.append(vacancy)
            else:
                expectedParameter = filterParameter[0]
                if self._filterFuncs[expectedParameter](filterParameter[1], vacancy):
                    newVacancies.append(vacancy)
        return newVacancies

    def __SortVacancies(self, sortParameter, vacancies, isReverseSort):

        newVacancies = vacancies
        if sortParameter:
            newVacancies.sort(key=self._sortFuncs[sortParameter], reverse=isReverseSort)
        return newVacancies


class Vacancy:
    def __init__(self, name, description, keySkills, experienceId, premium, employerName, salary, areaName,
                 publishedAt):
        self.name, self.description, self.keySkills, self.experienceId, self.premium, self.employerName, self.salary, self.areaName, self.publishedAt \
            = name, description, keySkills, experienceId, premium, employerName, salary, areaName, publishedAt


class Salary:
    _salaryCurrency = {"AZN": "Манаты",
                       "BYR": "Белорусские рубли",
                       "EUR": "Евро",
                       "GEL": "Грузинский лари",
                       "KGS": "Киргизский сом",
                       "KZT": "Тенге",
                       "RUR": "Рубли",
                       "UAH": "Гривны",
                       "USD": "Доллары",
                       "UZS": "Узбекский сум"}

    _salaryGross = {"True": "Без вычета налогов",
                    "False": "С вычетом налогов"}

    currencyToRub = {
        "AZN": 35.68,
        "BYR": 23.91,
        "EUR": 59.90,
        "GEL": 21.74,
        "KGS": 0.76,
        "KZT": 0.13,
        "RUR": 1,
        "UAH": 1.64,
        "USD": 60.66,
        "UZS": 0.0055,
    }

    __formatFuncsSalary = {"salaryFrom": lambda fr: f'{"{:,}".format(int(float(fr))).replace(",", " ")} - ',
                           "salaryTo": lambda to: f'{"{:,}".format(int(float(to))).replace(",", " ")} ',
                           "salaryCurrency": lambda currency: f'({Salary._salaryCurrency[currency]}) ',
                           "salaryGross": lambda gross: f'({Salary._salaryGross[gross]})',
                           }

    def __init__(self, salaryFrom, salaryTo, salaryCurrency, salaryGross):
        self.salaryFrom, self.salaryTo, self.salaryCurrency, self.salaryGross = salaryFrom, salaryTo, salaryCurrency, salaryGross

    def Format(self):
        salary = ""
        for name in self.__dict__.keys():
            salary += self.__formatFuncsSalary[name](self.__dict__[name])
        return salary

    def ChangeCurrency(self, salary):
        return salary * self.currencyToRub[self.salaryCurrency]

    def Sort(self):
        ch = int(float(self.salaryFrom)) + int(float(self.salaryTo)) / 2
        return self.ChangeCurrency((int(float(self.salaryFrom)) + int(float(self.salaryTo))) / 2)

    def SumFilter(self, expectedSum):
        return int(float(self.salaryTo)) >= int(float(expectedSum)) >= int(float(self.salaryFrom))

    def CurrencyFilter(self, expectedCurrency):
        return expectedCurrency == self._salaryCurrency[self.salaryCurrency]


inputData = InputConnect()
dataSet = DataSet(inputData)
inputData.PrintDataSet(dataSet)