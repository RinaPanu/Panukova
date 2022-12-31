import pandas as pd
import requests
from xml.etree import ElementTree as ET
from dataBase import DataBase as DB


class CurrenciesParser:

    def __init__(self, fileName):
        self.fileName = fileName
        df = pd.read_csv(self.fileName)
        self.df = self.ApplyPreselection(df)
        self.conversionTable = self.CreateConversionTable(self.df)
        self.dbController = DB("ConversionTable")
        self.dbController.CreateDataBase(self.conversionTable,
                                         "date text, USD float, EUR float, KZT float, UAH float, BYR float", True)
        self.dbCursor = self.dbController.OpenDB()

    def GetCurrenciesRatio(self, df):
        df = df.copy()
        df = df.dropna(how="all", subset=["salary_from", "salary_to"])
        df["CurrenciesRatio"] = df.groupby("salary_currency")["salary_currency"].transform("count")
        print(df['salary_currency'].value_counts())
        return df

    def GetRangePublications(self, df):
        firstPublication = df['published_at'].min()
        lastPublication = df['published_at'].max()
        dateRange = [f'{str(date)[0:7]}' for date in
                     pd.date_range(firstPublication, pd.Timestamp(lastPublication) + pd.offsets.MonthEnd(0), freq="M",
                                   normalize=True)]
        return dateRange

    def CreateConversionTable(self, df):
        dateRange = self.GetRangePublications(df)
        currenciesNames = [curr for curr in df['salary_currency'].unique() if curr != "RUR"]
        currencyDf = pd.DataFrame(index=dateRange, columns=currenciesNames)
        currencyDf.index.names = ["date"]
        for date in dateRange:
            y, m = date[0:4], date[5:7]
            response = requests.get(f'http://www.cbr.ru/scripts/XML_daily.asp?date_req=01/{m}/{y}d1')
            tree = ET.fromstring(response.content)
            for curr in tree.iter("Valute"):
                currName = curr.find("CharCode").text
                if currName in currenciesNames:
                    currencyDf.at[date, currName] = float(curr.find('Value').text.replace(',', '.')) / float(
                        curr.find('Nominal').text)

        currencyDf.to_csv("ConversionTable.csv")
        return currencyDf

    def ApplyPreselection(self, df):
        df = self.GetCurrenciesRatio(df)
        df = df[df['CurrenciesRatio'] > 5000]
        df.drop(columns="CurrenciesRatio")
        return df

    def ConvertToRub(self, returnFormat):
        # df = pd.read_csv(self.fileName)
        # df = self.ApplyPreselection(df)
        # conversionTable = self.CreateConversionTable(df)
        # df["salary"] = df[["salary_from", "salary_to"]].mean(axis=1)
        # currRange = df[df["salary_currency"]!="RUR"].groupby(["published_at", "salary_currency"])
        # i=0
        # for (date, currName), tempDf in currRange:
        #     df["salary"].update(tempDf["salary"].transform(lambda x: x * conversionTable.at[date[0:7], currName]))
        #     i+=1
        #     print(i)
        # df.loc[:,["name", "salary", "area_name","published_at"]].to_csv("ConvertedVacancies.csv", index=False)
        # return df
        # df = pd.read_csv(self.fileName)
        # df = self.ApplyPreselection(df)
        # conversionTable = self.CreateConversionTable(df)
        # df["salary"] = df[["salary_from", "salary_to"]].mean(axis=1)
        # df.index = df["published_at"].transform(lambda x: x[0:7])+df["salary_currency"]
        # currRange = df[df["salary_currency"]!="RUR"].groupby(["published_at", "salary_currency"])
        # i=0
        # for (date, currName), tempDf in currRange:
        #     df.at[f'{date[0:7]}{currName}', "salary"] *= conversionTable.at[date[0:7], currName]
        #     i+=1
        #     print(i)
        # df.loc[:,["name", "salary", "area_name","published_at"]].to_csv("ConvertedVacancies.csv", index=False)
        # return df

        df = self.df.copy()
        df["published_at"] = df["published_at"].transform(lambda x: x[:19])
        df["salary"] = df[["salary_from", "salary_to"]].mean(axis=1)
        df["salary"] = df.apply(lambda x: self.ConvertSalary(x), axis=1)
        df = df[df["salary"].notnull()]
        vacanciesDF = df.loc[:, ["name", "salary", "area_name", "published_at"]]
        vacanciesDF.to_csv("ConvertedVacancies.csv", index=False)
        self.dbController.CloseDB()
        if returnFormat == "df":
            return vacanciesDF, "ConvertedVacancies.csv"
        else:
            vacanciesDB = DB("convertedVacancies")
            vacanciesDB.CreateDataBase(vacanciesDF, "name text, salary float, area_name text, published_at text", False)
            return vacanciesDB

    def ConvertSalary(self, row):
        if row["salary_currency"] != "RUR":
            request = f"""SELECT {row['salary_currency']} 
                        FROM ConversionTable 
                        WHERE date = '{row["published_at"][0:7]}'"""
            rate = self.dbCursor.execute(request).fetchone()[0]
            return row['salary'] * rate if rate is not None else None
        return row["salary"]
