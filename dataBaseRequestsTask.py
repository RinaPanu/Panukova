from currenciesParser import CurrenciesParser

fileName = input("Введите название файла: ")
vacancyName = input("Введите название профессии: ")
areaName = input("Введите название региона: ")
currenciesParser = CurrenciesParser(fileName)
convertedCurrenciesDB = currenciesParser.ConvertToRub("db")
convertedCurrenciesDB.OpenDB()
print(convertedCurrenciesDB.GetResponseDF("""SELECT strftime('%Y', published_at) as publishedYear,
         CAST(AVG(salary) as INTEGER) as avgSalary 
         FROM convertedVacancies 
         GROUP BY publishedYear"""))
print(convertedCurrenciesDB.GetResponseDF(f"""SELECT strftime('%Y', published_at) as publishedYear,
         CAST(AVG(salary) as INTEGER) as avgSalary 
         FROM convertedVacancies 
         WHERE name 
         LIKE '%{vacancyName}%' 
         GROUP BY publishedYear"""))
print(convertedCurrenciesDB.GetResponseDF("""SELECT strftime('%Y', published_at) as publishedYear, 
        COUNT(name) as countVacancy
        FROM convertedVacancies
        GROUP BY publishedYear"""))
print(convertedCurrenciesDB.GetResponseDF(f"""SELECT strftime('%Y', published_at) as publishedYear,
        COUNT(name) as countVacancy
        FROM convertedVacancies 
        WHERE name LIKE '%{vacancyName}%' 
        GROUP BY publishedYear"""))
print(convertedCurrenciesDB.GetResponseDF(f"""SELECT area_name, 
        CAST(AVG(salary) as INTEGER) as avgSalary
        FROM convertedVacancies 
        GROUP BY area_name HAVING 
        (CAST(COUNT(name) as REAL) / (SELECT COUNT(*) 
        FROM convertedVacancies) >= 0.01) 
        ORDER BY avgSalary 
        DESC LIMIT 10"""))
print(convertedCurrenciesDB.GetResponseDF(f"""SELECT area_name, 
        ROUND(CAST(COUNT(name) as REAL) / (SELECT COUNT(*) 
        FROM convertedVacancies), 4) as ratio 
        FROM convertedVacancies 
        GROUP BY area_name 
        HAVING (CAST(COUNT(name) as REAL) / (SELECT COUNT(*) 
        FROM convertedVacancies) >= 0.01) 
        ORDER BY ratio 
        DESC LIMIT 10"""))