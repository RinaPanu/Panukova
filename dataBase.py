import sqlite3
import pandas as pd


class DataBase:

    def __init__(self, dbName):
        self.tableName = dbName
        self.dbName = f'{self.tableName}.db'

    def CreateDataBase(self, df, parameters, index):
        with sqlite3.connect(f'{self.dbName}') as db:
            cursor = db.cursor()
            cursor.execute(
                f"""CREATE TABLE 
                IF NOT EXISTS {self.tableName} 
                ({parameters})
                """)
            df.to_sql(f'{self.tableName}', db, if_exists='replace', index=index)

    def OpenDB(self):
        self.db = sqlite3.connect(self.dbName)
        return self.db.cursor()

    def CloseDB(self):
        self.db.close()

    def GetResponseDF(self, request):
        return pd.read_sql_query(request, self.db)