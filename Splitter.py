import pandas as pd

class Splitter:
    def __init__(self, fileName, outputPath, outputName):
        self.fileName = fileName
        self.outputPath = outputPath
        self.outputName = outputName
        self.SplitFileByYear()

    def SplitFileByYear(self):
        df = pd.read_csv(self.fileName)
        df["years"] = df["published_at"].apply(lambda date: int(date[0:4]))
        self.years = df["years"].unique()
        dfGroupByYear = df.groupby("years")
        for year, data in dfGroupByYear:
            data.iloc[:, :6].to_csv(f'{self.outputPath}\\{self.outputName}{year}.csv', index=False)
