import pandas as pd
from distributorVacancies import DistributorVacancies
d = DistributorVacancies()
BDayCurrentMonth = pd.Timestamp.now().replace(day = 1) + pd.offsets.BDay()

d.GetVacanciesCSV(BDayCurrentMonth, 4)
