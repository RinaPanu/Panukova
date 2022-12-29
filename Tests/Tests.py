from unittest import TestCase
from TableTask import InputConnect, DataSet, Salary
from PdfTask import Salary as pdfSalary

class InputConnectTests(TestCase):
    def test_MaxChars(self):
        self.assertEqual(InputConnect.SetMaxCharsTest({0: "asdas", 1: "213sdsd", 2: "", 3: "dljkrfghberifhgj bneiruhjgb niedujfhngk dfjngdkjfg ndjkfgndkjfg ndfjkgndkjfgndkjfngxkmcv dfdfgdfgb,xm skjfnsdfsdfs dgasdas dfgdfgdfgdf"}), {0: 'asdas', 1: '213sdsd', 2: '', 3: 'dljkrfghberifhgj bneiruhjgb niedujfhngk dfjngdkjfg ndjkfgndkjfg ndfjkgndkjfgndkjfngxkmcv dfdfgdfgb,x...'})


class DataSetTests(TestCase):
    def test_CleanRow(self):
        self.assertEqual(DataSet.CleanRowTest("<p><strong>Основные функции:</strong></p> <ul> <li>мониторинг состояния промышленных кластеров СУБД SAP ASE (Sybase) Банка;</li> <li>участие в штатных процедурах решения ИТ-инцидентов;</li> <li>выполнение работ по сопровождению промышленных и тестовых кластеров СУБД SAP ASE (Sybase) и подготовка планов, инструкций инженерному составу;</li> <li>сбор и анализ диагностической информации, в случае потребности;</li>"), 'Основные функции: мониторинг состояния промышленных кластеров СУБД SAP ASE (Sybase) Банка; участие в штатных процедурах решения ИТ-инцидентов; выполнение работ по сопровождению промышленных и тестовых кластеров СУБД SAP ASE (Sybase) и подготовка планов, инструкций инженерному составу; сбор и анализ диагностической информации, в случае потребности;')

class SalaryTests(TestCase):
    def test_Format(self):
        self.assertEqual(Salary(1000000, 2000000, "GEL", "False").Format(), '1 000 000 - 2 000 000 (Грузинский лари) (С вычетом налогов)')

    def test_ChangeCurrency(self):
        self.assertEqual(Salary.ChangeCurrencyTest(999, "EUR"), 59840.1)

    def test_GetAverage(self):
        self.assertEqual(pdfSalary(100, 100, "RUR").GetAverage(), 100.0)
