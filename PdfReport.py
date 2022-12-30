import pdfkit
import numpy as np
import matplotlib.pyplot as plt
from jinja2 import Environment, FileSystemLoader


class Report:
    """
    Класс, формирующий отчет для пользователя
    """

    def __init__(self, vacancyName, areaName):
        """
        Инициализирует объект Report
        Args:
            vacancyName (str): название профессии
        """

        self.areaName = areaName
        self.vacancyName = vacancyName

    def CheckEmptyText(self, value):
        """
        Проверяет ячейку на пустоту,
        если ячейка пустая (None) - задает ячейке значение ""
        Args:
            value (str): Ячейка таблицы
        Returns:
            str: значение ячейки
        """
        if value is None:
            return ""
        return str(value)

    def __CreateVerticalBars(self, ax, title, data1, data2, label1, label2, rotation):
        """
        Формирует вертикальную диаграмму для двух данных
        Args:
            ax (axes.SubplotBase): График
            title (str): Заголовок диаграммы
            data1 (dict): Общие данные
            data2 (dict): Данные для выбранной профессии
            label1 (str): Надпись легенды для общих данных
            label2 (str): Надпись легенды для данные выбранной профессии
            rotation (int): Угол поворота надписей оси X
        Returns:
            axes.SubplotBase: График
        """
        xIndexes = np.arange(len(data1.keys()))
        width = 0.35
        ax.set_title(title)
        ax.bar(xIndexes - width / 2, data1.values(), width, label=label1)
        ax.bar(xIndexes + width / 2, data2.values(), width, label=label2)
        ax.legend()
        ax.grid(axis="y", visible=True)
        ax.set_xticks(xIndexes, data1.keys(), rotation=rotation)
        return ax

    def __CreateHorizontalBar(self, ax, title, data):
        """
        Формирует горизонтальную диаграмму
        Args:
            ax (axes.SubplotBase): График
            title (str): Заголовок диаграммы
            data (dict): Данные
        Returns:
            axes.SubplotBase: График
        """
        width = 0.35
        ax.set_title(title)
        ax.barh(list(data.keys())[:10], list(data.values())[:10], width)
        ax.grid(axis="x", visible=True)
        ax.invert_yaxis()
        return ax

    def __CreatePie(self, ax, title, data):
        """
        Формирует круговую диаграмму
        Args:
            ax (axes.SubplotBase): График
            title (str): Заголовок диаграммы
            data (dict): Данные
        Returns:
            axes.SubplotBase: График
        """
        plt.rc('font', size=6)
        ax.set_title(title)
        ax.pie(data.values(), labels=data.keys(), labeldistance=1.1, startangle=-210)
        return ax

    def GenerateImage(self, listData):
        """
        Формирует изображение "graph.png" со статистикой вакансий в виде графиков
        Args:
            listData: Данные файла
        """
        plt.rc('font', size=8)
        figure = plt.figure()
        ax1 = figure.add_subplot(2, 2, 1)
        ax2 = figure.add_subplot(2, 2, 2)
        ax1 = self.__CreateVerticalBars(ax1, f"Уровень зарплат по годам\n в регионе {self.areaName}", listData[0],
                                        listData[2],
                                        "средняя з/п",
                                        f'з/п {self.vacancyName}', 90)
        ax2 = self.__CreateVerticalBars(ax2, f"Количество вакансий по годам\n в регионе {self.areaName}", listData[1],
                                        listData[3],
                                        "Количество вакансий", f'Количество вакансий {self.vacancyName}', 90)
        ax3 = figure.add_subplot(2, 2, 3)
        ax3 = self.__CreateHorizontalBar(ax3, "Уровень зарплат по городам", listData[4])
        plt.rc('font', size=6)
        tempDict = {k: v for k, v in list(listData[5].items())[:10]}
        tempDict["Другие"] = 1 - sum(tempDict.values())
        tempDict = dict(sorted(tempDict.items(), key=lambda x: x[1]))
        ax4 = figure.add_subplot(2, 2, 4)
        ax4 = self.__CreatePie(ax4, "Доля вакансий по городам", tempDict)
        plt.tight_layout()
        plt.savefig("graph.png")
        plt.show()

    def GeneratePDF(self, listData):
        """
        Формирует отчет  "report.pdf" со всей статистикой в виде графиков и таблицы
        Args:
            listData: Данные файла:
        """
        self.GenerateImage(listData)

        env = Environment(loader=FileSystemLoader('.'))
        template = env.get_template("pdfTemplate.html")
        headingsByYear = ["Год", "Средняя зарплата", f'Средняя зарплата - {self.vacancyName}',
                          "Количество вакансий",
                          f'Количество вакансий - {self.vacancyName}']
        headingsByCity = ["Город", "Уровень зарплат", "", "Город", "Доля вакансий"]
        pdfTemplate = template.render({
            "fileName": "graph.png",
            "vacancyName": self.vacancyName,
            "areaName": self.areaName,
            "headingsByYear": headingsByYear,
            "headingsByCity": headingsByCity,
            "dynamicsSalaries": listData[0],
            "dynamicsSalariesAtVacancy": listData[2],
            "dynamicsCountVacancies": listData[1],
            "dynamicsCountVacanciesAtVacancy": listData[3],
            "citiesSalaryLevel": {k: v for k, v in list(listData[4].items())[:10]},
            "citiesRatioVacancies": {k: f'{round(v * 100, 2)}%' for k, v in list(listData[5].items())[:10]}
        })

        options = {'enable-local-file-access': None}
        config = pdfkit.configuration(wkhtmltopdf=r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe")
        pdfkit.from_string(pdfTemplate, "report.pdf", configuration=config, options=options)