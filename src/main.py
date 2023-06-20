import csv
import os

from get_data import HeadHunterAPI
from DBManager import DBManager


def main():
    vac = HeadHunterAPI()

    vac.instantiate_from_csv("employers.csv")

    vac.create_database("Vacancy")
    vac.save_data_to_database("Vacancy")

    db_manager = DBManager("Vacancy")

    while True:
        print("\nВведите интересующий вас запрос :\n"
              "1 - Вывести все выбранные компании :\n"
              "2 - Вывести количество вакансий у каждой компании :\n"
              "3 - Вывести среднюю заработную плату в компаниях :\n"
              "4 - Вывести заработную плату выше чем средняя по компаниям :\n"
              "5 - Вывести вакансии с нужным фильтром :\n"
              "Введите 'exit' для выхода :\n")

        inp = input()
        try:
            if inp == "1":
                csvfile = open(os.path.join(os.path.dirname(__file__), "employers.csv"), encoding="utf-8", newline='')
                data = csv.DictReader(csvfile)
                for row in data:
                    print(row['Employer'])
                csvfile.close()
            elif inp == "2":
                for i in db_manager.get_companies_and_vacancies_count():
                    print(f"Компания : {i[0]} \nКоличество вакансий : {i[1]},")
            elif inp == "3":
                print(f"Средняя зарплата : {round(db_manager.get_avg_salary())}")
            elif inp == "4":
                for i in db_manager.get_vacancies_with_higher_salary():
                    print(f"Вакансия : {i[0]} {i[2]}\nЗаработная плата : {i[5]},")
            elif inp == "5":
                print("Введите слово по которому организовать поиск :")
                a = input()
                print(f"Происходит поиск по слову {a}")
                for i in db_manager.get_vacancies_with_keyword(a):
                    print(f"Вакансия : {i[0]} {i[2]}\nСсылка на вакансию : {i[8]},")
            else:
                print("Нет такого варианта")
        finally:
            if inp == "exit":
                print("Работа завершена")
                break

    db_manager.close()


if __name__ == '__main__':
    main()
