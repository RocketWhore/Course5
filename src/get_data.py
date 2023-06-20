import csv
import os
import time

import psycopg2
import requests


class ParsingError(Exception):
    def __str__(self):
        return 'Ошибка получения данных по API.'


class InstantiateCSVError(Exception):
    def __str__(self):
        return 'В структуре файла csv не хватает данных'


class HeadHunterAPI:
    def __init__(self):
        self.__header = {
            "HH-User-Agent": "unknown"
        }
        self.__params = None
        self.__vacancies = []
        self.__employers = {}

    def get_request_employer(self, employer_id):
        response = requests.get(f'https://api.hh.ru/employers/{employer_id}',
                                headers=self.__header,
                                params=self.__params)
        if response.status_code != 200:
            raise ParsingError
        return response.json()

    def get_request_vacancy(self):
        response = requests.get('https://api.hh.ru/vacancies',
                                headers=self.__header,
                                params=self.__params)
        if response.status_code != 200:
            raise ParsingError
        return response.json()['items']

    def get_vacancies(self, employer_id, page_count=1):
        self.__params = {
            "employer_id": employer_id,
            "page": 0,
            "per_page": 100,
            "locale": "RU",
            "host": "hh.ru",
            "area": 113
        }
        self.__vacancies = []
        while self.__params['page'] < page_count:
            print(
                f"HeadHunter, Парсинг страницы {self.__params['page'] + 1} для работодателя {self.employers[employer_id]['name']}",
                end=": ")
            try:
                values = self.get_request_vacancy()
            except ParsingError:
                print('Ошибка получения данных!')
                break
            print(f"Найдено ({len(values)}) вакансий.")
            if len(values) == 0:
                break
            self.__vacancies.extend(values)
            self.__params['page'] += 1
            time.sleep(1)

    def get_employer(self, employer_id):
        self.__params = {
            "locale": "RU",
            "host": "hh.ru"
        }
        try:
            values = self.get_request_employer(employer_id)
        except ParsingError:
            print('Ошибка получения данных!')
        self.__employers[employer_id] = values
        self.get_vacancies(employer_id, 1)  # Тут нужно последним параметром задать количество страниц парсинга вакансий
        self.__employers[employer_id]["vacancies"] = self.__vacancies

    def instantiate_from_csv(self, file_name: str):
        """
        Метод, заполняющий экземпляр класса данными о работодателях и их вакансиях в соответствии с файлом - списком
        работодателей file_name
        """
        try:
            csvfile = open(os.path.join(os.path.dirname(__file__), file_name), encoding="utf-8", newline='')
        except FileNotFoundError:
            raise FileNotFoundError('Отсутствует файл csv')
        else:
            data = csv.DictReader(csvfile)
            if 'id' in data.fieldnames:
                for row in data:
                    self.get_employer(row['id'])
            else:
                raise InstantiateCSVError(f"В структуре файла {file_name} не хватает данных")
            csvfile.close()

    @staticmethod
    def create_database(database_name: str):
        """
        Создание базы данных и таблиц для сохранения данных о работодателях и вакансиях.
        """

        conn = psycopg2.connect(dbname='postgres',
                                host='localhost',
                                user='postgres',
                                password='Ametist371')
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(f"DROP DATABASE IF EXISTS {database_name}")

        cur.execute(f"CREATE DATABASE {database_name}")

        conn.close()

        conn = psycopg2.connect(dbname=database_name,
                                host='localhost',
                                user='postgres',
                                password='Ametist371')

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS employers (
                    employer_id SERIAL PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    open_vacancies INTEGER,
                    site_url VARCHAR,
                    description TEXT
                )
            """)

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS vacancies (
                    vacancy_id SERIAL PRIMARY KEY,
                    employer_id INT,
                    title VARCHAR NOT NULL,
                    area_id INT,
                    area VARCHAR(255),
                    salary INT,
                    currency VARCHAR(3),
                    publish_date VARCHAR(24),
                    url VARCHAR,
                    FOREIGN KEY (employer_id) REFERENCES employers (employer_id)
                )
            """)

        conn.commit()
        conn.close()

    def save_data_to_database(self, database_name: str):
        """
        Метод сохраняет полученные данные о работодателях и их вакансиях в таблицы базы данных
        :return:
        """
        conn = psycopg2.connect(dbname=database_name,
                                host='localhost',
                                user='postgres',
                                password='Ametist371')

        employers_keys = self.__employers.keys()

        with conn.cursor() as cur:
            for key in employers_keys:
                employer_id = self.__employers[key]["id"]
                employer_title = self.__employers[key]["name"]
                employer_open_vacancies = self.__employers[key]["open_vacancies"]
                employer_site_url = self.__employers[key]["site_url"]
                employer_description = self.__employers[key]["description"]

                cur.execute(
                    """
                    INSERT INTO employers (employer_id, title, open_vacancies, site_url, description)
                    VALUES (%s, %s, %s, %s, %s) 
                    ON CONFLICT (employer_id) DO NOTHING;
                    """,
                    (employer_id, employer_title, employer_open_vacancies,
                     employer_site_url, employer_description)
                )
                vacancies_data = self.__employers[key]["vacancies"]
                for vacancy in vacancies_data:
                    vacancy_id = vacancy["id"]
                    vacancy_employer_id = vacancy["employer"]["id"]
                    vacancy_title = vacancy["name"]
                    vacancy_area_id = vacancy["area"]["id"]
                    vacancy_area = vacancy["area"]["name"]
                    if vacancy["salary"] is None or (
                            vacancy["salary"]["from"] is None and vacancy["salary"]["to"] is None):
                        vacancy_salary = None
                        vacancy_currency = None

                    elif vacancy["salary"]["to"] is not None and vacancy["salary"]["from"] is not None:
                        vacancy_salary = (vacancy["salary"]["to"] + vacancy["salary"]["from"])/2
                        vacancy_currency = vacancy["salary"]["currency"]
                    elif vacancy["salary"]["to"] is not None:
                        vacancy_salary = vacancy["salary"]["to"]
                        vacancy_currency = vacancy["salary"]["currency"]
                    else:
                        vacancy_salary = vacancy["salary"]["from"]
                        vacancy_currency = vacancy["salary"]["currency"]
                    vacancy_publish_date = vacancy["published_at"]
                    vacancy_url = vacancy["alternate_url"]
                    cur.execute(
                        """
                        INSERT INTO vacancies (vacancy_id, employer_id, title, area_id, area, salary, 
                        currency, publish_date, url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (vacancy_id) DO NOTHING;
                        """,
                        (vacancy_id, vacancy_employer_id, vacancy_title, vacancy_area_id, vacancy_area,
                         vacancy_salary, vacancy_currency, vacancy_publish_date, vacancy_url)
                    )

        conn.commit()
        conn.close()

    @property
    def vacancies(self):
        return self.__vacancies

    @property
    def employers(self):
        return self.__employers
