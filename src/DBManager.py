import psycopg2


class DBManager:
    """Класс подключение к бд заполнение ее и фильтрация"""
    def __init__(self, database_name):
        self.conn = psycopg2.connect(dbname=database_name,
                                     host='localhost',
                                     user='postgres',
                                     password='Ametist371')
        self.cur = self.conn.cursor()

    def get_companies_and_vacancies_count(self):  # количество вакансий у компаний
        self.cur.execute("""SELECT employers.title, COUNT(vacancies.vacancy_id) FROM employers LEFT JOIN vacancies 
        ON employers.employer_id = vacancies.employer_id GROUP BY employers.title""")
        result = self.cur.fetchall()
        return result

    def get_all_vacancies(self):  # функция вывода компании вакансий зп и ссылке
        self.cur.execute("""SELECT  employers.title, vacancies.title, vacancies.salary, vacancies.url 
            FROM vacancies 
            INNER JOIN employers ON vacancies.employer_id = employer_id """)
        result = self.cur.fetchall()
        vacancies = []
        for row in result:
            vacancy = {
                "employer_name": row[0],
                "vacancy_name": row[1],
                "salary": row[2],
                "url": row[3]
            }
            vacancies.append(vacancy)
        return vacancies

    def get_avg_salary(self):  # функция поиска средней зп
        self.cur.execute("""SELECT AVG(CAST(salary AS numeric)) FROM vacancies""")
        result = self.cur.fetchone()[0]
        return result

    def get_vacancies_with_higher_salary(self):  # поиск вакансии по средней зп и фильтрации выше нее
        self.cur.execute("""SELECT AVG(CAST(salary AS numeric)) FROM vacancies""")
        avg_salary = self.cur.fetchone()[0]
        self.cur.execute(f"""SELECT * FROM vacancies WHERE CAST(salary AS numeric) > {avg_salary}""")
        result = self.cur.fetchall()
        return result

    def get_vacancies_with_keyword(self, word):
        self.cur.execute(f"SELECT * FROM vacancies WHERE title LIKE '%{word}%'")
        result = self.cur.fetchall()
        return result

    def close(self):  # функция закрытия соединения с бд
        self.cur.close()
        self.conn.close()