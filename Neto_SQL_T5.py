import psycopg2
import configparser

DB_Name = 'neto_clients'

config = configparser.ConfigParser()
config.read('postgres.ini')
Postgres_password = config['Postgres']['password']


def del_table(cursor):
    cursor.execute("""
    DROP TABLE IF EXISTS phones;
    DROP TABLE IF EXISTS clients;
    """)
    print('<All tables have been deleted>')
    return conn.commit()


def create_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS clients(
        client_id SERIAL PRIMARY KEY,
        first_name VARCHAR(40) NOT NULL,
        last_name VARCHAR(40) NOT NULL,
        email VARCHAR(70) NOT NULL UNIQUE
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS phones(
        client_id INTEGER NOT NULL REFERENCES clients(client_id),
        phone VARCHAR(15) NOT NULL UNIQUE
    );    
    """)
    print("<Tables ('clients' and 'phones') have been created>")
    return conn.commit()


def add_client(cursor, first_name, last_name, email, phone=None):
    if _is_email(cursor, email) or _is_phone(cursor, phone):
        print('<Запись не создана>\n')
        return
    else:
        cursor.execute("""
        INSERT INTO clients (first_name, last_name, email)
        VALUES (%s, %s, %s) RETURNING client_id;""", (first_name, last_name, email))
        client_id = cursor.fetchone()[0]
        if phone:
            cursor.execute("""
            INSERT INTO phones (client_id, phone)
            VALUES (%s, %s);""", (client_id, phone))
        print(f'<New client has been added (id={client_id})>\n')
        return conn.commit()


def add_phone(cursor, client_id, phone):
    if _is_phone(cursor, phone) or not _is_client_id(cursor, client_id):
        print('<В добавлении телефона отказано>\n')
        return
    else:
        _insert_phone(cursor, client_id, phone)
        print(f'<Клиенту с id={client_id} добавлен телефон {phone}>\n')


def change_client(cursor, client_id, first_name=None, last_name=None, email=None, phone=None):
    change_d = {k: v for k, v in zip(['first_name', 'last_name', 'email', 'phone'],
                                     [first_name, last_name, email, phone]) if v}
    print(f'<Клиент id={client_id}: обновление данных: {change_d}>')
    if not change_d:
        print(f'<В запросе нет данных для обновления>\n Результат обновления  данных (клиент id={client_id}):\n '
              'Изменения внести невозможно\n')
        return
    if not _is_client_id(cursor, client_id) or \
            (email and _is_email(cursor, email) and client_id != _get_client_id(cursor, email)) or \
            (phone and _is_phone(cursor, phone) and client_id != _get_client_id(cursor, phone)):
        print(f' Результат обновления  данных (клиент id={client_id}):\n Изменения внести невозможно\n')
        return
    else:
        # get current values
        cursor.execute("""
                    SELECT first_name, last_name, email FROM clients
                    WHERE client_id = %s;""", (client_id,))
        res = cursor.fetchall()
        # change current values to new ones (if they are not None)
        cursor.execute("""
                    UPDATE clients
                    SET first_name =%s, last_name = %s, email = %s
                    WHERE client_id = %s;""",
                       (first_name or res[0][0], last_name or res[0][1], email or res[0][2], client_id))
        # add new phone
        if phone and not _is_phone(cursor, phone):
            _insert_phone(cursor, client_id, phone)
        print(f' Результат обновления  данных (клиент id={client_id}):\n Данные по клиенту обновлены\n')
        return conn.commit()


def delete_phone(cursor, phone):
    if not _is_phone(cursor, phone):
        print('<Такого телефона нет в базе>\n')
        return
    else:
        cursor.execute("""
        DELETE FROM phones
        WHERE phone = %s;""", (phone,))
        print(f'<Запись с телефоном {phone} удалена из таблицы phones>\n')
        return conn.commit()


def delete_client(cursor, client_id):
    if not _is_client_id(cursor, client_id):
        return
    else:
        if _has_phone(cursor, client_id):
            cursor.execute("""
            DELETE FROM phones
            WHERE client_id = %s;""", (client_id,))
            conn.commit()
            print(f'<Клиент с id {client_id} удален из таблицы phones>')
        cursor.execute("""
        DELETE FROM clients
        WHERE client_id = %s;""", (client_id,))
        print(f'<Клиент с id {client_id} удален из таблицы clients>\n')
        return conn.commit()


def find_client(cursor, first_name=None, last_name=None, email=None, phone=None):
    if first_name or last_name or email or phone:
        sql_q = "SELECT DISTINCT c.client_id, first_name, last_name, email FROM clients c " \
                "LEFT JOIN phones p ON c.client_id = p.client_id " \
                "WHERE"
        args = []
        for column, condition in zip(['first_name', 'last_name', 'email', 'phone'],
                                     [first_name, last_name, email, phone]):
            if condition:
                sql_q += f' {column} ILIKE %s AND'
                args.append(condition)
        sql_q = sql_q[:len(sql_q) - 4] + ';'
        cursor.execute(sql_q, tuple(args))
        res = cursor.fetchall()
        print(f'<Поиск по: {", ".join([i for i in (first_name, last_name, email, phone) if i])}>\n Результат:')
        if res:
            for i in range(len(res)):
                print(f' id: {res[i][0]}, имя: {res[i][1]}, фамилия: {res[i][2]}, email: {res[i][3]}\n')
        else:
            print(' Соответствия не найдено\n')
        return res
    else:
        print('<Нет данных для поиска>\n')
        return


def _is_email(cursor, email):
    """ Check if this specific email is in the 'clients'-table """
    cursor.execute("""
    SELECT EXISTS (
    SELECT email FROM clients
    WHERE email ILIKE %s);""", (email,))
    isemail = cursor.fetchone()[0]
    if isemail:
        print('<Такой email присутствует в базе>')
    return isemail


def _is_phone(cursor, phone):
    """ Check if this specific phone is in the 'phones'-table """
    cursor.execute("""
    SELECT EXISTS (
    SELECT phone FROM phones
    WHERE phone = %s);""", (phone,))
    isphone = cursor.fetchone()[0]
    if isphone:
        print('<Такой номер телефона есть в базе>')
    return isphone


def _is_client_id(cursor, client_id):
    """ Check if this client_id is in the 'clients'-table """
    cursor.execute("""
    SELECT EXISTS (
    SELECT client_id FROM clients
    WHERE client_id = %s);""", (client_id,))
    isclientid = cursor.fetchone()[0]
    if not isclientid:
        print(f'<Клиента с id={client_id} нет в базе>')
    return isclientid


def _insert_phone(cursor, client_id, phone):
    """ Insert phone by client_id """
    cursor.execute("""
    INSERT INTO phones (client_id, phone)
    VALUES (%s, %s);""", (client_id, phone))
    return conn.commit()


def _has_phone(cursor, client_id):
    """ Check if the client has a phone (in the database)"""
    cursor.execute("""
    SELECT EXISTS (
    SELECT client_id FROM phones
    WHERE client_id = %s);""", (client_id,))
    return cursor.fetchone()[0]


def _get_client_id(cursor, phone_or_email):
    cursor.execute("""
    SELECT DISTINCT c.client_id FROM clients c
    LEFT JOIN phones p ON c.client_id = p.client_id
    WHERE phone = %s OR email ILIKE %s;""", (phone_or_email, phone_or_email))
    client_id = cursor.fetchone()[0]
    print(f'<{phone_or_email} принадлежит клиенту с id={client_id}>')
    return client_id


# ________________________________________________________________________________________________________________

with psycopg2.connect(database=DB_Name, user='postgres', password=Postgres_password) as conn:
    with conn.cursor() as cur:
        del_table(cur)

        create_table(cur)

        print('_______ДОБАВЛЕНИЕ НОВЫХ КЛИЕНТОВ_______')
        add_client(cur, 'Olga', 'Onufrieva', 'O.Onufrieve@gmail.com', '+79524848500')
        add_client(cur, 'Olga', 'Vesta', 'O.Vesta@notgmail.com', '+79524848585')
        add_client(cur, 'Lada', 'Vesta', 'withoutabs@mail.ru')
        add_client(cur, 'Petr', 'Tupolev', 'ptu@dzhimail.com', '+79193881200')
        add_client(cur, 'Nikodim', 'Onegin', 'none@.onmail.org', '+79230950300')
        add_client(cur, 'Lada', 'Granta', 'ecomon@tomail.ru')
        add_client(cur, 'Lada', 'Vesta', 'another.woman@uar.br', '+79530950328')
        add_client(cur, 'Gena', 'Zotov', 'none@.onmail.org', '+79530950333') # есть такой email - не создаем
        add_client(cur, 'ELena', 'Guseva', 'egu111.yandex.ru', '+79530950328') # есть такой телефон - не создаем

        print('_______ДОБАВЛЕНИЕ ТЕЛЕФОНОВ_______')
        add_phone(cur, 5, '+79193881230')
        add_phone(cur, 7, '+79193881230')
        add_phone(cur, 2, '+79899998888')
        add_phone(cur, 5, '+79193881222')
        add_phone(cur, 100, '+79293885222') # нет такого client_id
        add_phone(cur, 5, '+79230950300') # уже есть такой телефон
        add_phone(cur, 4, '+79193881539')
        add_phone(cur, 4, '+79193881640')

        print('_______ПОИСК КЛИЕНТОВ_______')
        find_client(cur, first_name=None, last_name=None, email=None, phone=None)
        find_client(cur, first_name='Lada', last_name='Vesta', email='Withoutabs@mail.ru', phone=None)
        find_client(cur, first_name='Lada', last_name='Vesta', email='w@mai5.ru', phone=None) # ошибка по email
        find_client(cur, 'Nikodim', 'Onegin', None, '+79230950300')
        find_client(cur, 'Nikodim', 'Onegin', None, '+0') # ошибка по phone
        find_client(cur, None, None, None, '+79230950300')
        find_client(cur, None, None, None, '+79193881230')
        find_client(cur, first_name='lada', last_name='Vesta')
        find_client(cur, first_name='NEt takogo imeni', last_name='Vesta') # нет соответствия - пустой список
        find_client(cur, first_name='Lada', last_name='NeVesta')
        find_client(cur, first_name='lada', last_name='Vesta')
        find_client(cur, first_name='UsLada', last_name='Vesta') # нет такого сочетания
        find_client(cur, email='ptu@dzhimail.com')
        find_client(cur, last_name='Vesta')
        find_client(cur, 'Olga', 'Onufrieva', )

        print('_______ИЗМЕНЕНИЕ ДАННЫХ КЛИЕНТА_______')
        change_client(cur, 4, 'Sultan', None, 'a@a.aaa', phone='+79899998886')
        change_client(cur, 4, None, 'Onufrieff', email='o1@o.com', phone=None)
        change_client(cur, 6, None, 'Trubach', email='o1@o.com', phone=None) # чужой email
        change_client(cur, 2, 'Ruby', None, email='ptu@dzhimai.com', phone='+79524848585')
        change_client(cur, 2, 'Maxim', None, email='sokol2@dzhimai.com', phone='+79524848515')
        change_client(cur, 2, None, None, None, phone='+79524848686')
        change_client(cur, 3, 'Laslo', 'Chain', 'tt@tttttt.tt', '+79999999980')
        change_client(cur, 5, None, None, None, None) # пустые данные
        change_client(cur, 100, 'Nikola', None, None, None) # несуществующий client_id

        print('_______УДАЛЕНИЕ ТЕЛЕФОНОВ_______')
        delete_phone(cur, '+79524848500')
        delete_phone(cur, '+79524848585')
        delete_phone(cur, '+79524848500') # телефон только что удален
        delete_phone(cur, '+79524848777') # нет такого телефона

        print('_______УДАЛЕНИЕ КЛИЕНТОВ_______')
        delete_client(cur, 1)
        delete_client(cur, 5)
        delete_client(cur, 100) # нет такого client_id
