import csv
import re
from pprint import pprint
from pymongo import MongoClient

client = MongoClient(host='192.168.66.74', port=32017)


def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    client.drop_database(db)
    db = client[db]
    db_collection = db["artists"]
    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile, delimiter=',')
        for row in reader:
            db_collection.insert_one({
                'Исполнитель': row["Исполнитель"],
                'Цена': int(row["Цена"]),
                'Место': row["Место"],
                'Дата': row["Дата"]
            })
            # print(row["Исполнитель"], row["Цена"], row["Место"], row["Дата"])
    # pprint(list(db_collection.find()))
    return "Импортировали данные"


def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """
    db = client[db]
    db_collection = db["artists"]
    cursor = db_collection.find().sort("Цена", 1)
    for doc in cursor:
        print(doc)
    return "Отсортировали"


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке, например "Seconds to"),
    и вернуть их по возрастанию цены
    """
    db = client[db]
    db_collection = db["artists"]
    regex = re.compile('\w?\s?'+name+'\w?\s?')
    # regex = name
    pprint(list(db_collection.find({'Исполнитель': regex}).sort("Цена", 1)))
    return "Нашли"


def main():
    """
    Основная программа
    :return:
    """
    print(read_data('artists.csv', 'artists_db'))
    print(find_cheapest('artists_db'))
    pprint(find_by_name(' to M','artists_db'))


if __name__ == '__main__':
    main()