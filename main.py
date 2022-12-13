from venv import create
from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient

load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PW")

connection_string = f"mongodb+srv://admin:{password}@python-crud.dywamr3.mongodb.net/?retryWrites=true&w=majority"
# connection_string = "mongodb://localhost:27017/"

client = MongoClient(connection_string)

dbs = client.list_database_names()
test_db = client.test
collections = test_db.list_collection_names()
print(collections)


# INSERIR DOCUMENTOS#
def insert_test_doc():
    collection = test_db.test
    test_document = {
        "name": "Gabriel",
        "type": "test",
    }
    inserted_id = collection.insert_one(test_document).inserted_id
    print(inserted_id)


production = client.production
person_collection = production.person.collection


def create_documents():
    first_names = ["Gabriel", "Joao", "Maria", "Jose", "Pedro"]
    last_names = ["Silva", "Santos", "Souza", "Pereira", "Oliveira"]
    ages = [20, 21, 22, 23, 24]

    docs = []

    for first_names, last_names, ages in zip(first_names, last_names, ages):
        doc = {
            "first_name": first_names,
            "last_name": last_names,
            "age": ages,
        }
        docs.append(doc)
        # person_collection.insert_one(doc)

    person_collection.insert_many(docs)


printer = pprint.PrettyPrinter()

# create_documents()


# LISTAR DOCUMENTOS #
def find_all_people():
    people = person_collection.find()

    for person in people:
        printer.pprint(person)


# find_all_people()

# BUSCAR NOME ESPECIFICO #
def find_gabriel():
    gabriel = person_collection.find_one({"first_name": "Gabriel"})
    printer.pprint(gabriel)

# find_gabriel()


def count_all_people():
    count = person_collection.count_documents(filter={})
    print("Numero de pessoas:", count)

# count_all_people()

def get_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})
    printer.pprint(person)

# get_person_by_id("6321fc450641e6f4768a86c2")


# BUSCAR RANGE DE IDADES #
def get_age_range(min_age, max_age):
    query = {"$and": [
        {"age": {"$gte": min_age}},
        {"age": {"$lte": max_age}},
    ]}

    people = person_collection.find(query).sort("age")
    for person in people:
        printer.pprint(person)

# get_age_range(20, 35)

def project_columns():
    columns = {"_id": 0, "first_name": 1, "last_name": 1}
    people = person_collection.find({}, columns)
    for person in people:
        printer.pprint(person)

# project_columns()


# ATUALIZAR PESSOA POR ID
def update_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)

    # all_updates = {
    #    "$set": {"new_field": True},
    #    "$inc": {"age": 1},
    #    "$rename": {"first_name": "first", "last_name": "last"}
    # }
    # person_collection.update_one({"_id": _id}, all_updates)

    person_collection.update_one({"_id": _id}, {"$unset": {"new_field": ""}})


# update_person_by_id("6321fc450641e6f4768a86bf")

# ALTERAR NOME, SOBRENOME, IDADE
def replace_one(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    new_doc = {
        "first_name": "new first name",
        "last_name": "new last name",
        "age": 99,
    }

    person_collection.replace_one({"_id": _id}, new_doc)

# replace_one("6321fc450641e6f4768a86bf")


# DELETAR PESSOA POR ID
def delete_doc_by_id(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    person_collection.delete_one({"_id": _id})

    # DELETAR TODOS
    # person_collection.delete_many({})

# delete_doc_by_id("6321fc450641e6f4768a86bf")


