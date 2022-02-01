import csv
import pymysql
import toml
from collections import defaultdict

year = "2020b"

fields_sql = """
SELECT id, field from fields
"""

faculty_fields_sql = """
SELECT faculty_id, fields_id, importance
FROM faculty_fields
"""

def clean_field_name(field):
    field = "".join([c for c in field if c not in " -(),"])
    field = field[:30]
    return field

def get_fields(db):
    """
    Calls the database and files to get faculty information.
    Input: database connection object, paths to files with units names and fields
    Returns: dictionary of faculty information, key=faculty email
    """
    fields = {}

    with db.cursor() as cursor:
        cursor.execute(fields_sql)
        for id, field in cursor.fetchall():
            fields[field] = id

    return fields

def get_fields_info(db_fields, path):
    fields = {}
    file = open(path)
    csvfile = csv.reader(file)

    for field, category, _, _, _ in csvfile:
        cleaned = clean_field_name(field)
        if cleaned in db_fields:
            fields[db_fields[cleaned]] = {"short": cleaned , "full":field, "category": category  }
        else:
            print("Field", field, "-", cleaned,  "not found in DB")
    return fields

def export_fields(db, fields):
    """
    Updates the database table with the fields after manual confirm.
    Input: database connection object, faculty dictionnary
    Returns: Nothing
    """
    query = "UPDATE fields SET full_name = '{}', category = '{}' WHERE id = '{}';"

    with db.cursor() as cursor:
        for id in fields:
            q = query.format(fields[id]["full"], fields[id]["category"], id)
            print(q)
            cursor.execute(q)
            db.commit()

def print_faculty_fields(db, fields):
    """
    Calls the database and files to get faculty information.
    Input: database connection object, paths to files with units names and fields
    Returns: dictionary of faculty information, key=faculty email
    """
    faculty = defaultdict(set)

    with db.cursor() as cursor:
        cursor.execute(faculty_fields_sql)
        for faculty_id, fields_id, importance in cursor.fetchall():
            if importance == "core":
                faculty[faculty_id].add(fields[fields_id]["category"])

    query = "UPDATE faculty SET field = '{}' WHERE faculty_id = '{}';"
    for f in faculty:
        field = ", ".join(sorted(list(faculty[f])))
        print(query.format(field, f))


def print_faculty_url( path):
    query = "UPDATE faculty SET url = '{}' WHERE email = '{}';"
    file = open(path)
    for url, email in csv.reader(file):
        print(query.format(url, email))


def connect(login):
    # Connect to database
    # db = pymysql.connect(host = login["aad"]["host"],
    #                      user = login["aad"]["username"],
    #                      passwd = login["aad"]["password"],
    #                      db     = f"selection_{year}") # name of the database
    db = pymysql.connect(host = "localhost",
                         user ="root",
                         passwd = "",
                         db     = f"selection_{year}") # name of the database
    return db



if __name__ == "__main__":
    login = toml.load("login.toml")
    db = connect(login)
    # Fields information from DB
    db_fields = get_fields(db)
    # Add information to fields
    fields = get_fields_info(db_fields, "input/fields_conversion.csv")
    # Faculty major fields
    print_faculty_fields(db, fields)
    # print_faculty_url("input/unit_url.csv")
    # fields = export_fields(db, fields)
    # Closes connection
    db.close()
