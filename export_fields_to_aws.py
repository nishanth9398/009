import csv
import editdistance
import pymysql
import math
import toml

weights = {"core core": 10, "core minor": 5, "minor minor": 2, "interest": 15
           }

database = "selection_2021b"
batch = 2

faculty_sql = """
SELECT userid, oistid, sdbid, username, email
FROM logon
WHERE ((class = 1 -- Faculty
OR userid = 52) -- Ulf, special because Dean
AND email IS NOT NULL)
-- OR userid = 801 -- Hibino-san
"""

faculty_fields_sql = """
SELECT faculty_id, full_name, importance FROM faculty_fields ff join fields f on ff.fields_id=f.id
"""

fields_sql = """
SELECT id, field, full_name from fields
"""

applicants_sql = f"""
-- in selection_2020b
SELECT a.family_n, a.given_n,
       a.user_id,
       a.fi_core, a.fi_sub,
       a.faculty1_id, a.faculty2_id, a.faculty3_id,
       e.comment -- comment from committee
FROM applicant a
LEFT JOIN eval_master e ON e.user_id=a.user_id
LEFT JOIN eval_master2 e2 ON e2.user_id=a.user_id
WHERE e.status=1 or e.status=4
"""


def clean_field_name(field):
    field = "".join([c for c in field if c not in " -(),"])
    field = field[:30]
    return field


def get_fields(db, fields_sql):
    """
    Calls the database and gets fields information.
    Input: database connection object, SQL query
    Returns: dictionary of student information, key=field name
    """
    fields = {}

    with db.cursor() as cursor:
        cursor.execute(fields_sql)
        for id, name, full_name in cursor.fetchall():
            fields[full_name] = id

    return fields


def get_students(db, applicants_sql, fields):
    """
    Calls the database and gets student information.
    Input: database connection object, SQL query, dict of fields name/ID
    Returns: dictionary of student information, key=student ID
    """
    students = {}

    with db.cursor() as cursor:
        cursor.execute(applicants_sql)
        for last, first, id, core, minor, f1, f2, f3, comment \
                in cursor.fetchall():
            faculty = [f1, f2, f3]
            cores = []
            for f in core.split("/"):
                f = f.strip() # clean_field_name(f.strip())
                if not f:
                    continue
                if f in fields:
                    cores.append(f)
                else:
                    print(f"Fields not found: {f}")

            minors = []
            for f in minor.split("/"):
                f = f.strip() # clean_field_name(f.strip())
                if not f:
                    continue
                if f in fields:
                    minors.append(f)
                else:
                    print(f"Fields not found: {f}")

            students[id] = {"name": f"{last} {first}", "faculty": faculty, "core": cores, "minor": minors, "match": {}, "comment": comment
                            }
    return students


def get_faculty(db):
    """
    Calls the database and files to get faculty information.
    Input: database connection object, paths to files with units names and fields
    Returns: dictionary of faculty information, key=faculty ID
    """
    faculty = {}

    with db.cursor() as cursor:
        cursor.execute(faculty_sql)
        for id, username, sdbid, name, email in cursor.fetchall():
            faculty[str(id)] = \
                {"name": name.strip(), "logon ID": str(id), "SDB ID": str(sdbid), "username": username, "email": email.strip().lower(), "core": [], "minor": [], "match": []
                 }

    # Get faculty fields
    with db.cursor() as cursor:
        cursor.execute(faculty_fields_sql)
        for fac, field, importance in cursor.fetchall():
            if str(fac) in faculty:
                faculty[str(fac)][importance].append(field)
            else:
                print("Faculty", fac, "not found.")

    # Changing the id to the student database ID
    faculty = {faculty[id]["SDB ID"]: faculty[id] for id in faculty}
    return faculty


def export(fields, faculty, students):
    """
    Rewrites the database table with the matching scores after manual confirm.
    Input: database connection object, student dictionnary
    Returns: Nothing
    """
    query = "DELETE FROM field_matrix;"
    cursor.execute(query)
    for stu in students:
        query = "INSERT INTO field_matrix (user_id, faculty_id, closeness) VALUES {} ;"
        values = []
        for fac in students[stu]["match"]:
            fac_id = faculty[fac]["logon ID"]
            score = students[stu]["match"][fac]
            values.append(f"(\"{stu}\", \"{fac_id}\", {score})")


def connect(login, database):
    # Connect to database
    db = pymysql.connect(host=login["aad"]["host"],
                         user=login["aad"]["username"],
                         passwd=login["aad"]["password"],
                         db=database)
    return db




if __name__ == "__main__":
    login = toml.load("login.toml")
    db = connect(login, database)
    # Fields information
    fields = get_fields(db, fields_sql)
    # Faculty information
    faculty = get_faculty(db)
    # Student information
    students = get_students(db, applicants_sql, fields)
    # Closes connection
    db.close()

    # # Print faculty fields
    # print("faculty_id", "field", "importance", sep="\t")
    # for s in faculty:
    #     for f in faculty[s]["core"]:
    #         print(faculty[s]["username"], f, 'major',sep="\t")
    #     for f in faculty[s]["minor"]:
    #         print(faculty[s]["username"], f, 'minor',sep="\t")
            
    # Print student fields
    print("user_id", "field", "importance", sep="\t")
    for s in students:
        for f in students[s]["core"]:
            print(s, f, 'major',sep="\t")
        for f in students[s]["minor"]:
            print(s, f, 'minor',sep="\t")

