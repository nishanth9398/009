import csv
import editdistance
import pymysql
import math
import toml

weights = {"core core": 10, "core minor": 5, "minor minor": 2, "interest": 15
           }

database = "ri2021apr"

faculty_sql = """
SELECT userid, oistid, username
FROM logon
WHERE (class = 1 -- Faculty
OR userid = 52) -- Ulf, special because Dean
"""

faculty_fields_sql = """
SELECT faculty_id, fields_id, importance FROM faculty_fields
"""

fields_sql = """
SELECT id, full_name from fields
"""

applicants_sql = f"""
-- in selection_2020b
SELECT a.l_name, a.f_name,
       a.user_id,
       a.fi_core, a.fi_sub,
       a.unit1, a.unit2, a.unit3
FROM applicant a
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
        for id, name in cursor.fetchall():
            fields[name] = id

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
        for last, first, id, core, minor, f1, f2, f3 in cursor.fetchall():
            faculty = [f for f in [f1, f2, f3] if f]
            cores = []
            for f in core.split("/"):
                f = f.strip()
                if not f:
                    continue
                if f in fields:
                    cores.append(fields[f])
                else:
                    print(f"Fields not found: {f}")

            minors = []
            for f in minor.split("/"):
                f = f.strip()
                if not f:
                    continue
                if f in fields:
                    minors.append(fields[f])
                else:
                    print(f"Fields not found: {f}")

            students[id] = {"name": f"{last} {first}",
                            "faculty": faculty,
                            "core": cores,
                            "minor": minors,
                            "match": {}
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
        for id, username, name in cursor.fetchall():
            faculty[str(id)] = \
                {"name": name.strip(),
                 "logon ID": str(id),
                 "username": username,
                 "core": [], "minor": [], "match": []
                 }

    # Get faculty fields
    with db.cursor() as cursor:
        cursor.execute(faculty_fields_sql)
        for fac, field, importance in cursor.fetchall():
            if str(fac) in faculty:
                faculty[str(fac)][importance].append(field)
            else:
                print("Faculty", fac, "not found.")

    return faculty


def clean_faculty_name(name):
    return " ".join(reversed(name.split(", ")))
    
def faculty_of_interest(faculty, students):
    names = {faculty[f]["name"] : f for f in faculty}
    names["Svante Paabo"] = '107'
    names["Nicholas Luscombe"] = '19'

    for s in students:
        facs = []
        for fac in students[s]["faculty"]:
            if fac:
                name = clean_faculty_name(fac)
                if name in names:
                    facs.append(names[name])
                else:
                    print(f"Faculty of interest {name} not found")

        students[s]["faculty"] = facs


def match(faculty, students, weights):
    """
    Compares faculty and student fields of interest to compute a matching score
    Input: faculty and student dictionnaries
    Returns: Nothing
    """
    co_co_score = weights["core core"]
    co_mi_score = weights["core minor"]
    mi_mi_score = weights["minor minor"]
    interest_score = weights["interest"]

    for stu in students:
        for fac in faculty:
            co_co = sum([f in faculty[fac]["core"]
                         for f in students[stu]["core"]])
            co_mi = sum([f in faculty[fac]["core"]
                         for f in students[stu]["minor"]])
            mi_co = sum([f in faculty[fac]["minor"]
                         for f in students[stu]["core"]])
            mi_mi = sum([f in faculty[fac]["minor"]
                         for f in students[stu]["minor"]])

            score = co_co_score * co_co + co_mi_score * \
                co_mi + co_mi_score * mi_co + mi_mi_score * mi_mi

            if fac in students[stu]["faculty"]:
                score += interest_score

            students[stu]["match"][fac] = score
            faculty[fac]["match"].append((score, stu))

    for fac in faculty:
        faculty[fac]["match"].sort(reverse=True)


def confirm():
    """
    Ask user to enter Y or N (case-insensitive), returns True if the answer is Y.
    Input: None
    Returns: Bool
    """
    answer = ""
    while answer not in ["y", "n"]:
        answer = input(
            "Would you like to update the database? [Y/N]? ").lower()
    return answer == "y"


def export(db, faculty, students):
    """
    Rewrites the database table with the matching scores after manual confirm.
    Input: database connection object, student dictionnary
    Returns: Nothing
    """
    if confirm():
        with db.cursor() as cursor:
            query = "DELETE FROM field_matrix;"
            cursor.execute(query)
            for stu in students:
                query = "INSERT INTO field_matrix (user_id, faculty_id, closeness) VALUES {} ;"
                values = []
                for fac in students[stu]["match"]:
                    fac_id = faculty[fac]["logon ID"]
                    score = students[stu]["match"][fac]
                    values.append(f"(\"{stu}\", \"{fac_id}\", {score})")
                cursor.execute(query.format(", ".join(values)))
        db.commit()


def get_match_distribution(faculty):
    dist = [0]*1000
    for fac in faculty:
        for score, _ in faculty[fac]["match"]:
            dist[score] += 1
    return [(match, n) for match, n in enumerate(dist) if n]


def connect(login, database):
    # Connect to database
    db = pymysql.connect(host=login["aad"]["host"],
                         user=login["aad"]["username"],
                         passwd=login["aad"]["password"],
                         db=database)
    return db


def stats(faculty, students):
    # Number of mentions per faculty
    fac = {}
    for f in faculty:
        fac_of_interest = 0
        for stu in students:
            if f in students[stu]["faculty"]:
                fac_of_interest += 1

        fac[faculty[f]["name"]] = fac_of_interest

    print("Number of mentions per faculty")
    for f in sorted(fac, reverse=True, key=lambda f: fac[f]):
        print(f, fac[f])

    mean = sum(fac.values())/len(fac)
    std = math.sqrt(sum([(mean - m)**2 for m in fac.values()])/len(fac))
    print("Mean number of mentions per faculty: {}".format(mean))
    print("Standard deviation: {}".format(std))

    print("Number of faculty mentionned: {}".format(
        len([1 for f in fac if fac[f]])))

    print(
        f"Matching score distribution (score, number): {get_match_distribution(faculty)}")


if __name__ == "__main__":
    login = toml.load("login.toml")
    db = connect(login, database)
    # Fields information
    fields = get_fields(db, fields_sql)
    # Faculty information
    faculty = get_faculty(db)
    # Student information
    students = get_students(db, applicants_sql, fields)
    # Clean up faculty references
    faculty_of_interest(faculty, students)
    # Compute matching scores
    match(faculty, students, weights)
    # Export scores to database
    export(db, faculty, students)
    # Closes connection
    db.close()
    # Shows stats
    stats(faculty, students)
