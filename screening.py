import csv
import editdistance
import pymysql
import math

faculty_sql = """
SELECT userid, username, email, usergroup
FROM logon
WHERE (class = 1 -- Faculty
OR userlevel = 1) -- Dean
AND email IS NOT NULL
"""

faculty_fields_sql = """
SELECT faculty_id, fields_id, importance FROM faculty_fields
"""

fields_sql = """
SELECT id, field from fields
"""

applicants_sql = """
-- in selection_2020
SELECT a.family_n, a.given_n,
       a.user_id,
       a.fields,
       a.faculty_last1, a.faculty_first1,
       a.faculty_last2, a.faculty_first2,
       a.faculty_last3, a.faculty_first3,
       e.group, -- panel
       e.comment -- comment from committee
FROM applicant a
JOIN eval_master e ON e.user_id=a.user_id
WHERE e.batch = 1 -- batch number
-- AND e.rubbish = 2 -- pre-selected candidates
"""



def clean_name(name):
    """
    Clean up names for comparaisons.
    Gets rid of punctuation, spaces, casing and more.
    Input: string
    Returns: string
    """
    n = name.lower()
    n = n.replace("prof", "")
    n = n.replace("í", "i") # For Sile
    n = n.replace("eileen", "") # For Gail
    n =  "".join([c for c in n if c.isalpha()])
    return n

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
        for last, first, name, minor, l1, f1, l2, f2, l3, f3, panel, comment \
                   in cursor.fetchall():
            faculty = [[l1, f1], [l2, f2], [l3, f3]]
            minor = [fields[f] for f in minor.split('/')[:-1]]

            students[name] = { "name"    : last + " " + first
                             , "panel"   : panel
                             , "faculty" : faculty
                             , "core"    : []
                             , "minor"   : minor
                             , "match"   : []
                             , "comment" : comment
                             }
    return students

def get_faculty(db, unit_path, fields_path):
    """
    Calls the database and files to get faculty information.
    Input: database connection object, paths to files with units names and fields
    Returns: dictionary of faculty information, key=faculty ID
    """
    faculty = {}
    email_to_id = {}

    with db.cursor() as cursor:
        cursor.execute(faculty_sql)
        for id, name, email, panel in cursor.fetchall():
            faculty[str(id)] = \
                  { "name"  : clean_name(name)
                  , "email" : email
                  , "id"    : str(id)
                  , "panel" : panel
                  , "unit"  : "x"*100
                  , "core"  : []
                  , "minor" : []
                  , "match" : []
                  , "avail" : 8
                  }
            email_to_id[email.lower()] = str(id)

    with open(unit_path) as csvfile:
        reader = csv.reader(csvfile)
        for email, unit in reader:
            email = email.lower()
            if email_to_id.get(email,-1) in faculty:
                faculty[email_to_id[email]]["unit"] = clean_name(unit)
            else:
                print("Email for Unit not found:", email, unit)

    with db.cursor() as cursor:
        cursor.execute(faculty_fields_sql)
        for fac, field, importance in cursor.fetchall():
            faculty[str(fac)][importance].append(field)

    return faculty

def fix_names(faculty, students):
    """
    Matches and updates students' faculty of interest (free input field) to faculty
    Input: faculty and student dictionnaries
    Returns: Nothing
    """
    real = [faculty[fac]["name"] for fac in faculty]
    accounted_for = 0
    not_in = set()
    cont = False

    for stu in students:
        fac = []
        for last, first in students[stu]["faculty"]:
            lf = clean_name(last + first)
            fl = clean_name(first + last)

            if lf == "":
                accounted_for += 1
                continue

            if lf in real:
                fac.append(lf)
                accounted_for += 1
                continue

            if fl in real:
                fac.append(fl)
                accounted_for += 1
                continue

            for name in real:
                if (name in fl) or (name in lf):
                    fac.append(name)
                    accounted_for += 1
                    cont = True
                    break
            if cont:
                cont = False
                continue

            dist_lf = [(editdistance.eval(lf, name), name) for name in real]
            dist_fl = [(editdistance.eval(fl, name), name) for name in real]
            best = min(dist_fl + dist_lf)
            if best[0] < 5:
                accounted_for += 1
                fac.append(best[1])
                continue
                # if best[0] == 4:
                #     print(lf, best)

            dist_lf = [(editdistance.eval(lf, faculty[f]["unit"]), faculty[f]["name"]) for f in faculty]
            dist_fl = [(editdistance.eval(fl, faculty[f]["unit"]), faculty[f]["name"]) for f in faculty]
            best = min(dist_fl + dist_lf)
            if best[0] < 3/10*len(fl):
                # print(lf, best)
                accounted_for += 1
                fac.append(best[1])
                continue

            not_in.add((lf, stu))

        students[stu]["faculty"] = fac

    print("{:%} of names are accounted for".format(accounted_for/3/len(students)))
    print("{} names are not found:\n{}".format(len(not_in), not_in))

def match(faculty, students, interview=False):
    """
    Compares faculty and student fields of interest to compute a matching score
    Input: faculty and student dictionnaries
    Returns: Nothing
    """
    interest_score = 15
    if interview:
        interest_score = 100
    co_co_score = 10
    co_mi_score = 5
    mi_mi_score = 2
    panel_score = 1

    for stu in students:
        for fac in faculty:
            co_co = sum([f in faculty[fac]["core"] for f in students[stu]["core"]])
            co_mi = sum([f in faculty[fac]["core"] for f in students[stu]["minor"]])
            mi_co = sum([f in faculty[fac]["minor"] for f in students[stu]["core"]])
            mi_mi = sum([f in faculty[fac]["minor"] for f in students[stu]["minor"]])

            score = co_co_score * co_co +  co_mi_score * co_mi \
                    + co_mi_score * mi_co + mi_mi_score * mi_mi

            if faculty[fac]["name"] in students[stu]["faculty"]:
                score += interest_score

            if faculty[fac]["panel"] == students[stu]["panel"]:
                score += panel_score

            students[stu]["match"].append((score, faculty[fac]["id"]))
            faculty[fac]["match"].append((score, stu))

        students[stu]["match"].sort(key = lambda x : -x[0])

    for fac in faculty:
        faculty[fac]["match"].sort(key = lambda x : -x[0])

def confirm():
    """
    Ask user to enter Y or N (case-insensitive), returns True if the answer is Y.
    Input: None
    Returns: Bool
    """
    answer = ""
    while answer not in ["y", "n"]:
        answer = input("Would you like to update the database? [Y/N]? ").lower()
    return answer == "y"

def export(db, students):
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
                values = ["({}, {}, {})".format(stu, fac, score) for score, fac in students[stu]["match"]]
                cursor.execute(query.format(", ".join(values)))
        db.commit()

def get_match_distribution(faculty):
    dist = [0]*100
    for fac in faculty:
        for score, _ in faculty[fac]["match"]:
            dist[score] += 1
    return dist

def connect():
    # Connect to database
    with open("password.txt") as f: # "password.txt" is not shared on GitHub
        [user, password] = f.read().split()
    db = pymysql.connect(host   = "aad.oist.jp",    # your host, usually localhost
                         user   = user,             # your username
                         passwd = password,         # your password
                         db     = "selection_2020") # name of the database
    return db


def stats(faculty, students):
    # Number of mentions per faculty
    fac = {}
    for f in faculty:
        fac_of_interest = 0
        for stu in students:
            if faculty[f]["name"] in students[stu]["faculty"]:
                fac_of_interest += 1

        fac[faculty[f]["name"]] = fac_of_interest

    print("Number of mentions per faculty")
    for f in sorted(fac, reverse=True, key=lambda f: fac[f]):
        print(f, fac[f])

    mean = sum(fac.values())/len(fac)
    std = math.sqrt(sum([ (mean - m)**2 for m in fac.values() ])/len(fac))
    print("Mean number of mentions per faculty: {}".format(mean))
    print("Standard deviation: {}".format(std))

    print("Number of faculty mentionned: {}".format(len(fac)))



if __name__ == "__main__":
    db = connect()
    # Fields information
    fields = get_fields(db, fields_sql)
    # Faculty information
    faculty = get_faculty(db, "input/units.csv", "input/faculty_fields.csv")
    # Student information
    students = get_students(db, applicants_sql, fields)
    # Data cleanup
    fix_names(faculty, students)
    # Compute matching scores
    match(faculty, students)
    # Export scores to database
    export(db, students)
    # Closes connection
    db.close()
    # Shows stats
    stats(faculty, students)
