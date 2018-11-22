import csv
import editdistance
import pymysql

faculty_sql = """
SELECT userid, username, email, usergroup
FROM logon
WHERE class = 1 -- Faculty
OR userlevel = 2 -- Dean
"""

applicants_sql = """
-- in selection_2019
SELECT a.user_id,
       a.fields,
       a.faculty_last1, a.faculty_first1,
       a.faculty_last2, a.faculty_first2,
       a.faculty_last3, a.faculty_first3,
       e.group -- panel
FROM applicant a
JOIN eval_master e ON e.user_id=a.user_id
WHERE e.batch = 1 -- batch number
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

def get_students(db):
    """
    Calls the database and gets student information.
    Input: database connection object
    Returns: dictionary of student information, key=student ID
    """
    students = {}
    faculty_names = set()

    with db.cursor() as cursor:
        cursor.execute(applicants_sql)
        for name, fields, l1, f1, l2, f2, l3, f3, panel in cursor.fetchall():
            faculty = [[l1, f1], [l2, f2], [l3, f3]]

            students[name] = { "panel"   : panel
                             , "faculty" : faculty
                             , "core"    : []
                             , "minor"   : fields.split('/')[:-1]
                             , "match"   : []
                             }
    return students

def get_faculty(db, unit_path, fields_path):
    """
    Calls the database and files to get faculty information.
    Input: database connection object, paths to files with units names and fields
    Returns: dictionary of faculty information, key=faculty email
    """
    faculty = {}

    with db.cursor() as cursor:
        cursor.execute(faculty_sql)
        for id, name, email, panel in cursor.fetchall():
            faculty[email] = { "name"   : clean_name(name)
                              , "id"    : id
                              , "panel" : panel
                              , "unit"  : "x"*100
                              , "core"  : []
                              , "minor" : []
                              , "match" : []
                              }

    with open(unit_path) as csvfile:
        reader = csv.reader(csvfile)
        for email, unit in reader:
            if email in faculty:
                faculty[email]["unit"] = clean_name(unit)
            else:
                print("Email for Unit not found:", email, unit)

    with open(fields_path) as csvfile:
        reader = csv.reader(csvfile)

        header = reader.__next__()
        fields = [ field[20:-1] for field in header[10:]] # Extracting fields
        fields = [ "".join([c for c in x if c not in " -(),"]) for x in fields] # filtering characters
        fields = [ x[:30] for x in fields] # 30 character cutoff

        for row in reader:
            email, name = row[1:3]

            core_index = [ check == "Core (3 or 4)" for check in row[10:]]
            core = [ field for (check, field) in zip(core_index, fields) if check]

            minor_index = [ check == "Minor (as many as applicable)" for check in row[10:]]
            minor = [ field for (check, field) in zip(minor_index, fields) if check]

            if email in faculty:
                faculty[email]["core"] = core
                faculty[email]["minor"] = minor
            else:
                print(email, "not found")

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

            not_in.add(lf)

        students[stu]["faculty"] = fac

    print("{:%} of names are accounted for".format(accounted_for/3/len(students)))
    print("{} names are not found:\n{}".format(len(not_in), not_in))


def match(faculty, students):
    """
    Compares faculty and student fields of interest to compute a matching score
    Input: faculty and student dictionnaries
    Returns: Nothing
    """
    interest_score = 15
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


if __name__ == "__main__":
    # Connect to database
    with open("password.txt") as f: # "password.txt" is not shared on GitHub
        [user, password] = f.read().split()
    db = pymysql.connect(host   = "aad.oist.jp",    # your host, usually localhost
                         user   = user,             # your username
                         passwd = password,         # your password
                         db     = "selection_2019") # name of the database

    # Faculty information
    faculty = get_faculty(db, "input/units.csv", "input/faculty_fields.csv")
    # Student information
    students = get_students(db)
    # Data cleanup
    fix_names(faculty, students)
    # Compute matching scores
    match(faculty, students)
    # Export scores to database
    export(db, students)
    # Closes connection
    db.close()
