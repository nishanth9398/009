import screening as m
import random
import pymysql
import csv
from collections import Counter

positive_sql = """
-- in selection_2019
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
      AND e.status = 1 -- positive status
"""

faculty_avail = """
-- in matrix
SELECT faculty_id, email, 22-COUNT(*) c
FROM faculty f
JOIN matrix m ON f.faculty_id = m.faculty
GROUP BY faculty_id HAVING c>0
"""

student_avail = "select embark_id from student"

def defered_students(students, path):
    stu = {}
    with open(path) as csvfile:
        reader = csv.reader(csvfile)
        for last, first, name, fields, l1, f1, l2, f2, l3, f3, panel, com in reader:
            faculty = [[l1, f1], [l2, f2], [l3, f3]]

            stu[name] =  { "name"    : last + " " + first
                         , "panel"   : panel
                         , "faculty" : faculty
                         , "core"    : []
                         , "minor"   : fields.split('/')[:-1]
                         , "match"   : []
                         , "comment" : com
                         }
    students.update(stu)

def show_comments(students):
    for stu in students:
        comment = students[stu]["comment"]
        if comment:
            faculty = students[stu]["faculty"]
            print("Student {} {}: {}".format(stu, faculty, comment))

def add_availability(faculty, students):
    with open("password.txt") as f: # "password.txt" is not shared on GitHub
        [user, password] = f.read().split()

    db = pymysql.connect(host   = "aad.oist.jp",    # your host, usually localhost
                         user   = user,             # your username
                         passwd = password,         # your password
                         db     = "matrix")         # name of the database

    with db.cursor() as cursor:
        cursor.execute(faculty_avail)
        for id, email, avail in cursor.fetchall():
            if email in faculty:
                faculty[email]["avail"] = min(8, avail) # Maximum number of interviews
                faculty[email]["id"] = id # Override the id because the databases don't match
            else:
                print("Faculty not found", email)

    with db.cursor() as cursor:
        cursor.execute(student_avail)
        stu_avail = [s[0] for s in cursor.fetchall()]

    fac2 = {f:faculty[f] for f in faculty if faculty[f]["avail"] > 0}
    stu2 = {s:students[s] for s in students if s in stu_avail}
    return (fac2, stu2)

def make_matrix(faculty, students, forced):
    matrix = { stu : [] for stu in students }
    fac_ids = {faculty[f]["id"] : f for f in faculty }

    for stu, fac in forced:
        faculty[fac_ids[fac]]["avail"] -= 1
        students[stu]["match"] = list(filter(lambda x: x[1] != fac, students[stu]["match"]))
        matrix[stu].append((30, fac))

    for _ in range(5):
        order = list(students.keys())

        # order 1: Random order
        random.shuffle(order)

        # # order 2: By score
        # order.sort(key = lambda stu : -students[stu]["match"][0][0])

        for stu in order:
            (score, fac) = students[stu]["match"].pop(0)
            while faculty[fac_ids[fac]]["avail"] == 0:
                # print("No luck", score)
                (score, fac) = students[stu]["match"].pop(0)

            matrix[stu].append((score, fac))
            faculty[fac_ids[fac]]["avail"] -= 1
    return matrix

def matrix_analysis(matrix):
    av = 0
    for stu in matrix:
        av += sum([score for score, _ in matrix[stu]])
    av /= 5 * len(matrix)
    print("Average score per candidate: {}".format(av))
    print("Number of candidates: {}".format(len(matrix)))

    fac = {}
    for stu in matrix:
        for score, f in matrix[stu]:
            if f in fac:
                fac[f].append((score, stu))
            else:
                fac[f] = [(score, stu)]

    dist = {}
    av = 0
    ints = 0
    for f in fac:
        av += sum([score for score, _ in fac[f]])/len(fac[f])
        ints += len(fac[f])
    av /= len(fac)
    ints /= len(fac)
    print("Average score per faculty: {}".format(av))
    print("Average number of interviews per faculty: {}".format(ints))
    c = Counter([len(fac[f]) for f in fac])
    print("Distribution (number of ints, count): {}".format(sorted(list(c.items()))))
    print("Number of faculty: {}".format(len(fac)))

def export_matrix(path, faculty, matrix):
    facID = [faculty[f]["id"] for f in faculty]

    with open(path, "w") as f:
        f.write("Faculty ID," + ",".join(map(str, facID)))
        for stu in matrix:
            facList = []
            ids = [i for score, i in matrix[stu]]
            for fac in facID:
                if fac == matrix[stu][0][1]:
                    facList.append("2")
                elif fac in ids:
                    facList.append("1")
                else:
                    facList.append("")
            f.write("\n" + stu + "," + ",".join(facList))

def export_human(path, faculty, students, matrix):
    fac_count = {}
    for stu in matrix:
        for score, f in matrix[stu]:
            if f in fac_count:
                fac_count[f].append(stu)
            else:
                fac_count[f] = [stu]

    facName = [faculty[f]["name"] for f in faculty]
    facID = [faculty[f]["id"] for f in faculty]
    intCount = [str(len(fac_count.get(f, []))) for f in facID]

    with open(path, "w") as f:
        f.write(",Faculty ID," + ",".join(map(str, facID)) + "\n")
        f.write(",Faculty name," + ",".join(facName) + "\n")
        f.write(",Interview count," + ",".join(intCount))
        for stu in matrix:
            facList = []
            ids = {i : score for score, i in matrix[stu]}
            for fac in facID:
                if fac == matrix[stu][0][1]:
                    facList.append("*{}*".format(matrix[stu][0][0]))
                elif fac in ids:
                    if ids[fac] < 5: # Out of field interview
                        facList.append("_{}_".format(ids[fac]))
                    else:
                        facList.append(str(ids[fac]))
                else:
                    facList.append("")
            f.write("\n" + stu + "," + students[stu]["name"] + "," + ",".join(facList))

if __name__ == "__main__":
    db = m.connect()
    # Faculty information
    faculty = m.get_faculty(db, "input/units.csv", "input/faculty_fields.csv")
    # Student information
    students = m.get_students(db, positive_sql)
    # Closes connection
    db.close()
    # Student from last year
    defered_students(students, "input/2018_applicants.csv")
    # Add availabilities
    (faculty, students) = add_availability(faculty, students)
    # Data cleanup
    m.fix_names(faculty, students)
    # Show comments
    show_comments(students)
    # hand fixes from comments
    students["75206480"]["faculty"].append("thomasbourguignon")
    students["75206480"]["faculty"].append("alexandermikheyev")
    students["75207047"]["faculty"].append("deniskonstantinov")
    # Compute matching scores
    m.match(faculty, students)
    # Special case for Yanagida sensei => Zhang HaoLing
    forced = [("75183162", "42")]
    # Make matrix
    matrix = make_matrix(faculty, students, forced)
    # Analyze stats
    matrix_analysis(matrix)
    # Export data
    export_matrix("output/IM_Feb19.csv", faculty, matrix)
    export_human("output/matrix_human.csv", faculty, students, matrix)
