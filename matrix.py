import screening as m
import random
import pymysql
import csv
from collections import Counter

interview_number = 4
batch_number = 1
max_faculty_interview = 7

aws_students_sql = """
-- in selection_2019+
SELECT a.family_n, a.given_n,
       a.user_id,
       a.fields,
       a.faculty_last1, a.faculty_first1,
       a.faculty_last2, a.faculty_first2,
       a.faculty_last3, a.faculty_first3,
       e.group, -- panel
       e.comment -- comment from committee
FROM applicant a
JOIN eval_master2 e ON e.user_id=a.user_id
"""

matrix_students_sql = """
-- in matrix
SELECT embark_id
FROM student
"""

faculty_avail = """
-- in matrix
SELECT faculty_id, email, 12-COUNT(*) c
FROM faculty f
JOIN matrix m ON f.faculty_id = m.faculty
GROUP BY faculty_id
"""

wants_interview = """
-- in selection_2019+
SELECT user_id, userid
FROM eval_detail e
JOIN logon l on e.id_examiner = l.userid
WHERE interview = "yes";
"""

said_no = """
-- in selection_2019+
SELECT user_id, email
FROM eval_detail e
JOIN logon l on e.id_examiner = l.userid
WHERE invite = 1 -- 1: No, 2: Maybe, 3: Invite;
"""

# matches_sql = """
# -- in selection_2020
# SELECT user_id, faculty_id, closeness
# FROM field_matrix
# """

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
    """
    Adds availability from faculty
    Removes students not in matrix (deferred or cancelled students)
    """
    with open("password.txt") as f: # "password.txt" is not shared on GitHub
        [user, password] = f.read().split()

    db = pymysql.connect(host   = "aad.oist.jp",    # your host, usually localhost
                         user   = user,             # your username
                         passwd = password,         # your password
                         db     = "matrix")         # name of the database

    for f in faculty:
        faculty[f]['avail'] = max_faculty_interview

    with db.cursor() as cursor:
        cursor.execute(faculty_avail)
        for id, email, avail in cursor.fetchall():
            if id in faculty:
                faculty[id]["avail"] = min(max_faculty_interview, avail)
            else:
                print("Faculty not found", email)

    with db.cursor() as cursor:
        cursor.execute(matrix_students_sql)
        stu_avail = [s[0] for s in cursor.fetchall()]

    fac2 = {f:faculty[f] for f in faculty if faculty[f]["avail"] > 0}
    stu2 = {s:students[s] for s in students if s in stu_avail}
    return (fac2, stu2)

# This is to prioritize interviews that were requested by faculty
def requested_interviews(db, faculty, students):
    faculty_invite = 200

    with db.cursor() as cursor:
        cursor.execute(wants_interview)
        for stu, fac_id in cursor.fetchall():
            if stu in students and fac_id in faculty:
                matches = students[stu]["match"]
                matches = [(score, fac) for (score, fac) in matches if fac != fac_id]
                matches = [(faculty_invite, fac_id)] + matches
                students[stu]["match"] = matches

# This is to avoid interviews with students who were rejected by specific faculty
def rejected_students(db, faculty, students):
    rejections = {}

    with db.cursor() as cursor:
        cursor.execute(said_no)
        for stu, fac_email in cursor.fetchall():
            if stu in students and fac_email in faculty:
                rejections[stu] = rejections.get(stu, []) + [faculty[fac_email]["id"]]
    return rejections

def make_matrix(faculty, students, forced, rejected):
    matrix = { stu : [] for stu in students }
    fac_ids = {faculty[f]["id"] : f for f in faculty }

    for stu, fac in forced:
        faculty[fac_ids[fac]]["avail"] -= 1
        students[stu]["match"] = list(filter(lambda x: x[1] != fac, students[stu]["match"]))
        matrix[stu].append((30, fac))

    for _ in range(interview_number):
        order = list(students.keys())

        # order 1: Random order
        random.shuffle(order)

        # # order 2: By score
        # order.sort(key = lambda stu : -students[stu]["match"][0][0])

        for stu in order:
            (score, fac) = students[stu]["match"].pop(0)

            while fac not in fac_ids or \
                  stu in rejected and fac in rejected[stu] or \
                  faculty[fac_ids[fac]]["avail"] == 0 :
                # print("No luck", score)
                if len(students[stu]["match"]) == 0:
                    print("No more choice", stu, matrix)
                (score, fac) = students[stu]["match"].pop(0)

            matrix[stu].append((score, fac))
            faculty[fac_ids[fac]]["avail"] -= 1
    return matrix

def matrix_analysis(matrix, faculty, students):
    # fac_names = {}
    # for f in faculty:
    #     fac_names[faculty[f]["id"]] = faculty[f]["name"]

    print()
    av = 0
    for stu in matrix:
        s = [score for score, _ in matrix[stu]]
        if any([score < 5 for score in s]):
            intervs = [(sc, faculty[f]["name"]) for sc, f in matrix[stu]]
            print("Student ", students[stu]["name"], " has some low interview scores ", intervs)
        av += sum(s)
    av /= interview_number * len(matrix)
    print("Average score per candidate: {}".format(av))
    print("Number of candidates: {}".format(len(matrix)))

    fac = {}
    for stu in matrix:
        for score, f in matrix[stu]:
            if f in fac:
                fac[f].append((score, stu))
            else:
                fac[f] = [(score, stu)]

    print()
    dist = {}
    av = 0
    ints = 0
    print("Faculty with less than 5 interviews:")
    for f in faculty:
        if f not in fac:
            print(faculty[f]["name"], "has no interviews")
        else:
            av += sum([score for score, _ in fac[f]])/len(fac[f])
            ints += len(fac[f])
            if len(fac[f]) < 5 :
                print(faculty[f]["name"], "has", len(fac[f]), "interviews")
    av /= len(fac)
    ints /= len(fac)
    print()
    print("Average score per faculty: {}".format(av))
    print("Average number of interviews per faculty: {}".format(ints))
    c = Counter([len(fac[f]) for f in fac])
    print("Distribution (number of ints, count): {}".format(sorted(list(c.items()))))
    print("Number of faculty with interviews: {}".format(len(fac)))

def export_matrix(path, faculty, matrix):
    facID = [faculty[f]["id"] for f in faculty]

    with open(path, "w") as f:
        f.write("Faculty ID," + ",".join(map(str, facID)))
        for stu in matrix:
            facList = []
            ids = [i for score, i in matrix[stu]]
            for fac in facID:
                # no more in-depth interviews
                # if fac == matrix[stu][0][1]:
                #     facList.append("2")
                if fac in ids:
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
                # if fac == matrix[stu][0][1]:
                #     facList.append("*{}*".format(matrix[stu][0][0]))
                if fac in ids:
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
    faculty = m.get_faculty(db, "input/units.csv")
    # Hard fixes
    del faculty["1"] # robertbaughman
    del faculty["23"] # mukhlessowwan
    del faculty["26"] # Stephens
    del faculty["29"] # Mikheyev
    del faculty["64"] # danielrokhsar
    del faculty["72"] # anastasiiatsvietkova
    del faculty["83"] # milindpurohit
    del faculty["76"] # Pauly
    faculty["101"] = faculty["833"] # Wrong id?
    del faculty["833"] # xiaodanzhou

    # Fields information
    fields = m.get_fields(db, m.fields_sql)
    # Student information
    students = m.get_students(db, aws_students_sql, fields)
    # Student from last year
    defered_students(students, "input/defered.csv")
    # Data cleanup
    m.fix_names(faculty, students)
    # Add availabilities
    (faculty, students) = add_availability(faculty, students)
    # Show comments
    show_comments(students)
    # Manually adding faculty of interest
    # students['75218934']["faculty"].append("keikokono")
    # Compute matching scores
    m.match(faculty, students, interview=True)
    # Special case for Yanagida sensei => Zhang HaoLing
    # forced = [("75183162", "42")]
    forced = []
    # Add interviews that faculty requested
    requested_interviews(db, faculty, students)
    # Interviews to avoid
    reject = rejected_students(db, faculty, students)
    # Closes connection
    db.close()
    # Make matrix
    matrix = make_matrix(faculty, students, forced, reject)
    # Analyze stats
    matrix_analysis(matrix, faculty, students)
    # Export data
    export_matrix("output/IM_Feb20.csv", faculty, matrix)
    export_human("output/matrix_human.csv", faculty, students, matrix)
