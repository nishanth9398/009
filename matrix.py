import screening as m
import random
import pymysql
import csv
import toml
from collections import Counter, defaultdict
from pyomo.environ import *
from pyomo.opt import SolverFactory

year = 2020

interview_number = 4
max_faculty_interview = 7

interview_low_score = 10 # Prints interviews with scores that low

weights = { "core core": 10
          , "core minor": 5
          , "minor minor": 2
          , "interest": 100
          , "invite": 50 # Faculty wants to interview
          , "rejected": -10 # Faculty said no during screening
          , "force": 10000 # Forcing an interview (by hand only)
          , "intern": -10000 # Candidate was an intern (by hand)
          , "unavailable": -10000 # Faculty not available
          , "timeslot" : 1 # max point for best timeslots
          }

aws_students_sql = """
-- in selection_2019+
SELECT a.family_n, a.given_n,
       a.user_id,
       a.fields,
       a.faculty_last1, a.faculty_first1,
       a.faculty_last2, a.faculty_first2,
       a.faculty_last3, a.faculty_first3,
       e.comment -- comment from committee
FROM applicant a
JOIN eval_master e ON e.user_id=a.user_id
"""

matrix_students_sql = """
-- in matrix
SELECT embark_id
FROM student
"""

matrix_faculty_sql = """
SELECT faculty_id, name, email FROM faculty;
"""

faculty_unavail  = """
SELECT faculty, timeslot
FROM matrix
WHERE student LIKE \"X%\"
"""

# Taken directly from database, sorted by desirability
sorted_timeslots = ["5", "9", "16", "7", "11", "18", "6", "10", "17", "8", "12", "19"]

wants_interview = """
-- in selection_2019+
SELECT e.user_id, userid
FROM eval_detail e
JOIN logon l on e.id_examiner = l.userid
WHERE interview = "yes";
"""

said_no = """
-- in selection_2019+
SELECT e.user_id, userid
FROM eval_detail e
JOIN logon l on e.id_examiner = l.userid
WHERE invite = 1 -- 1: No, 2: Maybe, 3: Invite;
"""

def defered_students(students, path):
    stu = {}
    with open(path) as csvfile:
        reader = csv.reader(csvfile)
        for last, first, name, fields, l1, f1, l2, f2, l3, f3, com in reader:
            faculty = [[l1, f1], [l2, f2], [l3, f3]]

            stu[name] =  { "name"    : last + " " + first
                         , "faculty" : faculty
                         , "core"    : []
                         , "minor"   : fields.split('/')[:-1]
                         , "match"   : []
                         , "comment" : com
                         }
    students.update(stu)

def show_comments(students):
    print("Showing Comment:")
    for stu in students:
        comment = students[stu]["comment"]
        if comment:
            faculty = students[stu]["faculty"]
            print("Student {} {}: {}".format(stu, faculty, comment))
    print("End of comments\n")

def add_availability_and_check(db, faculty, students):
    """
    Adds availability from faculty
    Also check IDs are consistent
    Removes students not in matrix (deferred or cancelled students)
    """

    # First delete students from matrix to start fresh
    with db.cursor() as cursor:
        cursor.execute("DELETE FROM matrix WHERE student NOT LIKE \"X_%\";")

    # Checking faculty IDs are consistent
    print("\nFaculty ID checks:")
    fac_matrix = {}
    with db.cursor() as cursor:
        cursor.execute(matrix_faculty_sql)
        for id, name, email in cursor.fetchall():
            fac_matrix[id] = (name, email.strip().lower())

    # Check if IDs from matrix are all in selection
    for id in fac_matrix:
        name, email = fac_matrix[id]
        if id in faculty:
            if faculty[id]["email"] != email:
                name2 = faculty[id]["name"]
                email2 = faculty[id]["email"]
                print(f"ID {id}: matrix faculty {name} {email} different from selection faculty {name2} {email2}")
        else:
            print(f"Faculty {name}, {email}, ID {id} in matrix not found in selection_{year}")

    # Check if IDs from selection are in matrix
    for id in faculty:
        name2 = faculty[id]["name"]
        email2 = faculty[id]["email"]
        if id in fac_matrix:
            name, email = fac_matrix[id]
            if email2 != email:
                print(f"ID {id}: selection faculty {name2} {email2} different from matrix faculty {name} {email}")
        else:
            print(f"Faculty {name2}, {email2}, ID {id} in selection_{year} not found in matrix")
    print("End of facultyID checks\n")

    # Adding availability data
    for f in faculty:
        faculty[f]['avail'] = [x for x in sorted_timeslots]

    with db.cursor() as cursor:
        cursor.execute(faculty_unavail)
        for id, unavail in cursor.fetchall():
            if id in faculty:
                if unavail in faculty[id]["avail"]:
                    faculty[id]["avail"].remove(unavail)
            else:
                print("Matrix Faculty not found in selection faculty", id)

    # Filtering to students in matrix
    with db.cursor() as cursor:
        cursor.execute(matrix_students_sql)
        stu_avail = [s[0] for s in cursor.fetchall()]

    stu2 = {s:students[s] for s in students if s in stu_avail}
    fac2 = {f:faculty[f] for f in faculty if len(faculty[f]["avail"]) > 0 and f in fac_matrix}
    return (fac2, stu2)

def force_interviews(students, forced):
    """
    Manually forcing interviews
    Mutates students
    """
    for stu, fac in forced:
        students[stu]["match"][fac] += weights["forced"]

def reject_interns(students, interns):
    """
    Avoiding interviews with previous interns
    Mutates students
    """
    for stu, fac in interns:
        students[stu]["match"][fac] += weights["intern"]

def requested_interviews(db, faculty, students):
    """
    This is to prioritize interviews that were requested by faculty
    Mutates students
    """
    with db.cursor() as cursor:
        cursor.execute(wants_interview)
        for stu, fac_id in cursor.fetchall():
            if stu in students:
                matches = students[stu]["match"]
                if fac_id in matches:
                    matches[fac_id] += weights["invite"]

def rejected_students(db, faculty, students):
    """
    This is to avoid interviews with students who were rejected by specific faculty
    Mutates students
    """
    with db.cursor() as cursor:
        cursor.execute(said_no)
        for stu, fac_id in cursor.fetchall():
            if stu in students:
                matches = students[stu]["match"]
                if fac_id in matches:
                    matches[fac_id] += weights["rejected"]

def make_matrix(faculty_all, students_all):
    # Define parameters
    times = sorted_timeslots
    faculty = faculty_all.keys()
    students = students_all.keys()

    time_pref = { time: weights["timeslot"]*i/(len(sorted_timeslots)-1) \
                        for i, time in enumerate(reversed(sorted_timeslots))}

    # Initialize model
    model = ConcreteModel()

    # binary variables representing the time and session of each fac
    model.grid = Var(((fac, stu, time) for fac in faculty for stu in students for time in times) ,
                           within=Binary, initialize=0)

    # Define an objective function with model as input, to pass later
    def obj_rule(m):
        timing = sum(m.grid[fac, stu, time] * time_pref[time] for fac, stu, time in m.grid)

        matching = sum(m.grid[fac, stu, time] * students_all[stu]["match"][fac] \
                                                   for fac, stu, time in m.grid)

        unavailable = sum(m.grid[fac, stu, time] * (time not in faculty_all[fac]["avail"] ) \
                                                   for fac, stu, time in m.grid)

        return timing + matching + weights["unavailable"]*unavailable

    # add objective function to the model. rule (pass function) or expr (pass expression directly)
    model.obj = Objective(rule=obj_rule, sense=maximize)

    model.constraints = ConstraintList()  # Create a set of constraints

    # Constraint: N interviews per student
    for stu in students:
        model.constraints.add(
            sum(model.grid[fac, stu, time] for fac in faculty for time in times) \
             == interview_number
        )

    # Constraint: Maximum interviews per faculty
    for fac in faculty:
        model.constraints.add(
            sum(model.grid[fac, stu, time] for stu in students for time in times) \
             <=  max_faculty_interview
        )

    # Constraint: Max one interview per time per faculty
    for fac in faculty:
        for time in times:
            model.constraints.add(
                sum( model.grid[fac, stu, time] for stu in students) <= 1
            )

    # Constraint: Max one interview per time per student
    for stu in students:
        for time in times:
            model.constraints.add(
                sum( model.grid[fac, stu, time] for fac in faculty) <= 1
            )

    # Constraint: each student/faculty pair interviews maximum once
    for stu in students:
        for fac in faculty:
            model.constraints.add(
                sum( model.grid[fac, stu, time] for time in times ) <= 1
            )


    model.preprocess()
    # opt = SolverFactory('cbc', validate = False)  # Select solver
    # solver_manager = SolverManagerFactory('neos')  # Solve in neos server
    # results = solver_manager.solve(model, opt=opt)

    opt = SolverFactory('cbc')
    results = opt.solve(model) # Solve locally

    print(results)

    matrix = []
    for fac, stu, time in model.grid:
        if model.grid[fac, stu, time].value:
            matrix.append((fac, stu, time))

    return matrix

def matrix_analysis(matrix_original, faculty, students):
    matrix = defaultdict(list)
    for fac, stu, _ in matrix_original:
        matrix[stu].append((students[stu]["match"][fac], fac))

    print()
    av = 0
    for stu in matrix:
        s = [score for score, _ in matrix[stu]]
        if any([score < interview_low_score for score in s]):
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
    for f in sorted(faculty, key=lambda f: len(fac.get(f,[])), reverse = True):
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

def export_matrix(db, matrix):
    """
    Rewrites the database matrix after manual confirm.
    Input: database connection object, matrix
    Returns: Nothing
    """
    with db.cursor() as cursor:
        query = "INSERT INTO matrix (student, faculty, timeslot) VALUES {};"
        values = []
        for fac, stu, t in matrix:
            values.append(f"({stu}, {fac}, {t})")
        cursor.execute(query.format(", ".join(values)))
    db.commit()

    """
    Checks that faculty and students IDs from selection and matrix match
    """
    pass

if __name__ == "__main__":
    login = toml.load("login.toml")
    selection_db = m.connect(login, f"selection_{year}")
    matrix_db = m.connect(login, "matrix")

    # Faculty information
    faculty = m.get_faculty(selection_db, "input/units.csv")
    # Hard fixes
    # del faculty["1"] # robertbaughman
    # del faculty["23"] # mukhlessowwan
    # del faculty["26"] # Stephens
    # del faculty["29"] # Mikheyev
    # del faculty["64"] # danielrokhsar
    # del faculty["72"] # anastasiiatsvietkova
    # del faculty["83"] # milindpurohit
    # del faculty["76"] # Pauly
    # faculty["101"] = faculty["833"] # Wrong id?
    # del faculty["833"] # xiaodanzhou

    # Fields information
    fields = m.get_fields(selection_db, m.fields_sql)
    # Student information
    students = m.get_students(selection_db, aws_students_sql, fields)
    # Student from last year
    defered_students(students, "input/defered.csv")
    # Add availabilities
    (faculty, students) = add_availability_and_check(matrix_db, faculty, students)
    # Data cleanup
    m.fix_names(faculty, students)
    # Show comments
    show_comments(students)
    # Manually adding faculty of interest
    # students['75218934']["faculty"].append("keikokono")
    # Compute matching scores
    m.match(faculty, students, weights)
    # Special case for Yanagida sensei => Zhang HaoLing
    # forced = [("75183162", "42")]
    forced = []
    force_interviews(students, forced)
    # Avoiding pairing intern students
    print("\nRemember to remove past interns!")
    # interns = [("75183162", "42")] :: [(student ID, faculty ID)]
    interns = []
    reject_interns(students, interns)
    # Add interviews that faculty requested
    requested_interviews(selection_db, faculty, students)
    # Interviews to avoid
    rejected_students(selection_db, faculty, students)
    # Closes connection
    # Make matrix
    matrix = make_matrix(faculty, students)
    # Analyze stats
    matrix_analysis(matrix, faculty, students)
    # # Export data
    export_matrix(matrix_db, matrix)

    selection_db.close()
    matrix_db.close()
