import screening as m
import random
import pymysql
import csv
import toml
from collections import Counter, defaultdict
from pyomo.environ import *
from pyomo.opt import SolverFactory

year = "2020b"
workshop = "Jun"
batch = 2

interview_number = 4
max_faculty_interview = 6

interview_low_score = 10 # Prints interviews with scores that low

weights = { "core core": 10
          , "core minor": 5
          , "minor minor": 2
          , "interest": 100
          , "invite": 50 # Faculty wants to interview
          , "rejected": -10 # Faculty said no during screening
          , "force": 10000 # Forcing an interview (by hand only)
          , "intern": -100000 # Candidate was an intern (by hand)
          , "unavailable": -100000 # Faculty not available
          , "maybe available": -10 # Faculty maybe available
          , "block" : -10 # Cost for extra time block
          }

# SQL scripts for selection_2020b

aws_students_sql = f"""
-- in selection_2020b
SELECT a.family_n, a.given_n,
       a.user_id,
       a.fi_core, a.fi_sub,
       a.faculty1_id, a.faculty2_id, a.faculty3_id,
       e.comment -- comment from committee
FROM applicant a
LEFT JOIN eval_master e ON e.user_id=a.user_id
"""

wants_interview = """
-- in selection_2020b
SELECT e.user_id, oistid
FROM eval_detail e
JOIN logon l on e.id_examiner = l.userid
WHERE interview = "yes";
"""

said_no = """
-- in selection_2020b
SELECT e.user_id, oistid
FROM eval_detail e
JOIN logon l on e.id_examiner = l.userid
WHERE invite = 1 -- 1: No, 2: Maybe, 3: Invite;
"""

 # SQL scripts for apd_prod

matrix_faculty_sql = """
SELECT faculty_id, name, email FROM faculty;
"""

matrix_students_sql = """
SELECT user_id FROM applicant;
"""

faculty_avail_sql  = """
SELECT faculty_id, time_block_id, available
FROM availabilities
WHERE applicant_id IS NULL
"""

student_avail_sql  = """
SELECT applicant_id, time_block_id, available
FROM availabilities
WHERE faculty_id IS NULL
"""

times_sql = """
select id, time_block_id from times
"""

def time_block(time_id): return (time_id - 1) // 3

def show_comments(students):
    print("Showing Comment:")
    for stu in students:
        name = students[stu]["name"]
        comment = students[stu]["comment"]
        if comment:
            faculty = students[stu]["faculty"]
            print("Student {} ({}) {}: {}".format(name, stu, faculty, comment))
    print("End of comments\n")

def get_times(db):
    # Getting time slot IDs
    times = defaultdict(list)
    with db.cursor() as cursor:
        cursor.execute(times_sql)
        for id, time_block_id in cursor.fetchall():
            times[time_block_id].append(id)
    return times

def add_availability_and_check(db, times, faculty, students):
    """
    Adds availability from faculty
    Also check IDs are consistent
    Removes students not in matrix (deferred or cancelled students)
    """
    # Filtering to students in matrix
    with db.cursor() as cursor:
        cursor.execute(matrix_students_sql)
        stu_avail = [s[0] for s in cursor.fetchall()]

    students = {s:students[s] for s in students if s in stu_avail}

    # Delete everything from matrix to start fresh
    with db.cursor() as cursor:
        cursor.execute("DELETE FROM matrix;")


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
        faculty[f]["avail"] =   defaultdict(list)

    for s in students:
        students[s]["avail"] =  defaultdict(list)

    # Faculty availability
    with db.cursor() as cursor:
        cursor.execute(faculty_avail_sql)
        for id, time_block_id, available in cursor.fetchall():
            if id in faculty:
                for time in times[time_block_id]:
                    faculty[id]["avail"][available].append(time)
            else:
                print("Matrix Faculty not found in selection faculty", id)

    # Student availability
    with db.cursor() as cursor:
        cursor.execute(student_avail_sql)
        for id, time_block_id, available in cursor.fetchall():
            if id in students:
                for time in times[time_block_id]:
                    students[id]["avail"][available].append(time)
            else:
                print("Matrix Student not found in selection applicants", id)

    fac2 = {f:faculty[f] for f in faculty if len(faculty[f]["avail"]) > 0 and f in fac_matrix}
    return (fac2, students)

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
    n = 0
    with db.cursor() as cursor:
        cursor.execute(wants_interview)
        for stu, fac_id in cursor.fetchall():
            if stu in students:
                matches = students[stu]["match"]
                if fac_id in matches:
                    matches[fac_id] += weights["invite"]
                    n += 1
                    print(faculty[fac_id]["name"] , "\t", students[stu]["name"] )
    print(n, "requested interviews")

def rejected_students(db, faculty, students):
    """
    This is to avoid interviews with students who were rejected by specific faculty
    Mutates students
    """
    n = 0
    with db.cursor() as cursor:
        cursor.execute(said_no)
        for stu, fac_id in cursor.fetchall():
            if stu in students:
                matches = students[stu]["match"]
                if fac_id in matches:
                    matches[fac_id] += weights["rejected"]
                    n += 1
    print(n, "rejected students")

def make_matrix(time_blocks, faculty_all, students_all):
    # Define parameters
    times = []
    for block in time_blocks:
        times += time_blocks[block]

    faculty = faculty_all.keys()
    students = students_all.keys()

    # Initialize model
    model = ConcreteModel()

    # binary variables representing the time and session of each fac
    model.grid = Var(((fac, stu, time) for fac in faculty for stu in students for time in times) ,
                           within=Binary, initialize=0)

    # Define an objective function with model as input, to pass later
    def obj_rule(m):
        matching = sum(m.grid[fac, stu, time] * students_all[stu]["match"][fac] \
                       for fac, stu, time in m.grid)

        fac_no = sum(m.grid[fac, stu, time] * \
                          (time in faculty_all[fac]["avail"]["no"] ) \
                          for fac, stu, time in m.grid)

        stu_no = sum(m.grid[fac, stu, time] * \
                          (time in students_all[stu]["avail"]["no"] ) \
                          for fac, stu, time in m.grid)

        fac_maybe = sum(m.grid[fac, stu, time] * \
                          (time in faculty_all[fac]["avail"]["maybe"] ) \
                          for fac, stu, time in m.grid)

        stu_maybe = sum(m.grid[fac, stu, time] * \
                          (time in students_all[stu]["avail"]["maybe"] ) \
                          for fac, stu, time in m.grid)

        stu_time_blocks = len(set( (stu, time_block(time))
                          for fac, stu, time in m.grid if m.grid[fac, stu, time]))

        fac_time_blocks = len(set( (fac, time_block(time))
                          for fac, stu, time in m.grid if m.grid[fac, stu, time]))

        return matching \
               + weights["unavailable"] * (fac_no + stu_no) \
               + weights["maybe available"] * (fac_maybe + stu_maybe) \
               + weights["block"] * (fac_time_blocks + stu_time_blocks)

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
    for fac, stu, time in matrix_original:
        matrix[stu].append((students[stu]["match"][fac], fac))

    print("Interviews by students with score")
    av = 0
    for stu in matrix:
        s = [score for score, _ in matrix[stu]]
        if any([score < interview_low_score for score in s]):
            intervs = [(sc, faculty[f]["name"]) for sc, f in matrix[stu]]
            print("Student ", students[stu]["name"], " has interviews ", intervs)
        av += sum(s)
    av /= interview_number * len(matrix)
    print("Average score per candidate: {}".format(av))
    print("Number of candidates: {}".format(len(matrix)))

    fac = {}
    for stu in matrix:
        for score, f in matrix[stu]:
            if f in fac:
                fac[f].append((score, students[stu]["name"]))
            else:
                fac[f] = [(score, students[stu]["name"])]

    print()
    dist = {}
    av = 0
    ints = 0
    print("Interviews by faculty with score:")
    for f in sorted(faculty, key=lambda f: len(fac.get(f,[])), reverse = True):
        if f not in fac:
            print(faculty[f]["name"], "has no interviews")
        else:
            av += sum([score for score, _ in fac[f]])/len(fac[f])
            ints += len(fac[f])
            if len(fac[f]) < 50 :
                print(faculty[f]["name"], "has interviews", fac[f])
    av /= len(fac)
    ints /= len(fac)
    print()
    print("Average score per faculty: {}".format(av))
    print("Average number of interviews per faculty: {}".format(ints))
    c = Counter([len(fac[f]) for f in fac])
    print("Distribution (number of ints, count): {}".format(sorted(list(c.items()))))
    print("Number of faculty with interviews: {}".format(len(fac)))

    print("\nChecking number of time blocks")

    times = { "times": defaultdict(list), "blocks": defaultdict(set) }
    for fac, stu, time in matrix_original:
        times["times"][stu].append(time)
        times["blocks"][stu].add(time_block(time))
        times["times"][fac].append(time)
        times["blocks"][fac].add(time_block(time))

    print("By faculty:")
    for f in sorted(faculty, key=lambda f: len(times["times"].get(f,[])), reverse = True):
        print(faculty[f]["name"], "has", len(times["times"].get(f,[])), "interviews in", len(times["blocks"].get(f,[])), "blocks")

    print("\nBy students:")
    for f in sorted(students, key=lambda f: len(times["times"].get(f,[])), reverse = True):
        print(students[f]["name"], "has", len(times["times"].get(f,[])), "interviews in", len(times["blocks"].get(f,[])), "blocks")

def export_matrix(db, matrix):
        """
        Rewrites the database matrix after manual confirm.
        Input: database connection object, matrix
        Returns: Nothing
        """

        with db.cursor() as cursor:
            cursor.execute("DELETE FROM matrix")
            query = "INSERT INTO matrix (user_id, faculty_id, time_id, current, inserted_at, updated_at) VALUES {};"
            values = []
            for fac, stu, t in matrix:
                values.append(f"(\"{stu}\", \"{fac}\", {t}, TRUE, NOW(), NOW())")
            cursor.execute(query.format(", ".join(values)))
        db.commit()


if __name__ == "__main__":
    login = toml.load("login.toml")
    selection_db = pymysql.connect(host = "localhost",
                             user = "root",
                             passwd = "",
                             db = "selection_2020b")

    matrix_db = pymysql.connect(host = "localhost",
                             user = "root",
                             passwd = "",
                             db = "apd_dev")

    # Faculty information
    faculty = m.get_faculty(selection_db)

    # Rearrange faculty by username
    faculty = {faculty[f]["username"]: faculty[f]  for f in faculty }
    del faculty["faculty_id"]


    # Fields information
    fields = m.get_fields(selection_db, m.fields_sql)
    # Student information
    students = m.get_students(selection_db, aws_students_sql, fields)
    # Time slots
    times = get_times(matrix_db)
    # Add availabilities
    (faculty, students) = add_availability_and_check(matrix_db, times, faculty, students)
    # Show comments
    show_comments(students)
    # Manually adding faculty of interest
    students['2390d3db-37dc-423e-85c8-74fae0de31ce']["faculty"].append("21") 
    students['2390d3db-37dc-423e-85c8-74fae0de31ce']["faculty"].append("14")
    students['630eda8f-52f7-40d2-99d7-c9efc72458c2']["faculty"].append("64")
    # Compute matching scores
    m.match(faculty, students, weights)
    # Special cases
    # forced = [("75183162", "42")] 
    # force_interviews(students, forced)
    # Avoiding pairing intern students
    print("\nRemember to remove past interns!")
    # interns :: [(student ID, faculty username)]
    interns = [ ("7828857c-b0aa-404d-9af5-69a9686d1fd7", "faculty_id")
              ]
    reject_interns(students, interns)
    # Add interviews that faculty requested
    requested_interviews(selection_db, faculty, students)
    # Interviews to avoid
    rejected_students(selection_db, faculty, students)
    # Closes connection
    selection_db.close()
    # Make matrix
    # matrix = make_matrix(times, faculty, students)
    # # Analyze stats
    # matrix_analysis(matrix, faculty, students)
    # # # # Export data
    # export_matrix(matrix_db, matrix)
    # #
    # # matrix_db.close()
    # #
    # #
