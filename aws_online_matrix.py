import screening as m
import random
import pymysql
import csv
import toml
import datetime
from collections import Counter, defaultdict
from pyomo.environ import *
from pyomo.opt import SolverFactory

year = "2022"
workshop = "Feb"
batch = 1

interview_number = 4
max_faculty_interview = 10

interview_low_score = 10  # Prints interviews with scores that low

weights = {"core core": 10, "core minor": 5, "minor minor": 2, "interest": 100, "invite": 50  # Faculty wants to interview
           , "rejected": -10  # Faculty said no during screening
           , "force": 10000  # Forcing an interview (by hand only)
           , "intern": -100000  # Candidate was an intern (by hand)
           , "maybe available": -10  # Faculty maybe available
           , "block": -10  # Cost for extra time block
           , "consecutive": 10  # Trying to get consecutive interviews
           }

# SQL scripts for selection_2021

aws_students_sql = f"""
-- in selection_2021b
SELECT a.family_n, a.given_n,
       a.user_id,
       a.faculty1, a.faculty2, a.faculty3,
       e.comment -- comment from committee
FROM applicant a
LEFT JOIN eval_master e ON e.user_id=a.user_id
WHERE status = 1 -- Invited
"""

faculty_sql = """
SELECT userid, oistid, sdbid, username, email
FROM logon
WHERE ((class = 1 -- Faculty
OR userid = 52) -- Ulf, special because Dean
AND email IS NOT NULL)
-- OR userid = 801 -- Hibino-san
"""


wants_interview = """
-- in selection_2021
SELECT e.user_id, oistid
FROM eval_detail e
JOIN logon l on e.id_examiner = l.userid
WHERE interview = "yes";
"""

said_no = """
-- in selection_2021
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

faculty_avail_sql = """
SELECT faculty_id, time_block_id, available
FROM availabilities
WHERE applicant_id IS NULL
"""

student_avail_sql = """
SELECT applicant_id, time_block_id, available
FROM availabilities
WHERE faculty_id IS NULL
"""

times_sql = """
select id, time_block_id, times.from, times.to FROM times order by times.from
"""

faculty_fields_sql = """
SELECT faculty_id, field_id, major FROM faculty_fields
"""

student_fields_sql = """
SELECT user_id, field_id, major FROM applicant_fields
"""


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
    times = dict()
    blocks = defaultdict(list)
    with db.cursor() as cursor:
        cursor.execute(times_sql)
        for id, time_block_id, frm, to in cursor.fetchall():
            blocks[time_block_id].append(id)
            times[id] = { "time block" : time_block_id
                        , "from" : frm
                        , "to" : to
                        }
    # Sort IDs for each time_block to make sure
    blocks = {b: sorted(blocks[b]) for b in blocks}

    # Check that all times are ordered by id, if no issues, we assume that it is the case
    for t1, t2 in zip(times.keys(), list(times.keys())[1:]):
        if t2 != t1 + 1:
            print(f"WARNING: Times IDs are not continuous: ({t1} and {t2})")
        if times[t2]["from"] - times[t1]["from"] != datetime.timedelta(minutes=15):
            print(f"WARNING: Times are not evenly spaced by 15 minutes: ({t1} and {t2})")

    return (blocks, times)


def get_students(selection_db, aws_db, applicants_sql, student_fields_sql):
    """
    Calls the database and gets student information.
    Input: database connection object, SQL query, dict of fields name/ID
    Returns: dictionary of student information, key=student ID
    """
    students = {}

    with selection_db.cursor() as cursor:
        cursor.execute(applicants_sql)
        for last, first, id, f1, f2, f3, comment \
                in cursor.fetchall():
            faculty = [f1, f2, f3]
            cores = []
            minors = []
            students[id] = {"name": f"{last} {first}", "faculty": faculty, "core": cores, "minor": minors, "match": {}, "comment": comment
                            }

    with aws_db.cursor() as cursor:
        cursor.execute(student_fields_sql)
        for user_id, field_id, major in cursor.fetchall():
            if major == 1:
                students[user_id]["core"].append(field_id)
            else:
                students[user_id]["minor"].append(field_id)

    return students


def get_faculty(selection_db, aws_db, faculty_sql, faculty_fields_sql):
    """
    Calls the database and files to get faculty information.
    Input: database connection object, paths to files with units names and fields
    Returns: dictionary of faculty information, key=faculty ID
    """
    faculty = {}

    fix_username = {
        'MOHAMMAD-KHAN': 'mohammad-khan',
        'ULF-DIECKMANN2': 'ulf-dieckmann',
        'ARTUR-EKERT': 'artur-ekert',
        'KAE-NEMOTO': 'kae-nemoto'
    }

    with selection_db.cursor() as cursor:
        cursor.execute(faculty_sql)
        for id, username, sdbid, name, email in cursor.fetchall():
            if username in fix_username: username = fix_username[username]
            faculty[username] = \
                {"name": name.strip(), "logon ID": str(id), "username": username, "email": email.strip().lower(), "core": [], "minor": [], "match": []
                 }

    # Get faculty fields
    with aws_db.cursor() as cursor:
        cursor.execute(faculty_fields_sql)
        for user_id, field_id, major in cursor.fetchall():
            if major == 1:
                faculty[user_id]["core"].append(field_id)
            else:
                faculty[user_id]["minor"].append(field_id)

    return faculty


def worst_availability(available, available_next):
    if available == "yes":
        return available_next
    elif available == "no":
        return available
    else: # maybe
        if available_next == "no":
            return available_next
        else:
            return available
    

def add_availability_and_check(db, faculty, students, times, time_blocks):
    """
    Adds availability from faculty
    Also check IDs are consistent
    Removes students not in matrix (deferred or cancelled students)
    """
    # Filtering to students in matrix
    with db.cursor() as cursor:
        cursor.execute(matrix_students_sql)
        stu_avail = [s[0] for s in cursor.fetchall()]

    students = {s: students[s] for s in students if s in stu_avail}

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
    print("From matrix to selection:")
    for id in fac_matrix:
        name, email = fac_matrix[id]
        if id in faculty:
            if faculty[id]["email"] != email:
                name2 = faculty[id]["name"]
                email2 = faculty[id]["email"]
                print(
                    f"ID {id}: matrix faculty {name} {email} different from selection faculty {name2} {email2}")
        else:
            print(
                f"Faculty {name}, {email}, ID {id} in matrix not found in selection_{year}")

    # Check if IDs from selection are in matrix
    print("\nFrom selection to matrix:")
    for id in faculty:
        name2 = faculty[id]["name"]
        email2 = faculty[id]["email"]
        if id in fac_matrix:
            name, email = fac_matrix[id]
            if email2 != email:
                print(
                    f"ID {id}: selection faculty {name2} {email2} different from matrix faculty {name} {email}")
        else:
            print(
                f"Faculty {name2}, {email2}, ID {id} in selection_{year} not found in matrix")
    print("End of facultyID checks\n")

    # Adding availability data
    for f in faculty:
        faculty[f]["avail"] = defaultdict(list)

    for s in students:
        students[s]["avail"] = defaultdict(list)

    # Faculty availability
    with db.cursor() as cursor:
        cursor.execute(faculty_avail_sql)
        for id, time_block_id, available in cursor.fetchall():
            if id in faculty:
                for time in time_blocks[time_block_id]:
                    faculty[id]["avail"][available].append(time)
                
            else:
                print("Matrix Faculty not found in selection faculty", id)

    # Add default no for missing info
    for f in faculty:
        avail_times = faculty[f]["avail"]['yes'] + faculty[f]["avail"]['no'] + faculty[f]["avail"]['maybe']
        for time in times:
            if avail_times and time not in avail_times:
                faculty[f]["avail"]['no'].append(time)

    faculty = {f: faculty[f] for f in faculty 
                 if len(faculty[f]["avail"]['yes']+faculty[f]["avail"]['maybe']) > 0 and f in fac_matrix}

    # The availability of the last time slot in the each block must take into account the next availability since it overlaps
    last_times = [time_blocks[b][-1] for b in sorted(time_blocks.keys())[:-1]]
    for fac in faculty:
        for last_time in last_times:
            next_time = last_time + 1
            for available in ["yes", "maybe", "no"]:
                if last_time in faculty[fac]["avail"][available]:
                    available_last = available
                if next_time in faculty[fac]["avail"][available]:
                    available_next = available
            available_worst = worst_availability(available_last, available_next)
            faculty[fac]["avail"][available_last].remove(last_time)
            faculty[fac]["avail"][available_worst].append(last_time)
                
    # Student availability
    with db.cursor() as cursor:
        cursor.execute(student_avail_sql)
        for id, time_block_id, available in cursor.fetchall():
            if id in students:
                for time in time_blocks[time_block_id]:
                    students[id]["avail"][available].append(time)
            else:
                print("Matrix Student not found in selection applicants", id)

    # The availability of the last time slot in the each block must take into account the next availability since it overlaps
    for stu in students:
        for last_time in last_times:
            next_time = last_time + 1
            available_last, available_next = "", ""
            for available in ["yes", "maybe", "no"]:
                if last_time in students[stu]["avail"][available]:
                    available_last = available
                if next_time in students[stu]["avail"][available]:
                    available_next = available
            if available_last and available_next:
                available_worst = worst_availability(available_last, available_next)
                students[stu]["avail"][available_last].remove(last_time)
                students[stu]["avail"][available_worst].append(last_time)
            else: 
                print(f"Student {stu} has no availability")
                break

    # Only keep faculyt and students with some availability
    students = { s: students[s] for s in students 
               if len(students[s]["avail"]["yes"] + students[s]["avail"]["maybe"]) > 0 }

    faculty = { s: faculty[s] for s in faculty 
               if len(faculty[s]["avail"]["yes"] + faculty[s]["avail"]["maybe"]) > 0 }

    return (faculty, students)


def force_interviews(students, faculty, forced):
    """
    Manually forcing interviews
    Mutates students
    """
    for stu, fac in forced:
        if stu not in students:
            print("Forced interview student ", stu, "not found")
        elif fac not in faculty:
            print("Forced interview faculty ", fac, "not found")
        else:
            students[stu]["match"][fac] += weights["force"]



def reject_interns(students, faculty, interns):
    """
    Avoiding interviews with previous interns
    Mutates students
    """
    for stu, fac in interns:
        if stu not in students:
            print("Rejected interview student ", stu, "not found")
        elif fac not in faculty:
            print("Rejected interview faculty ", fac, "not found")
        else:
            students[stu]["match"][fac] += weights["intern"]


def requested_interviews(db, faculty, students):
    """
    This is to prioritize interviews that were requested by faculty
    Mutates students
    """
    n = 0
    requested = []
    with db.cursor() as cursor:
        cursor.execute(wants_interview)
        for stu, fac_id in cursor.fetchall():
            if stu in students:
                matches = students[stu]["match"]
                if fac_id in matches:
                    matches[fac_id] += weights["invite"]
                    n += 1
                    requested.append(
                        (students[stu]["name"], faculty[fac_id]["name"]))

    print("\n", n, "requested interviews: ")
    for stu, fac in sorted(requested):
        print(stu, "\t", fac)


def rejected_students(db, faculty, students):
    """
    This is to avoid interviews with students who were rejected by specific faculty
    Mutates students
    """
    n = 0
    rejected = []
    with db.cursor() as cursor:
        cursor.execute(said_no)
        for stu, fac_id in cursor.fetchall():
            if stu in students:
                matches = students[stu]["match"]
                if fac_id in matches:
                    matches[fac_id] += weights["rejected"]
                    n += 1
                    rejected.append(
                        (students[stu]["name"], faculty[fac_id]["name"]))

    print("\n", n, "rejected students: ")
    for stu, fac in sorted(rejected):
        print(stu, "\t", fac)

def times_overlap(times, time1, time2):
    return  abs(times[time1]["from"] - times[time2]["from"]) <= datetime.timedelta(minutes=30)

def consecutives(times):
    """ 
    Caclulates the number of consecutive interviews (with 15 minute break in between)
    """
    return sum(t + 3 in times for t in times)

def make_matrix(times_all, faculty_all, students_all):
    # Define parameters
    times = times_all.keys()
    faculty = faculty_all.keys()
    students = students_all.keys()

    # Initialize model
    model = ConcreteModel()

    # binary variables representing the time and session of each fac
    model.grid = Var(((fac, stu, time) for fac in faculty for stu in students for time in times),
                     within=Binary, initialize=0)


    # Define an objective function with model as input, to pass later
    def obj_rule(m):
        matching = sum(m.grid[fac, stu, time] * students_all[stu]["match"][fac]
                       for fac, stu, time in m.grid)

        fac_maybe = sum(m.grid[fac, stu, time] *
                        (time in faculty_all[fac]["avail"]["maybe"])
                        for fac, stu, time in m.grid)

        stu_maybe = sum(m.grid[fac, stu, time] *
                        (time in students_all[stu]["avail"]["maybe"])
                        for fac, stu, time in m.grid)

        stu_consecutive = sum(consecutives([time for fac, s, time in m.grid if s == stu and m.grid[fac, s, time]])
                              for stu in students)

        fac_consecutive = sum(consecutives([time for f, stu, time in m.grid if f == fac and m.grid[f, stu, time]]) 
                              for fac in faculty)

        # stu_time_blocks = len(set((stu, times_all[time]["time block"])
        #                           for fac, stu, time in m.grid if m.grid[fac, stu, time]))

        fac_time_blocks = len(set((fac, times_all[time]["time block"])
                                  for fac, stu, time in m.grid if m.grid[fac, stu, time]))

        return matching \
            + weights["maybe available"] * (fac_maybe + stu_maybe) \
            + weights["consecutive"] * (fac_consecutive + stu_consecutive) \
            + weights["block"] * fac_time_blocks #+ stu_time_blocks)

    # add objective function to the model. rule (pass function) or expr (pass expression directly)
    model.obj = Objective(rule=obj_rule, sense=maximize)

    model.constraints = ConstraintList()  # Create a set of constraints

    # Constraint: N interviews per student
    for stu in students:
        model.constraints.add(
            sum(model.grid[fac, stu, time] for fac in faculty for time in times) == interview_number
        )

    # Constraint: Maximum interviews per faculty
    for fac in faculty:
        model.constraints.add(
            sum(model.grid[fac, stu, time] for stu in students for time in times) <= max_faculty_interview
        )

    # Constraint: Max one interview per time per faculty
    for fac in faculty:
        for time in times:
            model.constraints.add(
                sum(model.grid[fac, stu, time] for stu in students) <= 1
            )

    # Constraint: Max one interview per time per student
    for stu in students:
        for time in times:
            model.constraints.add(
                sum(model.grid[fac, stu, time] for fac in faculty) <= 1
            )

    # Constraint: each student/faculty pair interviews maximum once
    for stu in students:
        for fac in faculty:
            model.constraints.add(
                sum(model.grid[fac, stu, time] for time in times) <= 1
            )

    # Constraint: No interview on unavailable time slots for faculty and students    
    model.constraints.add(
                sum(model.grid[fac, stu, time] for stu in students for fac in faculty for time in times  
                           if time in faculty_all[fac]["avail"]["no"] + students_all[stu]["avail"]["no"] ) == 0
            )

    # Constraint: No overlapping interviews
    for time in times:
        overlapping_times = [t for t in [time, time + 1, time + 2] if t in times]
        for fac in faculty:
            model.constraints.add(
                    sum(model.grid[fac, stu, time2] for stu in students for time2 in overlapping_times) <= 1
                )
        for stu in students:
            model.constraints.add(
                    sum(model.grid[fac, stu, time2] for fac in faculty for time2 in overlapping_times) <= 1
                )




    model.preprocess()
    # opt = SolverFactory('cbc', validate = False)  # Select solver
    # solver_manager = SolverManagerFactory('neos')  # Solve in neos server
    # results = solver_manager.solve(model, opt=opt)

    opt = SolverFactory('cbc')
    results = opt.solve(model)  # Solve locally

    print(results)

    matrix = []
    for fac, stu, time in model.grid:
        if model.grid[fac, stu, time].value:
            matrix.append((fac, stu, time))

    return matrix


def matrix_analysis(matrix_original, faculty, students, times):
    matrix = defaultdict(list)
    for fac, stu, time in matrix_original:
        matrix[stu].append((students[stu]["match"][fac], fac))

    print("Interviews by students with score")
    av = 0
    for stu in matrix:
        s = [score for score, _ in matrix[stu]]
        if any([score < interview_low_score for score in s]):
            intervs = [(sc, faculty[f]["name"]) for sc, f in matrix[stu]]
            print("Student ", students[stu]["name"],
                  " has interviews ", intervs)
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
    for f in sorted(faculty, key=lambda f: len(fac.get(f, [])), reverse=True):
        if f not in fac:
            print(faculty[f]["name"], "has no interviews")
        else:
            av += sum([score for score, _ in fac[f]])/len(fac[f])
            ints += len(fac[f])
            if len(fac[f]) < 50:
                print(faculty[f]["name"], "has interviews", fac[f])
    av /= len(fac)
    ints /= len(fac)
    print()
    print("Average score per faculty: {}".format(av))
    print("Average number of interviews per faculty: {}".format(ints))
    c = Counter([len(fac[f]) for f in fac])
    print("Distribution (number of ints, count): {}".format(
        sorted(list(c.items()))))
    print("Number of faculty with interviews: {}".format(len(fac)))

    print("\nChecking unavailable time blocks")
    unavail = 0
    for fac, stu, time in matrix_original:
        if (time in students[stu]["avail"]["no"]) or (time in faculty[fac]["avail"]["no"]):
            unavail +=1
    print(f"There are {unavail} time slots allocated to unavailable time blocks")


    print("\nChecking overlapping times")
    times_an = defaultdict(list)
    for fac, stu, time in matrix_original:
        times_an[stu].append(time)
        times_an[fac].append(time)
    overlap_count = 0
    for stu in students:
        t = times_an[stu]
        if len([ 0 for t1 in t for t2 in t if t1 < t2 and times_overlap(times, t1, t2)]) > 0:
          overlap_count += 1
    print(f"There are {overlap_count} counts of overlap for students")
    overlap_count = 0
    for fac in faculty:
        t = times_an[fac]
        if len([ 0 for t1 in t for t2 in t if t1 < t2 and times_overlap(times, t1, t2)]) > 0:
          overlap_count += 1
    print(f"There are {overlap_count} counts of overlap for faculty")

    print("\nChecking number of consecutive times")
    times_an = {"times": defaultdict(list)}
    for fac, stu, time in matrix_original:
        times_an["times"][stu].append(time)
        times_an["times"][fac].append(time)
    print("By faculty:")
    for f in faculty:
        consec = consecutives(times_an["times"][f])
        if consec:
            print(faculty[f]["name"], "has", consec," consecutive interviews")
    print("\nBy students:")
    for s in students:
        consec = consecutives(times_an["times"][s])
        if consec:
            print(students[s]["name"], "has", consec," consecutive interviews")

    print("\nChecking number of time blocks")
    times_an = {"times": defaultdict(list), "blocks": defaultdict(set)}
    for fac, stu, time in matrix_original:
        times_an["times"][stu].append(time)
        times_an["blocks"][stu].add(times[ time]["time block"])
        times_an["times"][fac].append(time)
        times_an["blocks"][fac].add(times[ time]["time block"])

    print("By faculty:")
    for f in sorted(faculty, key=lambda f: len(times_an["times"].get(f, [])), reverse=True):
        print(faculty[f]["name"], "has", len(times_an["times"].get(f, [])),
              "interviews in", len(times_an["blocks"].get(f, [])), "blocks")

    print("\nBy students:")
    for f in sorted(students, key=lambda f: len(times_an["times"].get(f, [])), reverse=True):
        print(students[f]["name"], "has", len(times_an["times"].get(
            f, [])), "interviews in", len(times_an["blocks"].get(f, [])), "blocks")


def export_matrix(db, matrix):
    """
    Rewrites the database matrix after manual confirm.
    Input: database connection object, matrix
    Returns: Nothing
    """

    with db.cursor() as cursor:
        chunks = [[]]
        for m in matrix:
            if len(chunks[-1]) >= 200:
                chunks.append([m])
            else:
                chunks[-1].append(m)

        cursor.execute("DELETE FROM matrix")
        for chunk in chunks:
            query = "INSERT INTO matrix (user_id, faculty_id, time_id, modified_by, current, inserted_at, updated_at) VALUES {};"
            values = []
            for fac, stu, t in chunk:
                values.append(
                    f"(\"{stu}\", \"{fac}\", {t}, \"jeremie-gillet\", TRUE, NOW(), NOW())")
            cursor.execute(query.format(", ".join(values)))
    db.commit()


if __name__ == "__main__":
    login = toml.load("login.toml")
    # selection_db = pymysql.connect(host=login["aad"]["host"],
    #                                user=login["aad"]["username"],
    #                                passwd=login["aad"]["password"],
    #                                db="selection_2022")
    selection_db = pymysql.connect(host="localhost",
                                user="root",
                                passwd="",
                                db="selection_2022")

    matrix_db = pymysql.connect(host="localhost",
                                user="root",
                                passwd="",
                                db="aws_feb22")

    # Faculty information
    faculty = get_faculty(selection_db, matrix_db, faculty_sql, faculty_fields_sql)

    # Student information
    students = get_students(selection_db, matrix_db, aws_students_sql, student_fields_sql)
    m.faculty_of_interest(faculty, students)

    # Time slots
    time_blocks, times = get_times(matrix_db)
    # Add availabilities
    (faculty, students) = add_availability_and_check(matrix_db, faculty, students, times, time_blocks)
    # Show comments
    show_comments(students)
    # Manually modifying faculty of interest
    students['43369']["faculty"] = ['bkuhn', 'keiko-kono', 'plaurino']
    # Compute matching scores
    m.match(faculty, students, weights)
    # Special cases
    # forced = []
    forced = [("43369", "AKIMITSU-NARITA") ] # Arnott, Caoimhín
    force_interviews(students, faculty, forced)
    # Avoiding pairing intern students or students with a OIST letter of recommendation
    print("\nRemember to remove past interns!")
    # interns :: [(student ID, faculty username)]
    interns = [ ("39611", "greg-stephens"), # Kargin Timofei
                ("77163", "pinaki"), # Sato Fuga  
                ("39803", "VINCENT-LAUDET"), # Pilieva, Polina
                ("69751", "norisky"), # Okabe Nanako
                ("84508", "SAMUEL-REITER"), # Fernandez, Olivier
                ("83536", "TOM-FROESE"), # Cadena Alvear, Itzel
                ("83536", "tripp"), # Cadena Alvear, Itzel
              ]
    reject_interns(students, faculty, interns)
    # Add interviews that faculty requested
    requested_interviews(selection_db, faculty, students)
    # Interviews to avoid
    rejected_students(selection_db, faculty, students)
    # Closes connection
    selection_db.close()

    # # Limit numbers for testing
    # students = {s:students[s] for i, s in enumerate(students) if i <= 10}
    # faculty = {s:faculty[s] for i, s in enumerate(faculty) if i <= 10}

    # Make matrix
    matrix = make_matrix(times, faculty, students)
    # Export data
    export_matrix(matrix_db, matrix)
    # Analyze stats
    matrix_analysis(matrix, faculty, students, times)
    # Closes connection
    matrix_db.close()
