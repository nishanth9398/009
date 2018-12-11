import matchmaker as m

applicants_sql = """
-- in selection_2019
SELECT a.user_id,
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

def show_comments(students):
    for stu in students:
        comment = students[stu]["comment"]
        if comment:
            faculty = students[stu]["faculty"]
            print("Student {} {}: {}".format(stu, faculty, comment))

def make_matrix(faculty, students):
    matrix = {}

    return matrix

if __name__ == "__main__":
    db = m.connect()
    # Faculty information
    faculty = m.get_faculty(db, "input/units.csv", "input/faculty_fields.csv")
    # Student information
    students = m.get_students(db, applicants_sql)
    # Closes connection
    db.close()
    # Data cleanup
    m.fix_names(faculty, students)
    # Compute matching scores
    m.match(faculty, students)
    # Show comments
    show_comments(students)
    # hand fixes from comments
    students["75206480"]["faculty"].append("thomasbourguignon")
    students["75206480"]["faculty"].append("alexandermikheyev")
    students["75207047"]["faculty"].append("deniskonstantinov")
    matrix = make_matrix(faculty, students)
    print(matrix)
