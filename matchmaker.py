import csv

def get_factulty(path):
    faculty = {}
    with open(path) as csvfile:
        reader = csv.reader(csvfile)

        header = reader.__next__()
        fields = [ field[20:-1] for field in header[10:]]

        for row in reader:
            email, name = row[1:3]

            core_index = [ check == "Core (3 or 4)" for check in row[10:]]
            core = [ field for (check, field) in zip(core_index, fields) if check]

            minor_index = [ check == "Minor (as many as applicable)" for check in row[10:]]
            core = [ field for (check, field) in zip(minor_index, fields) if check]

            faculty[email] = { "name" : name
                             , "core" : core
                             , "core_index" : core_index
                             , "minor" : core
                             , "minor_index" : core_index
                             , "match" : []
                             }

    return faculty

def match(students, faculty):
    co_co_weight = 5
    co_mi_weight = 3
    mi_mi_weight = 1

    for stu in students:
        for fac in faculty:
            co_co = zip(students[stu]["core_index"], faculty[fac]["core_index"])
            co_mi = zip(students[stu]["core_index"], faculty[fac]["minor_index"])
            mi_co = zip(students[stu]["minor_index"], faculty[fac]["core_index"])
            mi_mi = zip(students[stu]["minor_index"], faculty[fac]["minor_index"])

            score = co_co_weight * sum([ i*j for (i, j) in co_co ]) \
                    + co_mi_weight * sum([ i*j for (i, j) in co_mi ]) \
                    + co_mi_weight * sum([ i*j for (i, j) in mi_co ]) \
                    + mi_mi_weight * sum([ i*j for (i, j) in mi_mi ])

            if score > 0:
                students[stu]["match"].append((score, fac))
                faculty[fac]["match"].append((score, stu))

        students[stu]["match"].sort(key = lambda x : -x[0])
    for fac in faculty:
        faculty[fac]["match"].sort(key = lambda x : -x[0])




faculty = get_factulty("faculty.csv")
students = dict(faculty)
match(students, faculty)
