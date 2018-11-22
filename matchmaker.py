import csv
import editdistance

def clean_name(name):
    n = name.lower()
    n = n.replace("prof", "")
    n = n.replace("í", "i") # For Sile
    n = n.replace("eileen", "") # For Gail
    n =  "".join([c for c in n if c not in " ,-()_."])
    return n

def get_students(path):
    students = {}
    faculty_names = set()

    with open(path) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            name, fields, l1, f1, l2, f2, l3, f3, panel = row

            faculty = [[l1, f1], [l2, f2], [l3, f3]]

            students[name] = { "panel" : panel
                             , "faculty" : faculty
                             , "core" : []
                             , "minor" : fields.split('/')[:-1]
                             , "match" : []
                             }
    return students

def get_faculty(info_path, unit_path, fields_path):
    faculty = {}
    with open(info_path) as csvfile:
        reader = csv.reader(csvfile)
        for id, name, email, panel in reader:
            faculty[email] = { "name" : clean_name(name)
                              , "id" : id
                              , "panel" : panel
                              , "unit" : "x"*100
                              , "core" : []
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

            # if fl in real:
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
        # print(fac)

    print("{:%} of names are accounted for".format(accounted_for/3/len(students)))
    print("{} names are not found:\n{}".format(len(not_in), not_in))


def match(students, faculty):
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

def export(students, path):
    with open(path, "w") as f:
        f.write("student_id,faculty_id,score\n")
        for stu in students:
            for (score, fac) in students[stu]["match"]:
                f.write(stu + "," + fac + "," + str(score) + "\n")



faculty = get_faculty("input/faculty.csv", "input/units.csv", "input/faculty_fields.csv")
students = get_students("input/applicants.csv")
fix_names(faculty, students)
match(students, faculty)
export(students, "output/scores.csv")
