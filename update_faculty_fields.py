import csv
import pymysql
import toml

year = "2020b"

faculty_sql = """
SELECT userid, email
FROM logon
WHERE (class = 1 -- Faculty
OR userlevel = 1) -- Dean
AND email IS NOT NULL
"""

fields_sql = """
SELECT id, field from fields
"""

def clean_field_name(field):
    field = field[20:-1]
    field = "".join([c for c in field if c not in " -(),"])
    field = field[:30]
    return field



def get_faculty(db, fields_path):
    """
    Calls the database and files to get faculty information.
    Input: database connection object, paths to files with units names and fields
    Returns: dictionary of faculty information, key=faculty email
    """
    faculty = {}

    with db.cursor() as cursor:
        cursor.execute(faculty_sql)
        for id, email in cursor.fetchall():
            faculty[email.lower()] = { "id"    : str(id)
                              , "core"  : []
                              , "minor" : []
                              }

    with open(fields_path) as csvfile:
        reader = csv.reader(csvfile)

        header = reader.__next__()
        fields = list(map(clean_field_name, header[3:]))


        for row in reader:
            name, email = row[1:3]

            core_index = [ check == "Core (3 or 4)" for check in row[3:]]
            core = [ field for (check, field) in zip(core_index, fields) if check]

            minor_index = [ check == "Minor (as many as applicable)" for check in row[3:]]
            minor = [ field for (check, field) in zip(minor_index, fields) if check]

            if email.lower() in faculty:
                faculty[email.lower()]["core"] = core
                faculty[email.lower()]["minor"] = minor
            else:
                print(email.lower(), "not found")

    return (fields, faculty)

def get_fields(db):
    """
    Calls the database and files to get faculty information.
    Input: database connection object, paths to files with units names and fields
    Returns: dictionary of faculty information, key=faculty email
    """
    fields = {}

    with db.cursor() as cursor:
        cursor.execute(fields_sql)
        for id, field in cursor.fetchall():
            fields[field] = id

    return fields

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

def export_fields(db, all_fields, fields):
    """
    Updates the database table with the fields after manual confirm.
    Input: database connection object, faculty dictionnary
    Returns: Nothing
    """
    values = ", ".join([ f"(\"{f}\")" for f in all_fields if f not in fields])
    print("Fields to be updated: ", values)

    if not confirm():
        return


    if values:
        with db.cursor() as cursor:
            query = f"INSERT INTO fields (field) VALUES {values};"
            cursor.execute(query)
            db.commit()

def export_fac_fields(db, faculty, fields):
    """
    Updates the database table with the fields of interest after manual confirm.
    Input: database connection object, faculty dictionnary
    Returns: Nothing
    """
    to_be_updated = [faculty[f] for f in faculty if faculty[f]['core'] + faculty[f]['minor']]
    print("Faculty fields to be updated: ", to_be_updated)

    if not confirm():
        return

    with db.cursor() as cursor:
        for fac in faculty:
            fac_id = faculty[fac]["id"]

            core_id = [fields[f] for f in faculty[fac]["core"]]
            core_val = ["({}, {}, {})".format(fac_id, x, "\"core\"") for x in core_id]

            minor_id = [fields[f] for f in faculty[fac]["minor"]]
            minor_val = ["({}, {}, {})".format(fac_id, x, "\"minor\"") for x in minor_id]

            if core_val + minor_val:
                query = "INSERT INTO faculty_fields (faculty_id, fields_id, importance) VALUES {} ;"
                values = ", ".join(core_val + minor_val)
                cursor.execute(query.format(values))
        db.commit()


def connect(login):
    # Connect to database
    db = pymysql.connect(host = login["aad"]["host"],
                         user = login["aad"]["username"],
                         passwd = login["aad"]["password"],
                         db     = f"selection_{year}") # name of the database
    return db



if __name__ == "__main__":
    login = toml.load("login.toml")
    db = connect(login)
    # Faculty information
    all_fields, faculty = get_faculty(db, "input/faculty_fields.csv")
    fields = get_fields(db)
    # Export fields to database
    fields = export_fields(db, all_fields, fields)
    # Get all fields again
    fields = get_fields(db)
    # Export faculty-fields to database
    export_fac_fields(db, faculty, fields)
    # Closes connection
    db.close()
