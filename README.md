# match-maker
Repo for matching students and faculty though common fields of interest

Run `python3 matchmaker.py`.

At the moment, it only runs locally with `.csv` files located in `input`. They should all be replaced with database queries.
The files are:

- `faculty.csv`: database info about faculty, the fields are `userid` (ID), `username` (full name), `email`, `usergroup` (panel number) from the database `selection_20xx`.

- `faculty_fields.csv`: information about the [faculty fields of interest](https://docs.google.com/forms/d/1T562dWG4rm3ewEz0InF1mkWBdPFHa2pij4_h5bQKRWA/). Emails serve as key to match the DB values, some had to be edited by hand (`matchmaker.py` will warn about unfound emails). The fields are numerous and messy.

- `units.csv`: unit names, the fields are email of unit leader and unit name. The data set is terrible, units names may have changed and many are missing. Heavily edited by hand, `matchmaker.py` will warn about wrong emails, but not missing units.

- `applicants.csv`: database information about applicants. The fields are user ID, fields of interest, faculty of interest (last name then first name, three times) and panel number.

The output will be written in `output/scores.csv`, the fields are student ID, faculty ID, and matching score.

