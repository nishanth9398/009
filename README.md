# match-maker
Repo for matching students and faculty though common fields of interest

Run `python3 matchmaker.py`.

At the moment, the main information is gathered from the database, but there are two `.csv` files located in `input`. They should eventually be replaced with database queries.
The files are:

- `faculty_fields.csv`: information about the [faculty fields of interest](https://docs.google.com/forms/d/1T562dWG4rm3ewEz0InF1mkWBdPFHa2pij4_h5bQKRWA/). Emails serve as key to match the DB values, some had to be edited by hand (`matchmaker.py` will warn about unfound emails). The fields are numerous and messy.

- `units.csv`: unit names, the fields are email of unit leader and unit name. The data set is terrible, units names may have changed and many are missing. Heavily edited by hand, `matchmaker.py` will warn about wrong emails, but not missing units.


The script updates the database automatically after manual confirmation.
