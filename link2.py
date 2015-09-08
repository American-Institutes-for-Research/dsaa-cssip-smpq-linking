import csv, datetime, os, re, subprocess, sys
import xlrd, MySQLdb

from collections import defaultdict
from contextlib import closing

PQ_INSTITUTION_IDS = {183: "Purdue", 129: "UMN", 96: "UIowa", 267: "UWisconsin"}
PQ_MIN_YEAR = 2000

# SM_SOURCE_IDS = {2: "Purdue", 5: "UMN", 4: "UIowa", 6: "UWisconsin"}
SM_SOURCE_IDS = {2: "Purdue", 
                 5: "UMN", 
                 4: "UIowa", 
                 6: "UWisconsin",
                 7: "UChicago",
                 8: "UMich",
                 9: "UIndiana",
                 10: "OSU",
                 11: "PSU",
                 12: "Caltech"}

GENDER_FILE = r"U:\Gender Paper\Inputs\ssnnamesdata.out"
GENDER_MALE_CUTOFF = 0.2
GENDER_FEMALE_CUTOFF = 0.8

LIFE_SCIENCE_FILE =r'U:\Gender Paper\Inputs\Life Sciences_Paula.xlsx'

LIBDIR = r"U:\Gender Paper\Library"
CLASSPATH = r".;U:\Gender Paper\Library\torch-1.0-SNAPSHOT.jar;" + LIBDIR


def dbconnect():
    return MySQLdb.connect(user="jtokle", 
                           passwd="Faa22dIuoW$2KmRgC40%", 
                           db="collaboration", 
                           host="wsumsftp01")


def with_db(f):
    def result(*args, **kwargs):
        with closing(dbconnect()) as db:
            return f(db, *args, **kwargs)

    return result


@with_db
def life_science_init(db):
    """Load the list of "life science" codes into the database."""

    cur = db.cursor()
    cur.execute("drop table if exists collaboration.smpq_life_science")
    cur.execute("""create table collaboration.smpq_life_science (
                   subject_code int unsigned not null,
                   subject_name varchar(200) not null,
                   index subject_code_ix (subject_code),
                   index subject_name_ix (subject_name))""")

    book = xlrd.open_workbook(LIFE_SCIENCE_FILE)
    sheet = book.sheet_by_name("Life Sciences")
    for i in range(1, sheet.nrows):
        cur.execute("""insert into collaboration.smpq_life_science
                       (subject_code, subject_name) values (%s, %s)""",
                    (int(sheet.cell_value(i, 0)), sheet.cell_value(i, 1)))

    db.commit()


def gender_probabilities_parse_row(row):
    """Parse one row of the gender-name file. Returns the upcased name, the 2008
    sliding model probability, and the predicted gender as a tuple."""

    name, data = row.split("\t")[:2]
    pr_fem = float(data.split("|")[-1])

    if pr_fem <= GENDER_MALE_CUTOFF:
        gender = "M"
    elif pr_fem >= GENDER_FEMALE_CUTOFF:
        gender = "F"
    else:
        gender = "N"

    return name.strip().upper(), pr_fem, gender


@with_db
def gender_probabilities_init(db):
    """Load names and probabilities from the gender modeling paper. Use the 2008 sliding
    model probability."""

    cur = db.cursor()

    cur.execute("drop table if exists collaboration.smpq_gender_probabilities")
    cur.execute("""create table collaboration.smpq_gender_probabilities (
                   firstname varchar(40) not null,
                   pr_fem float not null,
                   gender char(1) not null,
                   index firstname_ix (firstname))""")

    with open(GENDER_FILE) as f:
        f.readline() # skip header row
        for row in f:
            name, pr_fem, gender = gender_probabilities_parse_row(row)
            cur.execute("""insert into collaboration.smpq_gender_probabilities
                           (firstname, pr_fem, gender) values (%s, %s, %s)""",
                           [name, pr_fem, gender])

    db.commit()


@with_db
def pq_create_database(db):
    cur = db.cursor()

    cur.execute("drop table if exists collaboration.smpq_proquest")
    cur.execute("""create table collaboration.smpq_proquest (
                   _id int unsigned primary key,
                   publication_number varchar(20),
                   degree_year int not null,
                   author varchar(60) not null,
                   lastname varchar(40) not null,
                   firstname varchar(40) null,
                   gender char(1) null,
                   pr_fem float null,
                   school_code int not null,
                   institution_id int not null,
                   university varchar(45) null,
                   title text null,
                   translated_title text null,
                   advisors varchar(100) null,
                   advisor_firstname varchar(40) null,
                   advisor_gender char(1) null,
                   advisor_pr_fem float null,
                   subjects varchar(300) null,
                   subject_codes varchar(100) null,
                   subject_1 varchar(200) null,
                   subject_code_1 int null,
                   corporate_name varchar(250),
                   degree varchar(20),
                   language varchar(30),
                   life_science_code tinyint not null default 0,
                   abstract text null,
                   unique index publication_number_ix (publication_number))""")

    db.commit()


@with_db
def pq_insert_records(db, institution_ids=PQ_INSTITUTION_IDS):
    sql = """insert into collaboration.smpq_proquest
             (_id, publication_number, degree_year, author, lastname, firstname, school_code,
              institution_id, title, translated_title, advisors, advisor_firstname,
              subjects, subject_codes, subject_1, subject_code_1, corporate_name, degree, language)
             select d._id, d.publication_number, d.degree_year, d.author, d.author_lastname, d.author_firstname,
                    i.pq_institution_id, i._id, d.title, d.translated_title, d.advisors, d.advisor_firstname,
                    d.subjects, vds.subject_codes, s.name, s.pq_subject_id,
                    d.corporate_name, d.degree, d.language
             from proquest.dissertation d
             join proquest.institution i on i._id = d.institution_id
             left join proquest.v_dissertation_subject vds on vds.dissertation_id = d._id
             left join proquest.dissertation_subject ds on ds.dissertation_id = d._id and ds.position = 1
             left join proquest.subject s on s._id = ds.subject_id
             where d.institution_id in ({})
             and d.degree_year >= {}
             and d.degree like 'Ph.D.%'"""

    ids = ", ".join([str(n) for n in institution_ids])

    with closing(dbconnect()) as db:
        cur = db.cursor()
        cur.execute(sql.format(ids, PQ_MIN_YEAR))

        for k, v in institution_ids.items():
            cur.execute("""update collaboration.smpq_proquest
                           set university = %s
                           where institution_id = %s""",
                           (v, k))

        db.commit()


@with_db
def pq_life_science_prediction(db, institution_ids=None):
    """Update the life-science columns in the proquest table."""

    sql = """update collaboration.smpq_proquest pq
             join proquest.dissertation_subject ds on ds.dissertation_id = pq._id
             join proquest.subject s on s._id = ds.subject_id
             join collaboration.smpq_life_science ls on ls.subject_code = s.pq_subject_id
             set pq.life_science_code = 1"""

    if institution_ids:
        ids = ", ".join(str(n) for n in institution_ids)
        sql += " where pq.institution_id in ({})".format(ids)

    cur = db.cursor()
    cur.execute(sql)

    db.commit()


@with_db
def pq_gender_prediction(db, institution_ids=None):
    """Update gender prediction columns in the proquest table."""
    
    if institution_ids:
        ids = ", ".join(str(n) for n in institution_ids)
        where_clause = " where pq.institution_id in ({})".format(ids)
    else:
        where_clause = ""

    cur = db.cursor()
    cur.execute("""update collaboration.smpq_proquest pq
                   join collaboration.smpq_gender_probabilities prob using (firstname)
                   set pq.pr_fem = prob.pr_fem, pq.gender = prob.gender""" + where_clause)

    cur.execute("""update collaboration.smpq_proquest pq
                   join collaboration.smpq_gender_probabilities prob on pq.advisor_firstname = prob.firstname
                   set pq.advisor_pr_fem = prob.pr_fem, pq.advisor_gender = prob.gender""" + where_clause)

    cur.execute("""update collaboration.smpq_proquest pq
                   set pq.gender = 'U'
                   where pq.gender is null""")

    cur.execute("""update collaboration.smpq_proquest pq
                   set pq.advisor_gender = 'U'
                   where pq.advisor_gender is null""")

    db.commit()


@with_db
def pq_init(db, institution_ids=PQ_INSTITUTION_IDS):
    """Extract a set of records from the ProQuest data corresponding to the given school code."""

    pq_create_database()
    pq_insert_records(institution_ids)
    pq_life_science_prediction()
    pq_gender_prediction()


@with_db
def pq_name_fix(db, institution_ids=None):
    """Update the last-name field in proquest, if this works well then the fix should be applied
    to the proquest database itself.
    
    NOTE: The current best matches are obtained by taking only first alphabetic word from
    the last name in each file"""

    cur = db.cursor()
    cur2 = db.cursor()

    sql = "select publication_number, author from collaboration.smpq_proquest pq"
    if institution_ids:
        ids = ", ".join(str(n) for n in institution_ids)
        sql += " where pq.institution_id in ({})".format(ids)

    cur.execute(sql)

    sql = "update collaboration.smpq_proquest set lastname = %s where publication_number = %s"
    expr = re.compile("^[^.,]+")

    for row in cur:
        publication_number, author = row
        m = expr.search(author)
        if m:
            lastname = m.group().upper()
            cur2.execute(sql, (lastname, publication_number))

    db.commit()


def sm_source_id_list():
    return ", ".join("{}".format(v) for v in SM_SOURCE_IDS.keys())


@with_db
def sm_names_init(db):
    """Create a table to store starmetrics employees."""

    cur = db.cursor()
    cur.execute("drop table if exists collaboration.smpq_sm_names")
    cur.execute("""create table collaboration.smpq_sm_names (
                   __employee_id int unsigned not null,
                   university varchar(45) null,
                   last_name varchar(50) null,
                   first_name varchar(50) null,
                   max_grad_year int null,
                   min_period_start_date date not null,
                   max_period_end_date date not null,
                   days_worked int unsigned not null,
                   work_6_months_over_2_years tinyint not null default 0,
                   work_12_months_over_2_years tinyint not null default 0,
                   gender char(1) null,
                   pr_fem float null,
                   life_science_code tinyint null,
                   nsf tinyint not null default 0,
                   nih tinyint not null default 0,
                   usda tinyint not null default 0,
                   first_appear_bucketed_occup varchar(100) null,
                   first_appear_orig_occup varchar(100) null,
                   first_appear_date date null,
                   days_worked_under_first_occup int not null default 0,
                   first_appear_as_grad_date date null,
                   first_appear_as_grad_orig_occup varchar(100) null,
                   last_appear_as_grad_date date,
                   last_appear_as_grad_orig_occup varchar(100),
                   days_worked_as_grad int not null default 0,
                   last_appear_bucketed_occup varchar(100) null,
                   last_appear_orig_occup varchar(100) null,
                   days_worked_under_last_occup int not null default 0,
                   work_6_months_on_nih tinyint not null default 0,
                   work_6_months_on_nsf tinyint not null default 0,
                   work_6_months_on_usda tinyint not null default 0,
                   primary key (__employee_id))""")


def is_nsf(agency_code):
    return int(agency_code) == 47


def is_usda(agency_code):
    return int(agency_code) == 10


class Employee(object):

    def __init__(self, row):
        self.__employee_id = row['__employee_id']
        self.university = row['university']
        self.last_name = row['last_name']
        self.first_name = row['first_name']

        self.min_period_start_date = row['period_start_date']
        self.max_period_end_date = row['period_end_date']

        if row['occupational_classification'].lower().startswith('graduate'):
            self.min_grad_period_start_date = row['period_start_date']
            self.max_grad_period_end_date = row['period_end_date']
            self.first_grad_orig_occup = row['x_occupational_classification']
            self.last_grad_orig_occup = row['x_occupational_classification']
        else:
            self.min_grad_period_start_date = datetime.date(3000, 1, 1)
            self.max_grad_period_end_date = datetime.date(1900, 1, 1)
            self.first_grad_orig_occup = None
            self.last_grad_orig_occup = None

        self.first_appear_bucketed_occup = row['occupational_classification']
        self.first_appear_orig_occup = row['x_occupational_classification']
        self.last_appear_bucketed_occup = row['occupational_classification']
        self.last_appear_orig_occup = row['x_occupational_classification']

        self.days_worked_by_occup = defaultdict(int)
        dt = (row['period_end_date'] - row['period_start_date']).days + 1
        self.days_worked_by_occup[row['occupational_classification']] = dt
        self.total_days_worked = dt

        self.total_days_worked_on_nih = 0
        self.total_days_worked_on_nsf = 0
        self.total_days_worked_on_usda = 0

        if row["is_nih"] == 1:
            self.total_days_worked_on_nih += dt

        if is_nsf(row["agency_code"]):
            self.total_days_worked_on_nsf += dt

        if is_usda(row["agency_code"]):
            self.total_days_worked_on_usda += dt


    def addtransaction(self, row):
        if row['period_start_date'] < self.min_period_start_date:
            self.min_period_start_date = row['period_start_date'] 
            self.first_appear_bucketed_occup = row['occupational_classification']
            self.first_appear_orig_occup = row['x_occupational_classification']

        if row['period_end_date'] > self.max_period_end_date:
            self.max_period_end_date = row['period_end_date']
            self.last_appear_bucketed_occup = row['occupational_classification']
            self.last_appear_orig_occup = row['x_occupational_classification']

        if row['occupational_classification'].lower().startswith('graduate'):
            if row['period_start_date'] < self.min_grad_period_start_date:
                self.min_grad_period_start_date = row['period_start_date'] 
                self.first_grad_orig_occup = row['x_occupational_classification']

            if row['period_end_date'] > self.max_grad_period_end_date:
                self.max_grad_period_end_date = row['period_end_date']
                self.last_grad_orig_occup = row['x_occupational_classification']

        dt = (row['period_end_date'] - row['period_start_date']).days
        self.days_worked_by_occup[row['occupational_classification']] += dt
        self.total_days_worked += dt

        if row["is_nih"] == 1:
            self.total_days_worked_on_nih += dt

        if is_nsf(row["agency_code"]):
            self.total_days_worked_on_nsf += dt

        if is_usda(row["agency_code"]):
            self.total_days_worked_on_usda += dt


    def days_spanned(self):
        return (self.max_period_end_date - self.min_period_start_date).days


    def work_6_months_over_2_years(self):
        return self.days_spanned() > 2 * 365 and self.total_days_worked > 180


    def work_12_months_over_2_years(self):
        return self.days_spanned() > 2 * 365 and self.total_days_worked > 360


    def days_worked_as_grad(self):
        return sum(v for k, v in self.days_worked_by_occup.items() if k.lower().startswith("graduate"))


    def work_6_months_on_nih(self):
        return self.total_days_worked_on_nih > 180


    def work_6_months_on_nsf(self):
        return self.total_days_worked_on_nsf > 180


    def work_6_months_on_usda(self):
        return self.total_days_worked_on_usda > 180


    def todict(self):
        days_worked_as_grad = self.days_worked_as_grad()

        # if days_worked_as_grad > 0:
        if self.first_grad_orig_occup:
            min_grad_period_start_date = self.min_grad_period_start_date
            max_grad_period_end_date = self.max_grad_period_end_date
            max_grad_year = max_grad_period_end_date.year
        else:
            min_grad_period_start_date = None
            max_grad_period_end_date = None
            max_grad_year = None

        return {"__employee_id": self.__employee_id,
                "university": self.university,
                "last_name": self.last_name,
                "first_name": self.first_name,
                "max_grad_year": max_grad_year,
                "min_period_start_date": self.min_period_start_date,
                "max_period_end_date": self.max_period_end_date,
                "days_worked": self.total_days_worked,
                "work_6_months_over_2_years": self.work_6_months_over_2_years(),
                "work_12_months_over_2_years": self.work_12_months_over_2_years(),
                "work_lt_6_months_or_lt_2_years": not (self.work_6_months_over_2_years() or self.work_12_months_over_2_years()),
                "first_appear_bucketed_occup": self.first_appear_bucketed_occup,
                "first_appear_orig_occup": self.first_appear_orig_occup,
                "first_appear_date": self.min_period_start_date,
                "days_worked_under_first_occup": self.days_worked_by_occup[self.first_appear_bucketed_occup],
                "first_appear_as_grad_date": min_grad_period_start_date,
                "first_appear_as_grad_orig_occup": self.first_grad_orig_occup,
                "last_appear_as_grad_date": max_grad_period_end_date,
                "last_appear_as_grad_orig_occup": self.last_grad_orig_occup,
                "days_worked_as_grad": days_worked_as_grad,
                "last_appear_bucketed_occup": self.last_appear_bucketed_occup,
                "last_appear_orig_occup": self.last_appear_orig_occup,
                "days_worked_under_last_occup": self.days_worked_by_occup[self.last_appear_bucketed_occup],
                "work_6_months_on_nih": self.work_6_months_on_nih(),
                "work_6_months_on_nsf": self.work_6_months_on_nsf(),
                "work_6_months_on_usda": self.work_6_months_on_usda()}


@with_db
def sm_get_employees(db):
    employees = {}

    cur = db.cursor(MySQLdb.cursors.SSDictCursor)
    cur.execute("""select e.__employee_id, s.name university, upper(e.last_name) last_name,
                   upper(e.first_name) first_name, et.period_start_date, et.period_end_date,
                   datediff(et.period_end_date, et.period_start_date) + 1 days_worked,
                   o.occupational_classification, et.x_occupational_classification,
                   ap.agency_code, ap.program_code, ap.is_nih
                   from starmetricsnew.employee_transaction et
                   join starmetricsnew.employee e using (__employee_id)
                   join starmetricsnew.source s on s.__source_id = et.__source_id
                   join starmetricsnew.occupation o using (__occupation_id)
                   join starmetricsnew.award a using (__award_id)
                   join starmetricsnew.agency_program ap using (__agency_program_id)
                   where et.__source_id in ({})""".format(sm_source_id_list()))

    row = cur.fetchone()
    while row:
        id = row["__employee_id"]
        if id in employees:
            employees[id].addtransaction(row)
        else:
            employees[id] = Employee(row)

        row = cur.fetchone()

    return employees


@with_db
def sm_insert_employees(db, employees):
    insert_columns = ["__employee_id", "university", "last_name", "first_name", "max_grad_year",
                      "min_period_start_date", "max_period_end_date", "days_worked",
                      "work_6_months_over_2_years", "work_12_months_over_2_years",
                      "first_appear_bucketed_occup", "first_appear_orig_occup",
                      "first_appear_date", "days_worked_under_first_occup",
                      "first_appear_as_grad_date", "first_appear_as_grad_orig_occup",
                      "last_appear_as_grad_date", "last_appear_as_grad_orig_occup",
                      "days_worked_as_grad", "last_appear_bucketed_occup",
                      "last_appear_orig_occup", "days_worked_under_last_occup",
                      "work_6_months_on_nih", "work_6_months_on_nsf", "work_6_months_on_usda"]

    sql = "insert into collaboration.smpq_sm_names ("
    sql += ", ".join(insert_columns)
    sql += ") values ("
    sql += ", ".join("%s" for _ in insert_columns)
    sql += ")"

    cur = db.cursor()

    for e in employees.values():
        d = e.todict()
        values = [d[k] for k in insert_columns]
        cur.execute(sql, values)

    db.commit()


@with_db
def sm_name_fix(db):
    """Extract the first alphabetic word from the last name."""

    cur = db.cursor()
    cur2 = db.cursor()

    cur.execute("select __employee_id, last_name from collaboration.smpq_proquest")

    sql = "update collaboration.smpq_proquest set lastname = %s where publication_number = %s"
    expr = re.compile("^[^.,]+")

    for row in cur:
        publication_number, author = row
        m = expr.search(author)
        if m:
            lastname = m.group().upper()
            cur2.execute(sql, (lastname, publication_number))

    db.commit()



@with_db
def sm_names_set_flags(db):

    cur = db.cursor()
    cur.execute("""update collaboration.smpq_sm_names sm
                   join collaboration.smpq_gender_probabilities prob on sm.first_name = prob.firstname
                   set sm.pr_fem = prob.pr_fem, sm.gender = prob.gender""")

    cur.execute("""update collaboration.smpq_sm_names
                   set gender = 'U'
                   where gender is null""")

    db.commit()


@with_db
def sm_awards_init(db):
    cur= db.cursor()
    cur.execute("drop table if exists collaboration.smpq_sm_awards")
    cur.execute("drop table if exists collaboration.smpq_sm_awards_by_year")

    cur.execute("""create table collaboration.smpq_sm_awards (
                   __award_id int unsigned not null,
                   unique_award_number varchar(60) not null,
                   award_id varchar(50) null,
                   umetricsgrants varchar(45) null,
                   xwalk_id int unsigned null,
                   cfda varchar(10) not null,
                   agency_code int not null,
                   nih tinyint not null default 0,
                   nsf tinyint not null default 0,
                   usda tinyint not null default 0,
                   university varchar(45) not null,
                   first_transaction_year int unsigned not null,
                   first_period_start_date date not null,
                   last_period_end_date date not null,
                   team_size int not null,
                   primary key (__award_id),
                   index (unique_award_number),
                   index (umetricsgrants),
                   index (award_id))""")

    cur.execute("""create table collaboration.smpq_sm_awards_by_year (
                   __award_id int unsigned not null,
                   unique_award_number varchar(60) not null,
                   year int not null,
                   cfda varchar(10) not null,
                   agency_code int not null,
                   nih tinyint not null default 0,
                   nsf tinyint not null default 0,
                   usda tinyint not null default 0,
                   team_size int null,
                   primary key (__award_id, year))""")

    cur.execute("""insert into collaboration.smpq_sm_awards
                   (__award_id, unique_award_number, cfda, agency_code, nih, university, first_transaction_year, 
                   first_period_start_date, last_period_end_date, team_size)
                   select a.__award_id, a.unique_award_number, ap.cfda, ap.agency_code, ap.is_nih,
                   group_concat(distinct s.name order by s.name separator '|'),
                   min(year(et.period_start_date)), min(et.period_start_date), max(et.period_end_date),
                   count(distinct e.__employee_id)
                   from starmetricsnew.employee_transaction et
                   join starmetricsnew.employee e using (__employee_id)
                   join starmetricsnew.award a using (__award_id)
                   join starmetricsnew.agency_program ap using (__agency_program_id)
                   join starmetricsnew.source s on s.__source_id = et.__source_id
                   where et.__source_id in ({})
                   group by 1, 2, 3, 4, 5""".format(sm_source_id_list()))

    cur.execute("""insert ignore into collaboration.smpq_sm_awards_by_year
                   (__award_id, unique_award_number, cfda, agency_code, nih, year, team_size)
                   select a.__award_id, a.unique_award_number, ap.cfda, ap.agency_code, ap.is_nih, 
                          year(et.period_start_date), count(distinct e.__employee_id)
                   from starmetricsnew.employee_transaction et
                   join starmetricsnew.employee e using (__employee_id)
                   join starmetricsnew.award a using (__award_id)
                   join starmetricsnew.agency_program ap using (__agency_program_id)
                   where et.__source_id in ({})
                   group by 1, 2, 3, 4, 5, 6""".format(sm_source_id_list()))

    cur.execute("""update collaboration.smpq_sm_awards
                   set nsf = 1
                   where agency_code = 47""")

    cur.execute("""update collaboration.smpq_sm_awards_by_year
                   set nsf = 1
                   where agency_code = 47""")

    cur.execute("""update collaboration.smpq_sm_awards
                   set usda = 1
                   where agency_code = 10""")

    cur.execute("""update collaboration.smpq_sm_awards_by_year
                   set usda = 1
                   where agency_code = 10""")

    db.commit()


@with_db
def sm_names_awards_init(db):
    """For all names in the collaboration.smpq_sm_names table, create links to 
       all star metrics awards for which they have transactions."""

    cur = db.cursor()
    cur.execute("drop table if exists collaboration.smpq_sm_names_awards")

    cur.execute("""create table collaboration.smpq_sm_names_awards (
                   __employee_id int not null,
                   university varchar(45) not null,
                   __award_id int not null,
                   unique_award_number varchar(60) not null,
                   year int not null,
                   primary key (__employee_id, __award_id, year))""")

    cur.execute("""insert into collaboration.smpq_sm_names_awards
                   select distinct et.__employee_id, names.university, 
                          aby.__award_id, aby.unique_award_number, aby.year
                   from collaboration.smpq_sm_names names
                   join starmetricsnew.employee_transaction et using (__employee_id)
                   join collaboration.smpq_sm_awards_by_year aby
                   on aby.__award_id = et.__award_id and aby.year = year(et.period_start_date)""")

    db.commit()

    cur.execute("""update collaboration.smpq_sm_names names
                   join (select __employee_id, max(nih) nih, max(nsf) nsf, max(usda) usda
                         from collaboration.smpq_sm_names_awards awards
                         join collaboration.smpq_sm_awards using (__award_id)
                         group by 1) q using (__employee_id)
                   set names.nih = q.nih, names.nsf = q.nsf, names.usda = q.usda""")

    db.commit()


@with_db
def sm_team_size_init(db):
    """By employee (__employee_id) and year, compute the average team size worked on."""

    cur = db.cursor()

    cur.execute("drop table if exists collaboration.smpq_sm_team_size")

    cur.execute("""create table collaboration.smpq_sm_team_size (
                   __employee_id int not null,
                   year int not null,
                   avg_team_size float not null,
                   nih tinyint not null,
                   nsf tinyint not null,
                   primary key (__employee_id, year))""")

    cur.execute("""insert into collaboration.smpq_sm_team_size
                   select names.__employee_id, aby.year, avg(aby.team_size),
                          max(aby.nih), max(aby.nsf)
                   from collaboration.smpq_sm_names names
                   join collaboration.smpq_sm_names_awards na using (__employee_id)
                   join collaboration.smpq_sm_awards_by_year aby using (__award_id, year)
                   group by 1, 2""")

    db.commit()


@with_db
def sm_awards_xwalk_1x1(db):
    cur = db.cursor()

    cur.execute("""create temporary table collaboration.award_id_count
                   (primary key (award_id))
                   select award_id, count(*) count
                   from starmetrics.crosswalk
                   group by 1
                   having count(*) = 1""")

    cur.execute("""create temporary table collaboration.uniqueawardnumber_count
                   (primary key (uniqueawardnumber))
                   select uniqueawardnumber, count(*) count
                   from starmetrics.crosswalk
                   group by 1
                   having count(*) = 1""")

    cur.execute("""update collaboration.smpq_sm_awards awards
                   join starmetrics.crosswalk x on x.uniqueawardnumber = awards.unique_award_number
                   join collaboration.award_id_count a on a.award_id = x.award_id
                   join collaboration.uniqueawardnumber_count b on b.uniqueawardnumber = x.uniqueawardnumber
                   set awards.award_id = x.award_id, 
                       awards.umetricsgrants = x.umetricsgrants,
                       awards.xwalk_id = 1""")

    db.commit()


@with_db
def sm_awards_xwalk_agency(db, agency_table, agency_award_id, year_field, xwalk_id):
    cur = db.cursor()

    sql = """create temporary table collaboration.award_id_count
             (primary key (award_id, start_year))
             select x.award_id, year(a.{year_field}) start_year, count(distinct x.uniqueawardnumber) count
             from starmetrics.crosswalk x
             join umetricsgrants.{agency_table} a on a.{agency_award_id} = x.award_id
             where x.umetricsgrants = '{agency_table}'
             and a.{year_field} is not null
             group by 1, 2
             having count(distinct x.uniqueawardnumber) = 1"""

    cur.execute(sql.format(agency_table=agency_table, 
                           year_field=year_field, 
                           agency_award_id=agency_award_id))
    
    sql = """update collaboration.smpq_sm_awards awards
             join starmetrics.crosswalk x on x.uniqueawardnumber = awards.unique_award_number
             join collaboration.award_id_count a on a.award_id = x.award_id and a.start_year = awards.first_transaction_year
             set awards.award_id = x.award_id, 
                 awards.umetricsgrants = x.umetricsgrants,
                 awards.xwalk_id = {xwalk_id}
             where x.umetricsgrants = '{agency_table}'
             and awards.award_id is null"""

    cur.execute(sql.format(agency_table=agency_table, xwalk_id=xwalk_id))

    db.commit()


def sm_awards_xwalk():
    sm_awards_xwalk_1x1()
    sm_awards_xwalk_agency("nih_project", "FULL_PROJECT_NUM", "BUDGET_START", 2)
    sm_awards_xwalk_agency("nsf_award", "AwardId", "AwardEffectiveDate", 3)
    sm_awards_xwalk_agency("usda_grant", "grant_num", "start_date", 4)
    sm_awards_xwalk_agency("rg_award", "FederalAwardIDNumber", "AwardStartDate", 5)


@with_db
def sm_agency_init(db):
    cur = db.cursor()
    cur.execute("drop table if exists collaboration.smpq_nih")
    cur.execute("""create table collaboration.smpq_nih
                   (primary key (FULL_PROJECT_NUM))
                   select nih.*
                   from umetricsgrants.nih_project nih
                   join collaboration.smpq_sm_awards awards on awards.umetricsgrants = 'nih_project' and awards.award_id = nih.FULL_PROJECT_NUM
                   where nih.TOTAL_COST <> 0""")

    cur.execute("drop table if exists collaboration.smpq_nsf")
    cur.execute("""create table collaboration.smpq_nsf
                   (primary key (AwardId))
                   select nsf.*
                   from umetricsgrants.nsf_award nsf
                   join collaboration.smpq_sm_awards awards on awards.umetricsgrants = 'nsf_award' and awards.award_id = nsf.AwardId""")

    cur.execute("drop table if exists collaboration.smpq_usda")
    cur.execute("""create table collaboration.smpq_usda
                   (primary key (grant_num))
                   ignore select usda.*
                   from umetricsgrants.usda_grant usda
                   join collaboration.smpq_sm_awards awards on awards.umetricsgrants = 'usda_grant' and awards.award_id = usda.grant_num""")

    cur.execute("drop table if exists collaboration.smpq_rg")
    cur.execute("""create table collaboration.smpq_rg
                   (primary key (FederalAwardIDNumber))
                   select rg.*
                   from umetricsgrants.rg_award rg
                   join collaboration.smpq_sm_awards awards on awards.umetricsgrants = 'rg_award' and awards.award_id = rg.FederalAwardIDNumber""")

    db.commit()


def sm_init():
    sm_names_init()

    employees = sm_get_employees()
    sm_insert_employees(employees)
    sm_names_set_flags()

    sm_awards_init()
    sm_names_awards_init()
    sm_team_size_init()
    sm_awards_xwalk()
    sm_agency_init()


@with_db
def create_matching_input_files(db, directory=".", pq_institutions=None, sm_universities=None):
    """Create files to input into the matching program. Creates one CSV file from all records in
    collaboration.smpq_names with occupationalclassification 'Graduate' and another
    from all records in smpq_proquest."""

    cur = db.cursor()
    
    sql = """select left(last_name, 1), university, __employee_id, 
             last_name, first_name, max_grad_year
             from collaboration.smpq_sm_names
             where max_grad_year is not null"""

    if sm_universities:
        unis = ", ".join("'{}'".format(x) for x in sm_universities)
        sql += " and university in ({})".format(unis)

    cur.execute(sql)

    with open(os.path.join(directory, "smnames_grad.csv"), "w") as f:
        wr = csv.writer(f, lineterminator="\n")
        for row in cur:
            wr.writerow(row)

    sql = """select left(last_name, 1), university, __employee_id,
             last_name, first_name, ifnull(max_grad_year, year(max_period_end_date))
             from collaboration.smpq_sm_names"""

    if sm_universities:
        unis = ", ".join("'{}'".format(x) for x in sm_universities)
        sql += " where university in ({})".format(unis)

    cur.execute(sql)

    with open(os.path.join(directory, "smnames_all.csv"), "w") as f:
        wr = csv.writer(f, lineterminator="\n")
        for row in cur:
            wr.writerow(row)

    sql = """select left(lastname, 1), university, publication_number, lastname, firstname, degree_year
             from collaboration.smpq_proquest"""

    if pq_institutions:
        ids = ", ".join(str(n) for n in pq_institutions)
        sql += " where institution_id in ({})".format(ids)

    cur.execute(sql)

    with open(os.path.join(directory, "proquest.csv"), "w") as f:
        wr = csv.writer(f, lineterminator="\n")
        for row in cur:
            wr.writerow(row)


def initial_matching(directory="."):
    subprocess.call(["java", "-cp", CLASSPATH, "Match", directory])


def extract_1x1_links(directory="."):
    subprocess.call(["java", "-cp", CLASSPATH, "Assign", directory])


@with_db
def insert_1x1_links(db, directory=".", drop_tables=False):
    cur = db.cursor()

    if drop_tables:
        cur.execute("""drop table if exists collaboration.smpq_names_proquest""")
        cur.execute("""drop table if exists collaboration.smpq_names_proquest_link_type""")

        cur.execute("""create table collaboration.smpq_names_proquest_link_type (
                       __link_type_id int unsigned primary key auto_increment,
                       description varchar(100) not null)""")

        cur.execute("""insert into collaboration.smpq_names_proquest_link_type (description)
                       values 
                       ('StarMetrics to ProQuest (SM graduate students)'), 
                       ('ProQuest to StarMetrics (all SM employees)')""")

        cur.execute("""create table collaboration.smpq_names_proquest (
                       score double not null,
                       __employee_id int not null,
                       publication_number varchar(20) not null,
                       __link_type_id int unsigned not null,
                       primary key (__employee_id, publication_number, __link_type_id),
                       foreign key (__link_type_id) references collaboration.smpq_names_proquest_link_type (__link_type_id))""")

    with open(os.path.join(directory, "sm_pq_links_1x1.csv")) as f:
        f.readline()
        rr = csv.reader(f)

        for row in rr:
            row.append(1)
            cur.execute("""insert into collaboration.smpq_names_proquest
                           (score, __employee_id, publication_number, __link_type_id)
                           values (%s, %s, %s, %s)""", row)

    with open(os.path.join(directory, "pq_sm_links_1x1.csv")) as f:
        f.readline()
        rr = csv.reader(f)

        for row in rr:
            row.append(2)
            cur.execute("""insert into collaboration.smpq_names_proquest
                           (score, __employee_id, publication_number, __link_type_id)
                           values (%s, %s, %s, %s)""", row)

    db.commit()


