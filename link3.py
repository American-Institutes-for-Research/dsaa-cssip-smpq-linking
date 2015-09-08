import argparse, csv, datetime, os, re, subprocess, sys
import ConfigParser, xlrd, MySQLdb

from collections import defaultdict
from contextlib import closing

def read_configuration(f):
    """Read the configuration file f and return a dict."""

    conf = ConfigParser.ConfigParser()
    conf.read(f)

    result = {}
    result["sm_source_ids"] = {}

    for k, v in conf.items("UMETRICS Universities"):
        result["sm_source_ids"][int(v)] = k

    result["pq_min_year"] = int(conf.get("ProQuest", "MinYear"))

    result["pq_institution_ids"] = {}

    for k, v in conf.items("ProQuest Universities"):
        result["pq_institution_ids"][int(v)] = k

    result["gender_file"] = conf.get("Gender Coding", "file")
    result["gender_male_cutoff"] = float(conf.get("Gender Coding", "male_cutoff"))
    result["gender_female_cutoff"] = float(conf.get("Gender Coding", "female_cutoff"))

    result["life_science_file"] = conf.get("File System", "life_science_file")
    result["libdir"] = conf.get("File System", "libdir")
    result["classpath"] = ";".join([conf.get("File System", "classpath"), result["libdir"]])

    result["db_user"] = conf.get("Database", "user")
    result["db_passwd"] = conf.get("Database", "passwd")
    result["db_db"] = conf.get("Database", "db")
    result["db_host"] = conf.get("Database", "host")

    return result


def life_science_init(db, table_prefix="smpq"):
    """Load the list of "life science" codes into the database."""

    cur = db.cursor()
    cur.execute("drop table if exists air.{}_life_science".format(table_prefix))
    cur.execute("""create table air.{}_life_science (
                   subject_code int unsigned not null,
                   subject_name varchar(200) not null,
                   index subject_code_ix (subject_code),
                   index subject_name_ix (subject_name))""".format(table_prefix))

    book = xlrd.open_workbook(LIFE_SCIENCE_FILE)
    sheet = book.sheet_by_name("Life Sciences")
    for i in range(1, sheet.nrows):
        cur.execute("""insert into air.{}_life_science
                       (subject_code, subject_name) values (%s, %s)""".format(table_prefix),
                    (int(sheet.cell_value(i, 0)), sheet.cell_value(i, 1)))

    db.commit()


def gender_probabilities_parse_row(row, male_cutoff, female_cutoff):
    """Parse one row of the gender-name file. Returns the upcased name, the 2008
    sliding model probability, and the predicted gender as a tuple."""

    name, data = row.split("\t")[:2]
    pr_fem = float(data.split("|")[-1])

    if pr_fem <= male_cutoff:
        gender = "M"
    elif pr_fem >= female_cutoff:
        gender = "F"
    else:
        gender = "N"

    return name.strip().upper(), pr_fem, gender


def gender_probabilities_init(db, gender_file, male_cutoff, female_cutoff, table_prefix="smpq"):
    """Load names and probabilities from the gender modeling paper. Use the 2008 sliding
    model probability."""

    cur = db.cursor()

    cur.execute("drop table if exists air.{}_gender_probabilities".format(table_prefix))
    cur.execute("""create table air.{}_gender_probabilities (
                   firstname varchar(40) not null,
                   pr_fem float not null,
                   gender char(1) not null,
                   index firstname_ix (firstname))""".format(table_prefix))

    with open(gender_file) as f:
        f.readline() # skip header row
        for row in f:
            name, pr_fem, gender = gender_probabilities_parse_row(row, male_cutoff, female_cutoff)
            cur.execute("""insert into air.{}_gender_probabilities
                           (firstname, pr_fem, gender) values (%s, %s, %s)""".format(table_prefix),
                           [name, pr_fem, gender])

    db.commit()


def pq_create_database(db, table_prefix="smpq"):
    cur = db.cursor()

    cur.execute("drop table if exists air.{}_proquest".format(table_prefix))
    cur.execute("""create table air.{}_proquest (
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
                   unique index publication_number_ix (publication_number))""".format(table_prefix))

    db.commit()


def pq_insert_records(db, institution_ids, min_year, table_prefix="smpq"):
    ids = ", ".join([str(n) for n in institution_ids])

    sql = """insert into air.{}_proquest
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
             and d.degree like 'Ph.D.%'""".format(table_prefix, ids, min_year)


    cur = db.cursor()
    cur.execute(sql)

    for k, v in institution_ids.items():
        cur.execute("""update air.{}_proquest
                       set university = %s
                       where institution_id = %s""".format(table_prefix),
                       (v, k))

    db.commit()


def pq_life_science_prediction(db, institution_ids=None, table_prefix="smpq"):
    """Update the life-science columns in the proquest table."""

    sql = """update air.{table_prefix}_proquest pq
             join proquest.dissertation_subject ds on ds.dissertation_id = pq._id
             join proquest.subject s on s._id = ds.subject_id
             join air.{table_prefix}_life_science ls on ls.subject_code = s.pq_subject_id
             set pq.life_science_code = 1""".format(table_prefix=table_prefix)

    if institution_ids:
        ids = ", ".join(str(n) for n in institution_ids)
        sql += " where pq.institution_id in ({})".format(ids)

    cur = db.cursor()
    cur.execute(sql)

    db.commit()


def pq_gender_prediction(db, institution_ids=None, table_prefix="smpq"):
    """Update gender prediction columns in the proquest table."""
    
    if institution_ids:
        ids = ", ".join(str(n) for n in institution_ids)
        where_clause = " where pq.institution_id in ({})".format(ids)
    else:
        where_clause = ""

    cur = db.cursor()
    cur.execute("""update air.{table_prefix}_proquest pq
                   join air.{table_prefix}_gender_probabilities prob using (firstname)
                   set pq.pr_fem = prob.pr_fem, pq.gender = prob.gender""".format(table_prefix=table_prefix) + where_clause)

    cur.execute("""update air.{table_prefix}_proquest pq
                   join air.{table_prefix}_gender_probabilities prob on pq.advisor_firstname = prob.firstname
                   set pq.advisor_pr_fem = prob.pr_fem, pq.advisor_gender = prob.gender""".format(table_prefix=table_prefix) + where_clause)

    cur.execute("""update air.{}_proquest pq
                   set pq.gender = 'U'
                   where pq.gender is null""".format(table_prefix))

    cur.execute("""update air.{}_proquest pq
                   set pq.advisor_gender = 'U'
                   where pq.advisor_gender is null""".format(table_prefix))

    db.commit()


def pq_init(db, institution_ids, table_prefix="smpq"):
    """Extract a set of records from the ProQuest data corresponding to the given school code."""

    pq_create_database(db, table_prefix)
    pq_insert_records(db, institution_ids, table_prefix)
    pq_life_science_prediction(db, institution_ids, table_prefix)
    pq_gender_prediction(db, institution_ids, table_prefix)


def pq_name_fix(db, institution_ids=None, table_prefix="smpq"):
    """Update the last-name field in proquest, if this works well then the fix should be applied
    to the proquest database itself.
    
    NOTE: The current best matches are obtained by taking only first alphabetic word from
    the last name in each file"""

    cur = db.cursor()
    cur2 = db.cursor()

    sql = "select publication_number, author from air.{}_proquest pq".format(table_prefix)
    if institution_ids:
        ids = ", ".join(str(n) for n in institution_ids)
        sql += " where pq.institution_id in ({})".format(ids)

    cur.execute(sql)

    sql = "update air.{}_proquest set lastname = %s where publication_number = %s".format(table_prefix)
    expr = re.compile("^[^.,]+")

    for row in cur:
        publication_number, author = row
        m = expr.search(author)
        if m:
            lastname = m.group().upper()
            cur2.execute(sql, (lastname, publication_number))

    db.commit()


def sm_source_id_list(sm_source_ids):
    return ", ".join("{}".format(v) for v in sm_source_ids.keys())


def sm_names_init(db, table_prefix="smpq"):
    """Create a table to store starmetrics employees."""

    cur = db.cursor()
    cur.execute("drop table if exists air.{}_sm_names".format(table_prefix))
    cur.execute("""create table air.{}_sm_names (
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
                   primary key (__employee_id))""".format(table_prefix))


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


def sm_get_employees(db, sm_source_ids):
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
                   where et.__source_id in ({})""".format(sm_source_id_list(sm_source_ids)))

    row = cur.fetchone()
    while row:
        id = row["__employee_id"]
        if id in employees:
            employees[id].addtransaction(row)
        else:
            employees[id] = Employee(row)

        row = cur.fetchone()

    return employees


def sm_insert_employees(db, employees, table_prefix="smpq"):
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

    sql = "insert into air.{}_sm_names (".format(table_prefix)
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


def sm_name_fix(db, table_prefix="smpq"):
    """Extract the first alphabetic word from the last name."""

    cur = db.cursor()
    cur2 = db.cursor()

    cur.execute("select __employee_id, last_name from air.{}_proquest".format(table_prefix))

    sql = "update air.{}_proquest set lastname = %s where publication_number = %s".format(table_prefix)
    expr = re.compile("^[^.,]+")

    for row in cur:
        publication_number, author = row
        m = expr.search(author)
        if m:
            lastname = m.group().upper()
            cur2.execute(sql, (lastname, publication_number))

    db.commit()



def sm_names_set_flags(db, table_prefix="smpq"):

    cur = db.cursor()
    cur.execute("""update air.{}_sm_names sm
                   join air.{}_gender_probabilities prob on sm.first_name = prob.firstname
                   set sm.pr_fem = prob.pr_fem, sm.gender = prob.gender""".format(table_prefix, table_prefix))

    cur.execute("""update air.{}_sm_names
                   set gender = 'U'
                   where gender is null""".format(table_prefix))

    db.commit()


def sm_awards_init(db, sm_source_ids, table_prefix="smpq"):
    cur= db.cursor()
    cur.execute("drop table if exists air.{}_sm_awards".format(table_prefix))
    cur.execute("drop table if exists air.{}_sm_awards_by_year".format(table_prefix))

    cur.execute("""create table air.{}_sm_awards (
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
                   index (award_id))""".format(table_prefix))

    cur.execute("""create table air.{}_sm_awards_by_year (
                   __award_id int unsigned not null,
                   unique_award_number varchar(60) not null,
                   year int not null,
                   cfda varchar(10) not null,
                   agency_code int not null,
                   nih tinyint not null default 0,
                   nsf tinyint not null default 0,
                   usda tinyint not null default 0,
                   team_size int null,
                   primary key (__award_id, year))""".format(table_prefix))

    cur.execute("""insert into air.{}_sm_awards
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
                   group by 1, 2, 3, 4, 5""".format(table_prefix, sm_source_id_list(sm_source_ids)))

    cur.execute("""insert ignore into air.{}_sm_awards_by_year
                   (__award_id, unique_award_number, cfda, agency_code, nih, year, team_size)
                   select a.__award_id, a.unique_award_number, ap.cfda, ap.agency_code, ap.is_nih, 
                          year(et.period_start_date), count(distinct e.__employee_id)
                   from starmetricsnew.employee_transaction et
                   join starmetricsnew.employee e using (__employee_id)
                   join starmetricsnew.award a using (__award_id)
                   join starmetricsnew.agency_program ap using (__agency_program_id)
                   where et.__source_id in ({})
                   group by 1, 2, 3, 4, 5, 6""".format(table_prefix, sm_source_id_list(sm_source_ids)))

    cur.execute("""update air.{}_sm_awards
                   set nsf = 1
                   where agency_code = 47""".format(table_prefix))

    cur.execute("""update air.{}_sm_awards_by_year
                   set nsf = 1
                   where agency_code = 47""".format(table_prefix))

    cur.execute("""update air.{}_sm_awards
                   set usda = 1
                   where agency_code = 10""".format(table_prefix))

    cur.execute("""update air.{}_sm_awards_by_year
                   set usda = 1
                   where agency_code = 10""".format(table_prefix))

    db.commit()


def sm_names_awards_init(db, table_prefix="smpq"):
    """For all names in the air.{table_prefix}_sm_names table, create links to 
       all star metrics awards for which they have transactions."""

    cur = db.cursor()
    cur.execute("drop table if exists air.{}_sm_names_awards".format(table_prefix))

    cur.execute("""create table air.{}_sm_names_awards (
                   __employee_id int not null,
                   university varchar(45) not null,
                   __award_id int not null,
                   unique_award_number varchar(60) not null,
                   year int not null,
                   primary key (__employee_id, __award_id, year))""".format(table_prefix))

    cur.execute("""insert into air.{table_prefix}_sm_names_awards
                   select distinct et.__employee_id, names.university, 
                          aby.__award_id, aby.unique_award_number, aby.year
                   from air.{table_prefix}_sm_names names
                   join starmetricsnew.employee_transaction et using (__employee_id)
                   join air.{table_prefix}_sm_awards_by_year aby
                   on aby.__award_id = et.__award_id and aby.year = year(et.period_start_date)""".format(table_prefix=table_prefix))

    db.commit()

    cur.execute("""update air.{table_prefix}_sm_names names
                   join (select __employee_id, max(nih) nih, max(nsf) nsf, max(usda) usda
                         from air.{table_prefix}_sm_names_awards awards
                         join air.{table_prefix}_sm_awards using (__award_id)
                         group by 1) q using (__employee_id)
                   set names.nih = q.nih, names.nsf = q.nsf, names.usda = q.usda""".format(table_prefix=table_prefix))

    db.commit()


def sm_team_size_init(db, table_prefix="smpq"):
    """By employee (__employee_id) and year, compute the average team size worked on."""

    cur = db.cursor()

    cur.execute("drop table if exists air.{}_sm_team_size".format(table_prefix))

    cur.execute("""create table air.{}_sm_team_size (
                   __employee_id int not null,
                   year int not null,
                   avg_team_size float not null,
                   nih tinyint not null,
                   nsf tinyint not null,
                   primary key (__employee_id, year))""".format(table_prefix))

    cur.execute("""insert into air.{table_prefix}_sm_team_size
                   select names.__employee_id, aby.year, avg(aby.team_size),
                          max(aby.nih), max(aby.nsf)
                   from air.{table_prefix}_sm_names names
                   join air.{table_prefix}_sm_names_awards na using (__employee_id)
                   join air.{table_prefix}_sm_awards_by_year aby using (__award_id, year)
                   group by 1, 2""".format(table_prefix=table_prefix))

    db.commit()


def sm_awards_xwalk_1x1(db, table_prefix="smpq"):
    cur = db.cursor()

    cur.execute("""create temporary table air.award_id_count
                   (primary key (award_id))
                   select award_id, count(*) count
                   from starmetrics.crosswalk
                   group by 1
                   having count(*) = 1""")

    cur.execute("""create temporary table air.uniqueawardnumber_count
                   (primary key (uniqueawardnumber))
                   select uniqueawardnumber, count(*) count
                   from starmetrics.crosswalk
                   group by 1
                   having count(*) = 1""")

    cur.execute("""update air.{}_sm_awards awards
                   join starmetrics.crosswalk x on x.uniqueawardnumber = awards.unique_award_number
                   join air.award_id_count a on a.award_id = x.award_id
                   join air.uniqueawardnumber_count b on b.uniqueawardnumber = x.uniqueawardnumber
                   set awards.award_id = x.award_id, 
                       awards.umetricsgrants = x.umetricsgrants,
                       awards.xwalk_id = 1""".format(table_prefix))

    db.commit()

    cur.execute("drop temporary table air.award_id_count")
    cur.execute("drop temporary table air.uniqueawardnumber_count")

    db.commit()


def sm_awards_xwalk_agency(db, agency_table, agency_award_id, year_field, xwalk_id, table_prefix="smpq"):
    cur = db.cursor()

    sql = """create temporary table air.award_id_count
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
    
    sql = """update air.{table_prefix}_sm_awards awards
             join starmetrics.crosswalk x on x.uniqueawardnumber = awards.unique_award_number
             join air.award_id_count a on a.award_id = x.award_id and a.start_year = awards.first_transaction_year
             set awards.award_id = x.award_id, 
                 awards.umetricsgrants = x.umetricsgrants,
                 awards.xwalk_id = {xwalk_id}
             where x.umetricsgrants = '{agency_table}'
             and awards.award_id is null"""

    cur.execute(sql.format(table_prefix=table_prefix, agency_table=agency_table, xwalk_id=xwalk_id))

    db.commit()

    cur.execute("drop temporary table air.award_id_count")
    db.commit()


def sm_awards_xwalk(db, table_prefix="smpq"):
    sm_awards_xwalk_1x1(db, table_prefix)
    sm_awards_xwalk_agency(db, "nih_project", "FULL_PROJECT_NUM", "BUDGET_START", 2, table_prefix)
    sm_awards_xwalk_agency(db, "nsf_award", "AwardId", "AwardEffectiveDate", 3, table_prefix)
    sm_awards_xwalk_agency(db, "usda_grant", "grant_num", "start_date", 4, table_prefix)
    sm_awards_xwalk_agency(db, "rg_award", "FederalAwardIDNumber", "AwardStartDate", 5, table_prefix)


def sm_agency_init(db, table_prefix="smpq"):
    cur = db.cursor()
    cur.execute("drop table if exists air.{}_nih".format(table_prefix))
    cur.execute("""create table air.{table_prefix}_nih
                   (primary key (FULL_PROJECT_NUM))
                   select nih.*
                   from umetricsgrants.nih_project nih
                   join air.{table_prefix}_sm_awards awards on awards.umetricsgrants = 'nih_project' and awards.award_id = nih.FULL_PROJECT_NUM
                   where nih.TOTAL_COST <> 0""".format(table_prefix=table_prefix))

    cur.execute("drop table if exists air.{}_nsf".format(table_prefix))
    cur.execute("""create table air.{table_prefix}_nsf
                   (primary key (AwardId))
                   select nsf.*
                   from umetricsgrants.nsf_award nsf
                   join air.{table_prefix}_sm_awards awards on awards.umetricsgrants = 'nsf_award' and awards.award_id = nsf.AwardId""".format(table_prefix=table_prefix))

    cur.execute("drop table if exists air.{}_usda".format(table_prefix))
    cur.execute("""create table air.{table_prefix}_usda
                   (primary key (grant_num))
                   ignore select usda.*
                   from umetricsgrants.usda_grant usda
                   join air.{table_prefix}_sm_awards awards on awards.umetricsgrants = 'usda_grant' and awards.award_id = usda.grant_num""".format(table_prefix=table_prefix))

    cur.execute("drop table if exists air.{}_rg".format(table_prefix))
    cur.execute("""create table air.{table_prefix}_rg
                   (primary key (FederalAwardIDNumber))
                   select rg.*
                   from umetricsgrants.rg_award rg
                   join air.{table_prefix}_sm_awards awards on awards.umetricsgrants = 'rg_award' and awards.award_id = rg.FederalAwardIDNumber""".format(table_prefix=table_prefix))

    db.commit()


def sm_init(db, sm_source_ids, table_prefix="smpq"):
    sm_names_init(db, table_prefix)

    employees = sm_get_employees(db, sm_source_ids)
    sm_insert_employees(db, employees, table_prefix)
    sm_names_set_flags(db)

    sm_awards_init(db, sm_source_ids, table_prefix)
    sm_names_awards_init(db, table_prefix)
    sm_team_size_init(db, table_prefix)
    sm_awards_xwalk(db, table_prefix)
    sm_agency_init(db, table_prefix)


def create_matching_input_files(db, directory=".", pq_institutions=None, sm_universities=None, table_prefix="smpq"):
    """Create files to input into the matching program. Creates one CSV file from all records in
    air.{table_prefix}_names with occupationalclassification 'Graduate' and another
    from all records in {table_prefix}_proquest."""

    cur = db.cursor()
    
    sql = """select left(last_name, 1), university, __employee_id, 
             last_name, first_name, max_grad_year
             from air.{}_sm_names
             where max_grad_year is not null""".format(table_prefix)

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
             from air.{}_sm_names""".format(table_prefix)

    if sm_universities:
        unis = ", ".join("'{}'".format(x) for x in sm_universities)
        sql += " where university in ({})".format(unis)

    cur.execute(sql)

    with open(os.path.join(directory, "smnames_all.csv"), "w") as f:
        wr = csv.writer(f, lineterminator="\n")
        for row in cur:
            wr.writerow(row)

    sql = """select left(lastname, 1), university, publication_number, lastname, firstname, degree_year
             from air.{}_proquest""".format(table_prefix)

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


def insert_1x1_links(db, directory=".", drop_tables=False, table_prefix="smpq"):
    cur = db.cursor()

    if drop_tables:
        cur.execute("""drop table if exists air.{}_names_proquest""".format(table_prefix))
        cur.execute("""drop table if exists air.{}_names_proquest_link_type""".format(table_prefix))

        cur.execute("""create table air.{}_names_proquest_link_type (
                       __link_type_id int unsigned primary key auto_increment,
                       description varchar(100) not null)""".format(table_prefix))

        cur.execute("""insert into air.{}_names_proquest_link_type (description)
                       values 
                       ('StarMetrics to ProQuest (SM graduate students)'), 
                       ('ProQuest to StarMetrics (all SM employees)')""".format(table_prefix))

        cur.execute("""create table air.{table_prefix}_names_proquest (
                       score double not null,
                       __employee_id int not null,
                       publication_number varchar(20) not null,
                       __link_type_id int unsigned not null,
                       primary key (__employee_id, publication_number, __link_type_id),
                       foreign key (__link_type_id) references air.{table_prefix}_names_proquest_link_type (__link_type_id))""".format(table_prefix=table_prefix))

    with open(os.path.join(directory, "sm_pq_links_1x1.csv")) as f:
        f.readline()
        rr = csv.reader(f)

        for row in rr:
            row.append(1)
            cur.execute("""insert into air.{}_names_proquest
                           (score, __employee_id, publication_number, __link_type_id)
                           values (%s, %s, %s, %s)""".format(table_prefix), row)

    with open(os.path.join(directory, "pq_sm_links_1x1.csv")) as f:
        f.readline()
        rr = csv.reader(f)

        for row in rr:
            row.append(2)
            cur.execute("""insert into air.{}_names_proquest
                           (score, __employee_id, publication_number, __link_type_id)
                           values (%s, %s, %s, %s)""".format(table_prefix), row)

    db.commit()


def main(argv):
    parser = argparse.ArgumentParser(
            description="Create STAR METRICS summary tables and link STAR METRICS to ProQuest")

    parser.add_argument("--table-prefix", action="store",
            help="Prefix to use for database tables created by this script")

    parser.add_argument("--directory", action="store",
            help="Directory where the program will create or look for matching input files")

    parser.add_argument("--life-science-init", action="store_true",
            help="Load the list of 'life science' codes into the database.")

    parser.add_argument("--gender-probabilities-init", action="store_true",
            help="Load names and gender probabilities into the database.")

    parser.add_argument("--sm-init", action="store_true",
            help="Initialize STAR METRICS summary tables")

    parser.add_argument("--pq-init", action="store_true",
            help="Initialize ProQuest summary tables")
    
    parser.add_argument("--matching-input", action="store_true",
            help="Create input files for matching")

    parser.add_argument("--matching", action="store_true",
            help="Run the matching program on the matching input file")

    parser.add_argument("--assignment", action="store_true",
            help="Run the one-to-one assignment algorith")

    parser.add_argument("--upload", action="store_true",
            help="Upload one-to-one links to the database")

    args = parser.parse_args(argv)

    conf = read_configuration("config.properties")

    if args.life_science_init or args.gender_probabilities_init or args.sm_init or \
            args.pq_init or args.matching_input or args.upload:
        db = MySQLdb.connect(user=conf["db_user"], 
                             passwd=conf["db_passwd"], 
                             db=conf["db_db"], 
                             host=conf["db_host"])

        if args.table_prefix:
            table_prefix = args.table_prefix
        else:
            table_prefix = "smpq"

        print("Using table prefix '{}'".format(table_prefix))

    if args.matching_input or args.matching or args.assignment or args.upload:
        if args.directory:
            print("Using working directory: {}".format(args.directory))
            directory = args.directory
        else:
            print("WARNING: no working directory specified, using current directory")
            directory = "."

    if args.life_science_init:
        life_science_init(db, table_prefix)

    if args.gender_probabilities_init:
        male_cutoff = conf["gender_male_cutoff"]
        female_cutoff = conf["gender_female_cutoff"]

        gender_probabilities_init(db, conf["gender_file"], male_cutoff, female_cutoff, table_prefix)

    if args.sm_init: 
        print("Creating STAR METRICS tables...")
        sm_init(db, conf["sm_source_ids"], table_prefix)

    if args.pq_init:
        print("Creating ProQuest tables...")
        pq_init(db, table_prefix)

    if args.matching_input:
        print("Creating matching input files...")
        create_matching_input_files(db, directory)

    if args.matching:
        print("Performing record linkage...")
        initial_matching(directory)

    if args.assignment:
        print("Extracting one-to-one links...")
        extract_1x1_links(directory)

    if args.upload:
        print("Loading one-to-one links into the database...")

    print("DONE")


if __name__ == "__main__":
    main(sys.argv[1:])
