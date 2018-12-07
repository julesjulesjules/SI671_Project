import sqlite3
import csv
import json
import codecs
import sys
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)

DBNAME = 'mimic3.db'

def make_db(DBNAME):
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except Error as e:
        print(e)
        return("Failed making database.")

################################################################################

    statement = '''
        DROP TABLE IF EXISTS 'ADMISSIONS';
    '''
    cur.execute(statement)
    conn.commit()

    statement = '''
        DROP TABLE IF EXISTS 'CHARTEVENTS';
    '''
    cur.execute(statement)
    conn.commit()

    statement = '''
        DROP TABLE IF EXISTS 'DATETIMEEVENTS';
    '''
    cur.execute(statement)
    conn.commit()

    statement = '''
        DROP TABLE IF EXISTS 'DIAGNOSES_ICD';
    '''
    cur.execute(statement)
    conn.commit()

    statement = '''
        DROP TABLE IF EXISTS 'ICU_STAYS';
    '''
    cur.execute(statement)
    conn.commit()

    statement = '''
        DROP TABLE IF EXISTS 'PATIENTS';
    '''
    cur.execute(statement)
    conn.commit()

    statement = '''
        DROP TABLE IF EXISTS 'PROCEDURES_ICD';
    '''
    cur.execute(statement)
    conn.commit()

    statement = '''
        DROP TABLE IF EXISTS 'SERVICES';
    '''
    cur.execute(statement)
    conn.commit()

    statement = '''
        DROP TABLE IF EXISTS 'TRANSFERS';
    '''
    cur.execute(statement)
    conn.commit()

################################################################################

    #make Admissions table
    statement = '''
        CREATE TABLE 'ADMISSIONS' (
            ROW_ID INTEGER,
            SUBJECT_ID INTEGER,
            HADM_ID INTEGER,
            ADMITTIME DATETIME,
            DISCHTIME DATETIME,
            DEATHTIME DATETIME,
            ADMISSION_TYPE TEXT,
            ADMISSION_LOCATION TEXT,
            DISCHARGE_LOCATION TEXT,
            INSURANCE TEXT,
            LANGUAGE TEXT,
            RELIGION TEXT,
            MARITAL_STATUS TEXT,
            ETHNICITY TEXT,
            EDREGTIME DATETIME,
            EDOUTTIME DATETIME,
            DIAGNOSIS TEXT,
            HOSPITAL_EXPIRE_FLAG INTEGER,
            HAS_CHARTEVENTS_DATA INTEGER
        ); '''

    cur.execute(statement)
    conn.commit()

    #make ChartEvents table
    statement = '''
        CREATE TABLE 'CHARTEVENTS' (
            'ROW_ID' INTEGER,
            'SUBJECT_ID' INTEGER,
            'HADM_ID' INTEGER,
            'ICUSTAY_ID' INTEGER,
            'ITEMID' INTEGER,
            'CHARTTIME' DATETIME,
            'STORETIME' DATETIME,
            'CGID' INTEGER,
            'VALUE' TEXT,
            'VALUENUM' REAL,
            'VALUEUOM' TEXT,
            'WARNING' INTEGER,
            'ERROR' INTEGER,
            'RESULTSTATUS' TEXT,
            'STOPPED' TEXT
        ); '''

    cur.execute(statement)
    conn.commit()

    # make DateTimeEvents table
    statement = '''
        CREATE TABLE 'DATETIMEEVENTS' (
            ROW_ID INTEGER,
            SUBJECT_ID INTEGER,
            HADM_ID INTEGER,
            ICUSTAY_ID INTEGER,
            ITEMID INTEGER,
            CHARTTIME DATETIME,
            STORETIME DATETIME,
            CGID INTEGER,
            VALUE DATETIME,
            VALUEUOM TEXT,
            WARNING INTEGER,
            ERROR INTEGER,
            RESULTSTATUS TEXT,
            STOPPED TEXT
        ); '''

    cur.execute(statement)
    conn.commit()

    # make Diangoses_ICD table
    statement = '''
        CREATE TABLE 'DIAGNOSES_ICD' (
            ROW_ID INTEGER,
            SUBJECT_ID INTEGER,
            HADM_ID INTEGER,
            SEQ_NUM INTEGER,
            ICD9_CODE TEXT
        ); '''

    cur.execute(statement)
    conn.commit()

    # make ICU_Stays table
    statement = '''
        CREATE TABLE 'ICU_STAYS' (
            ROW_ID INTEGER,
            SUBJECT_ID INTEGER,
            HADM_ID INTEGER,
            ICUSTAY_ID INTEGER,
            DBSOURCE TEXT,
            FIRST_CAREUNIT TEXT,
            LAST_CAREUNIT TEXT,
            FIRST_WARDID INTEGER,
            LAST_WARDID INTEGER,
            INTIME DATETIME,
            OUTTIME DATETIME,
            LOS REAL
        ); '''

    cur.execute(statement)
    conn.commit()

    # make Patients table
    statement = '''
        CREATE TABLE 'PATIENTS' (
            ROW_ID INTEGER,
            SUBJECT_ID INTEGER,
            GENDER TEXT,
            DOB TEXT,
            DOD DATETIME,
            DOD_HOSP DATETIME,
            DOD_SSN DATETIME,
            EXPIRE_FLAG INTEGER
        ); '''

    cur.execute(statement)
    conn.commit()

    # make Procedures_ICD table
    statement = '''
        CREATE TABLE 'PROCEDURES_ICD' (
            ROW_ID INTEGER,
            SUBJECT_ID INTEGER,
            HADM_ID INTEGER,
            SEQ_NUM INTEGER,
            ICD9_CODE TEXT
        ); '''

    cur.execute(statement)
    conn.commit()

    # make Services table
    statement = '''
        CREATE TABLE 'SERVICES' (
            ROW_ID INTEGER,
            SUBJECT_ID INTEGER,
            HADM_ID INTEGER,
            TRANSFERTIME DATETIME,
            PREV_SERVICE TEXT,
            CURR_SERVICE TEXT
        ); '''

    cur.execute(statement)
    conn.commit()

    # make Transfers table
    statement = '''
        CREATE TABLE 'TRANSFERS' (
            ROW_ID INTEGER,
            SUBJECT_ID INTEGER,
            HADM_ID INTEGER,
            ICUSTAY_ID INTEGER,
            DBSOURCE TEXT,
            EVENTTYPE TEXT,
            PREV_CAREUNIT TEXT,
            CURR_CAREUNIT TEXT,
            PREV_WARDID INTEGER,
            CURR_WARDID INTEGER,
            INTIME DATETIME,
            OUTTIME DATETIME,
            LOS REAL
        ); '''

    cur.execute(statement)
    conn.commit()

################################################################################

    with open("ADMISSIONS.csv") as f:
        readin = csv.reader(f)
        n = 0
        for eachrow in readin:
            if n == 0:
                pass
            else:
                insertion = (eachrow[0], eachrow[1], eachrow[2], eachrow[3], eachrow[4], eachrow[5], eachrow[6], eachrow[7], eachrow[8], eachrow[9], eachrow[10], eachrow[11], eachrow[12], eachrow[13], eachrow[14], eachrow[15], eachrow[16], eachrow[17], eachrow[18])
                insertstatement = "insert into 'ADMISSIONS' "
                insertstatement += "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
                cur.execute(insertstatement, insertion)
            n = n + 1
        conn.commit()

    with open("CHARTEVENTS.csv") as f:
        readin = csv.reader(f)
        n = 0
        for eachrow in readin:
            if n == 0:
                pass
            else:
                insertion = (eachrow[0], eachrow[1], eachrow[2], eachrow[3], eachrow[4], eachrow[5], eachrow[6], eachrow[7], eachrow[8], eachrow[9], eachrow[10], eachrow[11], eachrow[12], eachrow[13], eachrow[14])
                insertstatement = "insert into 'CHARTEVENTS' "
                insertstatement += "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
                cur.execute(insertstatement, insertion)
            n = n + 1
        conn.commit()

    with open("DATETIMEEVENTS.csv") as f:
        readin = csv.reader(f)
        n = 0
        for eachrow in readin:
            if n == 0:
                pass
            else:
                insertion = (eachrow[0], eachrow[1], eachrow[2], eachrow[3], eachrow[4], eachrow[5], eachrow[6], eachrow[7], eachrow[8], eachrow[9], eachrow[10], eachrow[11], eachrow[12], eachrow[13])
                insertstatement = "insert into 'DATETIMEEVENTS' "
                insertstatement += "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
                cur.execute(insertstatement, insertion)
            n = n + 1
        conn.commit()

    with open("DIAGNOSES_ICD.csv") as f:
        readin = csv.reader(f)
        n = 0
        for eachrow in readin:
            if n == 0:
                pass
            else:
                insertion = (eachrow[0], eachrow[1], eachrow[2], eachrow[3], eachrow[4])
                insertstatement = "insert into 'DIAGNOSES_ICD' "
                insertstatement += "values (?, ?, ?, ?, ?);"
                cur.execute(insertstatement, insertion)
            n = n + 1
        conn.commit()

    with open("ICUSTAYS.csv") as f:
        readin = csv.reader(f)
        n = 0
        for eachrow in readin:
            if n == 0:
                pass
            else:
                insertion = (eachrow[0], eachrow[1], eachrow[2], eachrow[3], eachrow[4], eachrow[5], eachrow[6], eachrow[7], eachrow[8], eachrow[9], eachrow[10], eachrow[11])
                insertstatement = "insert into 'ICU_STAYS' "
                insertstatement += "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
                cur.execute(insertstatement, insertion)
            n = n + 1
        conn.commit()

    with open("PATIENTS.csv") as f:
        readin = csv.reader(f)
        n = 0
        for eachrow in readin:
            if n == 0:
                pass
            else:
                insertion = (eachrow[0], eachrow[1], eachrow[2], eachrow[3], eachrow[4], eachrow[5], eachrow[6], eachrow[7])
                insertstatement = "insert into 'PATIENTS' "
                insertstatement += "values (?, ?, ?, ?, ?, ?, ?, ?);"
                cur.execute(insertstatement, insertion)
            n = n + 1
        conn.commit()

    with open("PROCEDURES_ICD.csv") as f:
        readin = csv.reader(f)
        n = 0
        for eachrow in readin:
            if n == 0:
                pass
            else:
                insertion = (eachrow[0], eachrow[1], eachrow[2], eachrow[3], eachrow[4])
                insertstatement = "insert into 'PROCEDURES_ICD' "
                insertstatement += "values (?, ?, ?, ?, ?);"
                cur.execute(insertstatement, insertion)
            n = n + 1
        conn.commit()

    with open("SERVICES.csv") as f:
        readin = csv.reader(f)
        n = 0
        for eachrow in readin:
            if n == 0:
                pass
            else:
                insertion = (eachrow[0], eachrow[1], eachrow[2], eachrow[3], eachrow[4], eachrow[5])
                insertstatement = "insert into 'SERVICES' "
                insertstatement += "values (?, ?, ?, ?, ?, ?);"
                cur.execute(insertstatement, insertion)
            n = n + 1
        conn.commit()

    with open("TRANSFERS.csv") as f:
        readin = csv.reader(f)
        n = 0
        for eachrow in readin:
            if n == 0:
                pass
            else:
                insertion = (eachrow[0], eachrow[1], eachrow[2], eachrow[3], eachrow[4], eachrow[5], eachrow[6], eachrow[7], eachrow[8], eachrow[9], eachrow[10], eachrow[11], eachrow[12])
                insertstatement = "insert into 'TRANSFERS' "
                insertstatement += "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
                cur.execute(insertstatement, insertion)
            n = n + 1
        conn.commit()

    conn.close()
    return("Success making database & tables.")

make_db(DBNAME)
