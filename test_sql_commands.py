import sqlite3
import csv
import json
import codecs
import sys
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)

#strftime('%Y-%m', ADMITTIME) yr_mon
#selectbit = '''SELECT AD.SUBJECT_ID, AD.HADM_ID, AD.ADMITTIME, AD.DISCHTIME, AD.LANGUAGE,
#                    AD.RELIGION, AD.MARITAL_STATUS, AD.ETHNICITY, AD.ADMISSION_TYPE, AD.DIAGNOSIS, PAT.GENDER
#                FROM ADMISSIONS AS AD
#                    JOIN PATIENTS AS PAT
#                        ON AD.SUBJECT_ID = PAT.SUBJECT_ID
#                WHERE AD.ADMISSION_TYPE = 'EMERGENCY' OR AD.ADMISSION_TYPE = 'URGENT'
#                ;'''

#selectbit2 = '''SELECT AD.SUBJECT_ID, AD.HADM_ID, AD.ADMISSION_TYPE, MIN(CE.CHARTTIME), CE.ITEMID, CE.VALUE, CE.VALUENUM
#                FROM ADMISSIONS AS AD
#                    LEFT JOIN CHARTEVENTS AS CE
#                        ON AD.SUBJECT_ID = CE.SUBJECT_ID AND AD.HADM_ID = CE.HADM_ID
#                WHERE AD.ADMISSION_TYPE = 'EMERGENCY' OR AD.ADMISSION_TYPE = 'URGENT'
#                GROUP BY AD.HADM_ID
#                ;'''

selectbit3 = '''SELECT AD.SUBJECT_ID, AD.HADM_ID, AD.ADMISSION_TYPE, MAX(CE.CHARTTIME), CE.ITEMID, CE.VALUE, CE.VALUENUM
                FROM ADMISSIONS AS AD
                    LEFT JOIN CHARTEVENTS AS CE
                        ON AD.SUBJECT_ID = CE.SUBJECT_ID AND AD.HADM_ID = CE.HADM_ID
                WHERE AD.ADMISSION_TYPE = 'EMERGENCY' OR AD.ADMISSION_TYPE = 'URGENT'
                GROUP BY AD.HADM_ID
                ;'''

#selectbit4 = '''SELECT AD.SUBJECT_ID, AD.HADM_ID, AD.ADMISSION_TYPE, SERV.TRANSFERTIME, SERV.PREV_SERVICE, SERV.CURR_SERVICE
#                FROM ADMISSIONS AS AD
#                    JOIN SERVICES AS SERV
#                        ON AD.SUBJECT_ID = SERV.SUBJECT_ID AND AD.HADM_ID = SERV.SUBJECT_ID
#                WHERE AD.ADMISSION_TYPE = 'EMERGENCY' OR AD.ADMISSION_TYPE = 'URGENT'
#                GROUP BY AD.HADM_ID
#                ;'''

#selectbit5 = '''SELECT AD.SUBJECT_ID, AD.HADM_ID, AD.ADMISSION_TYPE, MAX(SERV.TRANSFERTIME), SERV.PREV_SERVICE, SERV.CURR_SERVICE
#                FROM ADMISSIONS AS AD
#                    JOIN SERVICES AS SERV
#                        ON AD.SUBJECT_ID = SERV.SUBJECT_ID AND AD.HADM_ID = SERV.SUBJECT_ID
#                WHERE AD.ADMISSION_TYPE = 'EMERGENCY' OR AD.ADMISSION_TYPE = 'URGENT'
#                GROUP BY AD.HADM_ID
#                ;'''

#selectbit6 = '''SELECT AD.SUBJECT_ID, AD.HADM_ID, AD.ADMISSION_TYPE, ICU.INTIME, ICU.FIRST_CAREUNIT, ICU.LAST_CAREUNIT, ICU.FIRST_WARDID, ICU.LAST_WARDID, ICU.LOS
#                FROM ADMISSIONS AS AD
#                    JOIN ICU_STAYS AS ICU
#                        ON AD.SUBJECT_ID = ICU.SUBJECT_ID AND AD.SUBJECT_ID = ICU.SUBJECT_ID
#                WHERE AD.ADMISSION_TYPE = 'EMERGENCY' OR AD.ADMISSION_TYPE = 'URGENT'
#                ;'''


#selectbit5 = '''SELECT AD.SUBJECT_ID, AD.HADM_ID, AD.ADMISSION_TYPE, DIAG.ICD9_CODE, DIAG.SEQ_NUM
#                FROM ADMISSIONS AS AD
#                    JOIN DIAGNOSES_ICD AS DIAG
#                        ON AD.SUBJECT_ID = DIAG.SUBJECT_ID AND AD.HADM_ID = DIAG.HADM_ID
#                WHERE AD.ADMISSION_TYPE = 'EMERGENCY' OR AD.ADMISSION_TYPE = 'URGENT'
#                ;'''
# CE.ITEMID, CE.VALUE, CE.VALUENUM,
# SERV.CURR_SERVICE,
# ICU.LOS,
# DIAG.ICD9_CODE, DIAG.SEQ_NUM

#JOIN CHARTEVENTS AS CE
#    ON AD.SUBJECT_ID = CE.SUBJECT_ID AND AD.HADM_ID = CE.HADM_ID
#JOIN SERVICES AS SERV
#    ON AD.SUBJECT_ID = SERV.SUBJECT_ID AND AD.HADM_ID = SERV.SUBJECT_ID
#JOIN ICU_STAYS AS ICU
#    ON AD.SUBJECT_ID = ICU.SUBJECT_ID AND AD.SUBJECT_ID = ICU.SUBJECT_ID


DBNAME = 'mimic3.db'
conn = sqlite3.connect(DBNAME)
cur = conn.cursor()

#cur.execute(selectbit)
#returnlist = []
## save in list to return it
#for each in cur:
#    returnlist.append(each)

#print(returnlist[0:10])

#with open('AD_PAT.csv', 'w') as file:
#    header = "SUBJECT_ID,HADM_ID,ADMITTIME,DISCHTIME,LANGUAGE,RELIGION,"
#    header += "MARITAL_STATUS,ETHNICITY,ADMISSIONTYPE,DIAGNOSIS,GENDER\n"
#    file.write(header)
#    for each in returnlist: #18
#        file.write("{},{},{},{},{},{},{},{},{},{},{}\n".format(each[0], each[1], each[2], each[3], each[4], each[5], each[6], each[7], each[8], each[9].replace(",", ""), each[10]))

###

#cur.execute(selectbit2)
#returnlist = []
## save in list to return it
#for each in cur:
#    returnlist.append(each)

#print(returnlist[0:10])

#with open('AD_CE_MIN.csv', 'w') as file:
#    header = "SUBJECT_ID,HADM_ID,ADMISSIONTYPE,CHARTTIME,ITEMID,VALUE,VALUENUM\n"
#    #header += "MARITAL_STATUS,ETHNICITY,,DIAGNOSIS,GENDER\n"
#    file.write(header)
#    for each in returnlist: #18
#        file.write("{},{},{},{},{},{}\n".format(each[0], each[1], each[2], each[3], each[4], each[5], each[6]))

###

###

cur.execute(selectbit3)
returnlist = []
## save in list to return it
for each in cur:
    returnlist.append(each)

#print(returnlist[0:10])

with open('AD_CE_MAX.csv', 'w') as file:
    header = "SUBJECT_ID,HADM_ID,ADMISSIONTYPE,CHARTTIME,ITEMID,VALUE,VALUENUM\n"
    #header += "MARITAL_STATUS,ETHNICITY,,DIAGNOSIS,GENDER\n"
    file.write(header)
    for each in returnlist: #18
        try:
            file.write("{},{},{},{},{},{}\n".format(each[0], each[1], each[2], each[3], each[4], str(each[5]).replace(",", ""), each[6]))
        except:
            file.write("{},{},{},{},{},{}\n".format(each[0], each[1], each[2], each[3], each[4], each[5], each[6]))

###



#cur.execute(selectbit4)
#returnlist = []
## save in list to return it
#for each in cur:
#    returnlist.append(each)

#print(returnlist[0:10])
#AD.SUBJECT_ID, AD.HADM_ID, AD.ADMISSION_TYPE, MAX(SERV.TRANSFERTIME), SERV.PREV_SERVICE, SERV.CURR_SERVICE

#with open('AD_SERV_PLACE.csv', 'w') as file:
#    header = "SUBJECT_ID,HADM_ID,ADMISSIONTYPE,TRANSFERTIME,PREV_SERVICE,CURR_SERVICE\n"
    #header += "MARITAL_STATUS,ETHNICITY,,DIAGNOSIS,GENDER\n"
#    file.write(header)
#    for each in returnlist: #18
#        file.write("{},{},{},{},{},{}\n".format(each[0], each[1], each[2], each[3], each[4], each[5]))
#

#cur.execute(selectbit5)
#returnlist = []
## save in list to return it
#for each in cur:
#    returnlist.append(each)

#print(returnlist[0:10])
#AD.SUBJECT_ID, AD.HADM_ID, AD.ADMISSION_TYPE, MAX(SERV.TRANSFERTIME), SERV.PREV_SERVICE, SERV.CURR_SERVICE

#with open('AD_SERV_MAX.csv', 'w') as file:
#    header = "SUBJECT_ID,HADM_ID,ADMISSIONTYPE,MAX_TRANSFERTIME,PREV_SERVICE,CURR_SERVICE\n"
    #header += "MARITAL_STATUS,ETHNICITY,,DIAGNOSIS,GENDER\n"
#    file.write(header)
#    for each in returnlist: #18
#        file.write("{},{},{},{},{},{}\n".format(each[0], each[1], each[2], each[3], each[4], each[5]))


###

#cur.execute(selectbit6)
#returnlist = []
## save in list to return it
#for each in cur:
#    returnlist.append(each)

#print(returnlist[0:10])
# AD.SUBJECT_ID, AD.HADM_ID, AD.ADMISSION_TYPE, MIN(ICU.INTIME), ICU.FIRST_CAREUNIT, ICU.LAST_CAREUNIT, ICU.FIRST_WARDID, ICU.LAST_WARDID, ICU.LOS

#with open('AD_ICU_PLACE.csv', 'w') as file:
#    header = "SUBJECT_ID,HADM_ID,ADMISSIONTYPE,INTIMEICU,ICUFIRSTCAREUNIT,ICULASTCAREUNIT,ICUFIRSTWARDID,ICULASTWARDID,ICU_LOS\n"
    #header += "MARITAL_STATUS,ETHNICITY,,DIAGNOSIS,GENDER\n"
#    file.write(header)
#    for each in returnlist: #18
#        file.write("{},{},{},{},{},{},{},{},{}\n".format(each[0], each[1], each[2], each[3], each[4], each[5], each[6], each[7], each[8]))

###

#cur.execute(selectbit5)
#returnlist = []
## save in list to return it
#for each in cur:
#    returnlist.append(each)

#print(returnlist[0:10])

#with open('AD_DIAG.csv', 'w') as file:
#    header = "SUBJECT_ID,HADM_ID,ADMISSIONTYPE,ICD9_CODE,SEQ_NUM\n"
    #header += "MARITAL_STATUS,ETHNICITY,,DIAGNOSIS,GENDER\n"
#    file.write(header)
#    for each in returnlist: #18
#        file.write("{},{},{},{},{}\n".format(each[0], each[1], each[2], each[3], each[4]))

conn.close()
