# system will train on x number of units
# predict on x number
# replace/add to train base every 24 hours

# system tests for best predictor situation based on the day's dataset
# we want to account for the changing seasonality/population that a hospital faces


# for this system, we will upload the data and keep it
# in reality, the SQL would be pulled from the system each time

################################################################################
###                      IMPRORT NECESSARY LIBRARIES                         ###
################################################################################

import pandas as pd
import numpy as np
import pandas as pd
import numpy as np
import scipy as sp
import sklearn as sk
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score
import sklearn.ensemble as skens
import sklearn.metrics as skmetric
import sklearn.naive_bayes as sknb
import sklearn.tree as sktree
import matplotlib.pyplot as plt
#%matplotlib inline
#import seaborn as sns
#sns.set(style='white', color_codes=True, font_scale=1.3)
import sklearn.externals.six as sksix
#import IPython.display as ipd
from sklearn.model_selection import cross_val_score
from sklearn import metrics
import os
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score

################################################################################
###                         SYSTEM DATA BASE                                 ###
################################################################################

################################################################################
## Admissions + Diagnosis
diag = pd.read_csv("ad_diag.csv") # read in csv file
# create 3 new columns breaking out ICD9 code
diag['FIRST_ICD9_DIAGNOSIS'] = np.where(diag['SEQ_NUM'] == 1, diag['ICD9_CODE'], 0)
diag['SECOND_ICD9_DIAGNOSIS'] = np.where(diag['SEQ_NUM'] == 2, diag['ICD9_CODE'], 0)
diag['THIRD_ICD9_DIAGNOSIS'] = np.where(diag['SEQ_NUM'] == 3, diag['ICD9_CODE'], 0)
# only take necessary columns
diag_condense = diag[['SUBJECT_ID', 'HADM_ID', 'SEQ_NUM', 'FIRST_ICD9_DIAGNOSIS', 'SECOND_ICD9_DIAGNOSIS', 'THIRD_ICD9_DIAGNOSIS']].copy()
# only choose rows corresponding to first, second, and third assigned ICD9 code
diag_c = diag_condense[(diag_condense['SEQ_NUM'] < 4)]

################################################################################
## Admissions + Patients
pat = pd.read_csv('AD_PAT.csv')
# change ADMITTIME and DISCHTIME to datetime objects to make
# a new column for length of stay
pat['ADMITTIME'] = pd.to_datetime(pat['ADMITTIME'])
pat['DISCHTIME'] = pd.to_datetime(pat['DISCHTIME'])
pat['LENGTHOFSTAY'] = pat['DISCHTIME'] - pat['ADMITTIME']
pat['LENGTHOFSTAY_DAYS'] = [d.days for d in pat['LENGTHOFSTAY']]

################################################################################
## Admissions + ICU
icu = pd.read_csv('AD_ICU_PLACE.csv')
icu = icu[['SUBJECT_ID', 'HADM_ID', 'ICUFIRSTCAREUNIT', 'ICULASTCAREUNIT', 'ICUFIRSTWARDID', 'ICULASTWARDID','ICU_LOS']]
icu['SAMECARE'] = np.where(icu.ICUFIRSTCAREUNIT == icu.ICULASTCAREUNIT, 1, 0)
icu['SAMEWARD'] = np.where(icu.ICUFIRSTWARDID == icu.ICULASTWARDID, 1, 0)

################################################################################
## Admissions + Chart Events(MIN/FIRST)
ce_min = pd.read_csv('AD_CE_MIN.csv')
ce_min = ce_min[['SUBJECT_ID', 'HADM_ID', 'ITEMID', 'VALUE', 'VALUENUM']]
ce_min.columns = ['SUBJECT_ID', 'HADM_ID', 'FIRST_ITEMID', 'FIRST_VALUE', 'FIRST_VALUENUM']

categories = []
for index, row in ce_min.iterrows():
    try:
        int(row[3])
        categories.append(0)
    except:
        categories.append(row[3])

categories_final = []
for each in categories:
    try:
        float(each)
        categories_final.append(0)
    except:
        categories_final.append(each)
ce_min['FIRST_CATEGORY'] = categories_final

ce_min['FIRST_NUMBER'] = np.where(ce_min['FIRST_CATEGORY'] == 0, ce_min['FIRST_VALUE'], 0)
ce_min = ce_min[['SUBJECT_ID', 'HADM_ID', 'FIRST_ITEMID', 'FIRST_CATEGORY', 'FIRST_NUMBER']].copy()
grouped = ce_min[['FIRST_ITEMID', 'FIRST_CATEGORY']].copy()
grouped = grouped.drop_duplicates().sort_values(by=['FIRST_ITEMID'])

n = 0
m = 1
store = []
labels = []
for index, row in grouped.iterrows():
    if n == 0:
        store.append(row[0])
        labels.append(m)
    else:
        if row[0] != store[n-1]:
            m = 1
            labels.append(m)
            store.append(row[0])
        else:
            labels.append(m)
            store.append(row[0])
    n = n + 1
    m = m + 1

grouped['CATS'] = labels

together = ce_min.merge(grouped, left_on = ['FIRST_ITEMID', 'FIRST_CATEGORY'], right_on = ['FIRST_ITEMID', 'FIRST_CATEGORY'])
together = together[['SUBJECT_ID', 'HADM_ID', 'FIRST_ITEMID', 'CATS', 'FIRST_NUMBER']]
together['FIRST_ITEMID2'] = np.where(together.FIRST_ITEMID == 'None', -1, together.FIRST_ITEMID)

################################################################################
## Admissions + Chart Events(MAX/LAST)
ce_max = pd.read_csv('AD_CE_MAX.csv')
ce_max = ce_max[['SUBJECT_ID', 'HADM_ID', 'ITEMID', 'VALUE', 'VALUENUM']]
ce_max.columns = ['SUBJECT_ID', 'HADM_ID', 'LAST_ITEMID', 'LAST_VALUE', 'LAST_VALUENUM']

categories = []
for index, row in ce_max.iterrows():
    try:
        int(row[3])
        categories.append(0)
    except:
        categories.append(row[3])

categories_final = []
for each in categories:
    try:
        float(each)
        categories_final.append(0)
    except:
        categories_final.append(each)
ce_max['LAST_CATEGORY'] = categories_final

ce_max['LAST_NUMBER'] = np.where(ce_max['LAST_CATEGORY'] == 0, ce_max['LAST_VALUE'], 0)
ce_max = ce_max[['SUBJECT_ID', 'HADM_ID', 'LAST_ITEMID', 'LAST_CATEGORY', 'LAST_NUMBER']].copy()
grouped = ce_max[['LAST_ITEMID', 'LAST_CATEGORY']].copy()
grouped = grouped.drop_duplicates().sort_values(by=['LAST_ITEMID'])

n = 0
m = 1
store = []
labels = []
for index, row in grouped.iterrows():
    if n == 0:
        store.append(row[0])
        labels.append(m)
    else:
        if row[0] != store[n-1]:
            m = 1
            labels.append(m)
            store.append(row[0])
        else:
            labels.append(m)
            store.append(row[0])
    n = n + 1
    m = m + 1

grouped['LAST_CATS'] = labels

together_max = ce_max.merge(grouped, left_on = ['LAST_ITEMID', 'LAST_CATEGORY'], right_on = ['LAST_ITEMID', 'LAST_CATEGORY'])
together_max = together_max[['SUBJECT_ID', 'HADM_ID', 'LAST_ITEMID', 'LAST_CATS', 'LAST_NUMBER']]
together_max['LAST_ITEMID2'] = np.where(together_max.LAST_ITEMID == 'None', -1, together_max.LAST_ITEMID)

################################################################################


# merge on SUBJECT_ID and HADM_ID to make large set
# this will serve as our 'data base'
# diag_c, pat, icu, together, together_max
diag_pat = diag_c.merge(pat, left_on=['SUBJECT_ID', 'HADM_ID'], right_on=['SUBJECT_ID', 'HADM_ID'])
dp_icu = diag_pat.merge(icu, left_on=['SUBJECT_ID', 'HADM_ID'], right_on=['SUBJECT_ID', 'HADM_ID'])
dpi_ce_min = dp_icu.merge(together, left_on=['SUBJECT_ID', 'HADM_ID'], right_on=['SUBJECT_ID', 'HADM_ID'])
dpicm_cm = dpi_ce_min.merge(together_max, left_on=['SUBJECT_ID', 'HADM_ID'], right_on=['SUBJECT_ID', 'HADM_ID'])

# Variables we want to keep in the dataset:
# 'FIRST_ITEMID2', 'CATS', 'FIRST_NUMBER'
# 'ICUFIRSTCAREUNIT', 'ICULASTCAREUNIT', 'ICUFIRSTWARDID', 'ICULASTWARDID', 'ICU_LOS', 'SAMECARE', 'SAMEWARD'
# 'LANGUAGE', 'RELIGION', 'MARITAL_STATUS', 'ETHNICITY', 'ADMISSIONTYPE', 'DIAGNOSIS', 'GENDER', 'LENGTHOFSTAY_DAYS'

################################################################################

def choose_ICD9_set(data_bit, num_choice):
    returned_set_p1 = data_bit[(data_bit['SEQ_NUM'] == num_choice)]
    full_set = ['FIRST_ITEMID2', 'CATS', 'FIRST_NUMBER']
    full_set.extend(('LAST_ITEMID2', 'LAST_CATS', 'LAST_NUMBER'))
    full_set.extend(('ICUFIRSTCAREUNIT', 'ICULASTCAREUNIT', 'ICUFIRSTWARDID'))
    full_set.extend(('ICULASTWARDID', 'ICU_LOS', 'SAMECARE', 'SAMEWARD'))
    full_set.extend(('LANGUAGE', 'RELIGION', 'MARITAL_STATUS', 'ETHNICITY'))
    full_set.extend(('DIAGNOSIS', 'GENDER', 'LENGTHOFSTAY_DAYS'))
    if num_choice == 1:
        use_set = ['FIRST_ICD9_DIAGNOSIS']
        for each in full_set:
            use_set.append(each)
        returned_set = returned_set_p1[use_set]
    if num_choice == 2:
        use_set = ['SECOND_ICD9_DIAGNOSIS']
        for each in full_set:
            use_set.append(each)
        returned_set = returned_set_p1[use_set]
    if num_choice == 3:
        use_set = ['THIRD_ICD9_DIAGNOSIS']
        for each in full_set:
            use_set.append(each)
        returned_set = returned_set_p1[use_set]

    return(returned_set)

# then split into the set for the assigned ICD9
icd9_one = choose_ICD9_set(dpicm_cm, 1)
icd9_two = choose_ICD9_set(dpicm_cm, 2)
icd9_three = choose_ICD9_set(dpicm_cm, 3)

## replace NAs with zero and turn categorical variables to numbers

language_replace = {'LANGUAGE':{}, 'RELIGION':{}, 'MARITAL_STATUS':{},
'ETHNICITY':{}, 'GENDER':{}, 'DIAGNOSIS':{}}

icu_care = icu[['ICUFIRSTCAREUNIT', 'ICULASTCAREUNIT']].copy()
icu_care = icu_care.drop_duplicates()

icu_ward = icu[['ICUFIRSTWARDID', 'ICULASTWARDID']].copy()
icu_ward = icu_ward.drop_duplicates()

def cats_across_columns(two_columns):
    to_replace = {}
    n = 1
    for index, row in two_columns.iterrows():
        if row[0] not in to_replace:
            to_replace[row[0]] = n
            n = n + 1
        if row[1] not in to_replace:
            to_replace[row[1]] = n
            n = n + 1
    return(to_replace)


care_replace = cats_across_columns(icu_care)
icu_care_replace = {'ICUFIRSTCAREUNIT':care_replace, 'ICULASTCAREUNIT':care_replace}

ward_replace = cats_across_columns(icu_ward)
icu_ward_replace = {'ICUFIRSTWARDID':ward_replace, 'ICULASTWARDID':ward_replace}

def assigning_labels(data_subset, replace_dict):
    for each_label in replace_dict:
        n = 1
        for each in data_subset[each_label].unique():
            replace_dict[each_label][each] = n
            n = n + 1

    return(replace_dict)

replace_one = assigning_labels(dpicm_cm, language_replace)
replace_full = {**replace_one, **icu_care_replace, **icu_ward_replace}

def recode_cats(o_data, replace_dict):
    for each_key in replace_dict:
        o_data[each_key] = o_data[each_key].map(replace_dict[each_key])
    return(o_data)

# then apply recode
icd9_one = recode_cats(icd9_one, replace_full)
# replace nan with 0
icd_one = icd9_one.fillna(0)

# then apply recode
icd9_two = recode_cats(icd9_two, replace_full)
# replace nan with 0
icd_two = icd9_two.fillna(0)

# then apply recode
icd9_three = recode_cats(icd9_three, replace_full)
# replace nan with 0
icd_three = icd9_three.fillna(0)

################################################################################
###                                 PREDICTION                               ###
################################################################################

def visualize_accuracy(acc_dict, file_name):
    objects = []
    performance = []
    for each in acc_dict:
        objects.append(each)
        performance.append(acc_dict[each])

    y_pos = np.arange(len(objects))

    plt.bar(y_pos, performance, align='center', alpha=0.5, color=(254/255, 168/255, 182/255))
    plt.xticks(y_pos, objects)
    plt.ylim(0, 1)
    plt.ylabel('Accuracy')
    plt.xlabel('Test Size')
    plt.title('Accuracy Across Test/Train Sizes')
    plt.savefig(file_name)
    plt.close()

def forest_sizechange(o_data, test_list, icd9):

    if icd9 == 1:
        icd_col = 'FIRST_ICD9_DIAGNOSIS'
    elif icd9 == 2:
        icd_col = 'SECOND_ICD9_DIAGNOSIS'
    elif icd9 == 3:
        icd_col = 'THIRD_ICD9_DIAGNOSIS'
    else:
        icd_col = 'ICD9_DIAGNOSIS'

    acc_each = {}
    for each_test in test_list:
        otrain,otest = train_test_split(o_data, test_size=each_test)
        rf_model = skens.RandomForestClassifier(n_estimators=10,oob_score=True, criterion='entropy')
        rf_model.fit(otrain.ix[:,1:],otrain[icd_col])
        predicted_labels = rf_model.predict(otest.ix[:,1:])
        new_col = 'predicted_rf_tree_' + str(each_test)
        otest[new_col] = predicted_labels
        accuracy = accuracy_score(otest[icd_col], predicted_labels)
        acc_each[each_test] = accuracy

    return(acc_each)

t = [0.1, 0.2, 0.3, 0.4, 0.5]

#first_icd9 = forest_sizechange(icd_one, t, 1)
#visualize_accuracy(first_icd9, 'first_icd9.png')

#second_icd9 = forest_sizechange(icd_two, t, 2)
#visualize_accuracy(second_icd9, 'second_icd9.png')

#third_icd9 = forest_sizechange(icd_three, t, 3)
#visualize_accuracy(third_icd9, 'third_icd9.png')

icd_one_re = icd_one.rename({'FIRST_ICD9_DIAGNOSIS':'ICD9_DIAGNOSIS'}, axis=1)
icd_two_re = icd_two.rename({'SECOND_ICD9_DIAGNOSIS':'ICD9_DIAGNOSIS'}, axis=1)
icd_three_re = icd_three.rename({'THIRD_ICD9_DIAGNOSIS':'ICD9_DIAGNOSIS'}, axis=1)
icd_full = pd.concat([icd_one_re, icd_two_re, icd_three_re])

full_icd9 = forest_sizechange(icd_full, t, 4)
visualize_accuracy(full_icd9, 'full_icd9.png')


################################################################################
###                            TUNING THE MODEL                              ###
################################################################################



################################################################################
###                    LOCATING DIFFERENCES IN PREDICTION                    ###
################################################################################



################################################################################
###                   BETTER AT PREDICTING PART OF THE CODE?                 ###
################################################################################


################################################################################
###                 SYSTEM EXAMPLE REFLECTING DATA STREAM                    ###
################################################################################
# inital base model
rf_model = skens.RandomForestClassifier(n_estimators=10,oob_score=True, criterion='entropy')

# CHOOSE AN AMOUNT (i.e. number of patients per day)
p = 100

# CHOOSE AN AMOUNT (i.e. number of original train base)
t = 10000

base_startrow = 0
base_endrow = t
test_startrow = t + 1
test_endrow = test_startrow + p

# TRAIN THE MODEL ON t

rf_model.fit(diag_pat_one.ix[base_startrow:base_endrow,:],diag_pat_one_train.FIRST_ICD9_DIAGNOSIS)

# MAKE PREDICTIONS on first set of p
predicted_labels = rf_model.predict(diag_pat_one.ix[test_startrow:test_endrow,1:])

# PROVIDE STATISTICS ON THE PREDICTIONS
diag_pat_one_test['predicted_rf_tree'] = predicted_labels
accuracy = accuracy_score(diag_pat_one_test.FIRST_ICD9_DIAGNOSIS, predicted_labels)

base_startrow = base_startrow + p
base_endrow = base_endrow + p
test_startrow = base_endrow + 1
test_endrow = test_startrow + p

# REPLACE (A PERCENTAGE) OF THE ORIGINAL AMOUNT WITH 'NEW' PATIENTS
# remove 'last' p
# add 'new' p

# REPEAT

def system_data_stream(full_set_first, full_set_second, full_set_third):
    pass
