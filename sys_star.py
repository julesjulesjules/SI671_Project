################################################################################
###                      IMPORT NECESSARY LIBRARIES                         ###
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

one = pd.read_csv('icd_one.csv')
two = pd.read_csv('icd_two.csv')
three = pd.read_csv('icd_three.csv')
full = pd.read_csv('icd_full.csv')

################################################################################

################################################################################
###                 SYSTEM EXAMPLE REFLECTING DATA STREAM                    ###
################################################################################


# CHOOSE AN AMOUNT (i.e. number of patients per day)
p = 100

# CHOOSE AN AMOUNT (i.e. number of original train base)
t = 70000

def system_data_stream(full_set_first, ppd, otb):
    base_startrow = 1
    base_endrow = otb
    test_startrow = otb + 1
    test_endrow = test_startrow + ppd
    day = 1

    # inital base model
    rf_model = skens.RandomForestClassifier(n_estimators=10,oob_score=True, criterion='entropy')

    calendar = []

    while day < 30:
        # TRAIN THE MODEL ON t
        base = full_set_first.ix[base_startrow:base_endrow,:].copy()
        rf_model.fit(base, base.FIRST_ICD9_DIAGNOSIS)

        # MAKE PREDICTIONS on first set of p
        new_guys = full_set_first.ix[test_startrow:test_endrow,:].copy()
        predicted_labels = rf_model.predict(new_guys.ix[:,1:])

        # PROVIDE STATISTICS ON THE PREDICTIONS
        #diag_pat_one_test['predicted_rf_tree'] = predicted_labels
        accuracy = accuracy_score(new_guys['FIRST_ICD9_DIAGNOSIS'], predicted_labels)

        # record to the calendar
        calendar.append((day, accuracy))

        # REPLACE (A PERCENTAGE) OF THE ORIGINAL AMOUNT WITH 'NEW' PATIENTS
        base_startrow = base_startrow + ppd
        print(base_startrow)
        base_endrow = base_endrow + ppd
        print(base_endrow)
        test_startrow = base_endrow + 1
        print(test_startrow)
        test_endrow = test_startrow + ppd
        print(test_endrow)
        day = day + 1
        print(day)

    return(calendar)

# scramble one
one = one.sample(frac=1)

#print(system_data_stream(one, p, t))
rf_model = skens.RandomForestClassifier(n_estimators=10,oob_score=True, criterion='entropy')

calendar = []

base_startrow = 1
base_endrow = t
test_startrow = t + 1
test_endrow = test_startrow + p
day = 1

print(base_startrow)
print(base_endrow)
print(test_startrow)
print(test_endrow)
print(day)

# TRAIN THE MODEL ON t
base = one.iloc[base_startrow:base_endrow,:].copy()
rf_model.fit(base.ix[:,1:], base.ix[:,0])

# MAKE PREDICTIONS on first set of p
new_guys = one.ix[test_startrow:test_endrow,:].copy()
predicted_labels = rf_model.predict(new_guys.ix[:,1:])

# PROVIDE STATISTICS ON THE PREDICTIONS
#diag_pat_one_test['predicted_rf_tree'] = predicted_labels
accuracy = accuracy_score(new_guys.ix[:,0], predicted_labels)

# record to the calendar
calendar.append((day, accuracy))

# REPLACE (A PERCENTAGE) OF THE ORIGINAL AMOUNT WITH 'NEW' PATIENTS
base_startrow = base_startrow + p
print(base_startrow)
base_endrow = base_endrow + p
print(base_endrow)
test_startrow = base_endrow + 1
print(test_startrow)
test_endrow = test_startrow + p
print(test_endrow)
day = day + 1
print(day)

print(calendar)
print("\n")
print("\n")

#rf_model = skens.RandomForestClassifier(n_estimators=10,oob_score=True, criterion='entropy')

# TRAIN THE MODEL ON t
base = one.iloc[base_startrow:base_endrow,:].copy()
print(base.head())
#rf_model.fit(base.ix[:,1:], base.ix[:,0])

# MAKE PREDICTIONS on first set of p
#new_guys = one.ix[test_startrow:test_endrow,:].copy()
#predicted_labels = rf_model.predict(new_guys.ix[:,1:])

# PROVIDE STATISTICS ON THE PREDICTIONS
#diag_pat_one_test['predicted_rf_tree'] = predicted_labels
#accuracy = accuracy_score(new_guys.ix[:,0], predicted_labels)

# record to the calendar
#calendar.append((day, accuracy))

# REPLACE (A PERCENTAGE) OF THE ORIGINAL AMOUNT WITH 'NEW' PATIENTS
base_startrow = base_startrow + p
print(base_startrow)
base_endrow = base_endrow + p
print(base_endrow)
test_startrow = base_endrow + 1
print(test_startrow)
test_endrow = test_startrow + p
print(test_endrow)
day = day + 1
print(day)

print(calendar)
