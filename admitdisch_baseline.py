import pandas as pd

# read in dataset -- is emergency admits only
emer = pd.read_csv('EmergencyICD4.csv')

# convert dates to datetime
emer['ADMITTIME'] = pd.to_datetime(emer['ADMITTIME'])
emer['DISCHTIME'] = pd.to_datetime(emer['DISCHTIME'])

# split out date and time into separate columns
emer['admit_date'] = [d.date() for d in emer['ADMITTIME']]
emer['admit_time'] = [d.time() for d in emer['ADMITTIME']]
emer['disch_date'] = [d.date() for d in emer['DISCHTIME']]
emer['disch_time'] = [d.time() for d in emer['DISCHTIME']]

# create list of ICD-9 codes that make up the top 40% of admits
emer['DIAG.ICD9_CODE'].value_counts().head(30).sum() #17540
icdtouse = pd.DataFrame(emer['DIAG.ICD9_CODE'].value_counts().head(30))
icdtouse.head()
icd = icdtouse.index

# makes dictionary of overall counts for each icd-9 code
capture_overall = {"OTHER": 0}
for index, row in emer.iterrows():
    if row['DIAG.ICD9_CODE'] in icd:
        if row['DIAG.ICD9_CODE'] not in capture_overall:
            capture_overall[row['DIAG.ICD9_CODE']] = 0
        capture_overall[row['DIAG.ICD9_CODE']] = capture_overall[row['DIAG.ICD9_CODE']] + 1
    else:
        capture_overall['OTHER'] = capture_overall['OTHER'] + 1

#for each in capture_overall:
#    print("{} : {}".format(each, capture_overall[each]))

# makes dictionary of counts of each icd-9 code by day
capture_by_day = {}
for index, row in emer.iterrows():
    if row['admit_date'] not in capture_by_day:
        capture_by_day[row['admit_date']] = {}

    if row['DIAG.ICD9_CODE'] in icd:
        if row['DIAG.ICD9_CODE'] not in capture_by_day[row['admit_date']]:
            capture_by_day[row['admit_date']][row['DIAG.ICD9_CODE']] = 0
        capture_by_day[row['admit_date']][row['DIAG.ICD9_CODE']] = capture_by_day[row['admit_date']][row['DIAG.ICD9_CODE']] + 1
    else:
        if 'OTHER' not in capture_by_day[row['admit_date']]:
            capture_by_day[row['admit_date']]['OTHER'] = 0
        capture_by_day[row['admit_date']]['OTHER'] = capture_by_day[row['admit_date']]['OTHER'] + 1

#for each in capture_by_day:
#    print(each)
#    for every in capture_by_day[each]:
#        print('\t {} : {}'.format(every, capture_by_day[each][every]))

# change capture by day dictionary into matrix type form
byday = pd.DataFrame(capture_by_day)
byday = byday.transpose()

# make separate columns for the year, day, and month
# bump out index as a column
byday['admitdate'] = byday.index
#byday['admitdate'] = pd.to_datetime(byday['admitdate'])
# split it into year, month, day columns
steal_dates = pd.DatetimeIndex(byday['admitdate'])
byday['year'] = steal_dates.year
byday['month'] = steal_dates.month
byday['day'] = steal_dates.day

# sort the dataframe by year, then by month, then by day
# so it appears in chronological order
byday = byday.sort_values(by=['year', 'month', 'day'])

## create total admit by day column
each_day_total = byday.ix[:,:31].sum(axis=1)
byday['day_total'] = each_day_total

# break into day, total form
baseline_set = byday[['admitdate', 'day_total']].copy()

# change baseline_set.admitdate to datetime object
baseline_set['admitdate'] = pd.to_datetime(baseline_set['admitdate'])

# make a copy of baseline_set and make a new column with the day replaced with 01
baseline_set2 = baseline_set.copy()
change_day = baseline_set2['admitdate']
new_day = change_day.apply(lambda dt: dt.replace(day=1))
baseline_set2['admitmonth'] = new_day

# only take the day total and the altered date columns
baseline_month = baseline_set2[['day_total', 'admitmonth']].copy()

# sum by year/month (so we have admits per month)
baseline_month = baseline_month.groupby(['admitmonth']).sum()

# set up baseline_month to be used, pulling the date column back out & renaming
baseline_month['MONTH'] = baseline_month.index
baseline_month.columns = ['ADMITS', 'MONTH']
baseline_month['MONTH'] = pd.to_datetime(baseline_month['MONTH'])

## option 1: look at each day
## use baseline_set with 'admitdate' and 'day_total'

## option 2: look at each month
## use baseline_month with 'MONTH' and 'ADMITS'

# chop to be smaller just for a test
shorter_month_test = baseline_month.iloc[:50, :].copy()

## USING PYAF
import pyaf.ForecastEngine as autof
lEngine = autof.cForecastEngine()
lEngine.train(shorter_month_test, 'MONTH' , 'ADMITS', 12) # predicts next 12

lEngine.getModelInfo()

#admit_forecast_dataframe = lEngine.forecast(shorter_month_test, 12);
#print(admit_forecast_dataframe.tail(20)) # show last 20
#print("\n")
#print(admit_forecast_dataframe[['MONTH' , 'ADMITS' , 'ADMITS_Forecast']].tail(20))
