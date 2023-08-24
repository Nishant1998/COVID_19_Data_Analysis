#!/usr/bin/env python
# coding: utf-8

# # __Calculate peaks__

# In[10]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.signal import argrelextrema as extrema


# In[11]:


#import covid data
covidData = pd.read_csv("Data/districts-modified-v2.csv",low_memory=False,index_col=False)


# In[12]:


# get all districts list
with np.load("Data/districts.npz",allow_pickle=True) as data:
    districts = data['arr_0']    
# get district to state dictionary
with np.load("Data/distToState.npz",allow_pickle=True) as data:
    distToState = data['arr_0'].reshape(1)[0] 
    
states = covidData['State_Code'].unique()    
states.sort()


# In[ ]:





# In[13]:


# Analysis week/month/overall for all districts
cases_week = pd.DataFrame(data=None, index=None,columns=['districtid','timeid','cases'])


for district in districts:
    #get district wise table
    index = covidData[ covidData['District_Key'] == district].index
    notDistIndex = np.setdiff1d(covidData.index,index)
    districWiseData = covidData.drop(notDistIndex, inplace = False)

    #sort table by date
    districWiseData.sort_values(by = 'Date', inplace = True,ignore_index=True)

    
    week = [0]*148 # 148 for overlaping weeks
    #extract day wise case
    dayWiseCase = list(districWiseData['Number_Of_Cases'])
    
    # calculate cases week wise for every district
    for i, j in zip(range(0,int(len(week)/2)), range(0,len(dayWiseCase),7) ):
        case_sum_w1 = 0 # normale week
        case_sum_w2 = 0 # overlapping week
        for k in range(7):
            case_sum_w1 = case_sum_w1 + dayWiseCase[j+k]
            if j != 511:
                case_sum_w2 = case_sum_w2 + dayWiseCase[j+k+4]
        week[i*2] = case_sum_w1 
        week[i*2 + 1] = case_sum_w2
    
    

    
    # make table
    ## to store weekly(overlapping) cases 
    df_week = pd.DataFrame(data=None, index=None,columns=['districtid','timeid','cases'])
    
    
    df_week['districtid'] = [district]*148
    df_week['timeid'] = range(1,149)
    df_week['cases'] = week
    
    
    cases_week = pd.concat([cases_week, df_week], ignore_index = True)
        


# In[14]:


# get district monthly cases
cases_month = pd.read_csv("Data/cases_month.csv",low_memory=False,index_col=False)


# In[15]:


# add state code in table
cases_week.insert(loc=0, column='stateid', value='')
cases_month.insert(loc=0, column='stateid', value='')
for i in districts:
    cases_week.loc[cases_week['districtid'] == i, 'stateid'] = distToState.get(i)
    cases_month.loc[cases_month['districtid'] == i, 'stateid'] = distToState.get(i)


# #### Funtion to calculate peaks

# In[16]:




# find peak point from group
def maxpt(x,y):
    maxval = 0
    p = 0
    for i in x:
        if maxval <= y[i]:
            maxval = y[i]
            p = i
    return p

# funtion to find peaks 
## x represent time, y for number of cases, d represent point of sepration for waves, 
#### doPlot is bool var if true it plot graph hich represent peak
def peaks(x,y,d,doPlot):
    y = np.array(y)
    
    sortId = np.argsort(x)
    x = x[sortId]
    y = y[sortId]

    # this way the x-axis corresponds to the index of x
    ## find all maxima point
    maxima = extrema(y, np.greater) 
    minima = extrema(y, np.less)
    
    #divide in group for two waves
    g1 = np.array( sorted(i for i in maxima[0] if i <= d))
    g2 = np.array( sorted(i for i in maxima[0] if i >= d))
    peak1 = maxpt(g1,y) # get peak for wave 1
    peak2 = maxpt(g2,y) # get peak for wave 2
    
    # to plot graph 
    if doPlot == True:
        if d<=10:
            plt.plot(x, y)
        else:
            plt.plot(x-1,y)
        p = peak1
        q = y[peak1]
        plt.plot(p,q, marker='o', markersize=3, color="red")    
    
        p = peak2
        q = y[peak2]
        plt.plot(p,q, marker='o', markersize=3, color="red")
        plt.show()
        
    return peak1,peak2    
    
    
    


# #### For Districts

# In[17]:


districtPeaks = pd.DataFrame(data=None, index=None,columns=['districtid','wave1_weekid', 'wave2_weekid', 'wave1_monthid', 'wave2_monthid'])
statePeaks = pd.DataFrame(data=None, index=None,columns=['stateid','wave1_weekid', 'wave2_weekid', 'wave1_monthid', 'wave2_monthid'])
overallPeaks = pd.DataFrame(data=None, index=None,columns=['id','wave1_weekid', 'wave2_weekid', 'wave1_monthid', 'wave2_monthid'])


# In[20]:


## to find peaks for every districts
for district in districts:
    
    
    #get week wise table
    index = cases_week[ cases_week['districtid'] == district].index
    notDistIndex = np.setdiff1d(cases_week.index,index)
    weekWiseData = cases_week.drop(notDistIndex, inplace = False)
    
    #get month wise table
    index = cases_month[ cases_month['districtid'] == district].index
    notDistIndex = np.setdiff1d(cases_month.index,index)
    monthWiseData = cases_month.drop(notDistIndex, inplace = False)
    

    weekCaseList = list(weekWiseData['cases'])
    monthCaseList = list(monthWiseData['cases'])
    
    # Calculate peaks 
    peak1_weekid, peak2_weekid = peaks(np.array(range(0,148)), weekCaseList,80,False)
    peak1_monthid, peak2_monthid = peaks(np.array(range(0,17)), monthCaseList,10,False)
  
    
    # save peaks in data frame
    df = pd.DataFrame(columns=['districtid','wave1_weekid', 'wave2_weekid', 'wave1_monthid', 'wave2_monthid'])
    df.loc[0] = [district,peak1_weekid,peak2_weekid,peak1_monthid,peak2_monthid]
    districtPeaks = pd.concat([districtPeaks, df], ignore_index = True)

    
    


# In[22]:


overAllWeekSum = np.array([0]*148)
overAllMonthSum= np.array([0]*17)

# calculating peaks for states and overall
for state in states:
    #get week wise table
    index = cases_week[ cases_week['stateid'] == state].index
    notStateIndex = np.setdiff1d(cases_week.index,index)
    weekWiseData = cases_week.drop(notStateIndex, inplace = False)
    
    #get month wise table
    index = cases_month[ cases_month['stateid'] == state].index
    notStateIndex = np.setdiff1d(cases_month.index,index)
    monthWiseData = cases_month.drop(notStateIndex, inplace = False)
    
    stateDistricts = weekWiseData['districtid'].unique()
    stateDistricts.sort()
    
    #for every dist in week data
    weekSum = np.array([0]*148)
    for district in stateDistricts:
        index = weekWiseData[ weekWiseData['districtid'] == district].index
        notStateDistIndex = np.setdiff1d(weekWiseData.index,index)
        weekWiseStateDistData = weekWiseData.drop(notStateDistIndex, inplace = False)
        
        #add week cases of all distric for state
        weekSum = weekSum + np.array(weekWiseStateDistData['cases'])
        
    #for every dist in month data
    monthSum = np.array([0]*17)
    for district in stateDistricts:
        index = monthWiseData[ monthWiseData['districtid'] == district].index
        notStateDistIndex = np.setdiff1d(monthWiseData.index,index)
        monthWiseStateDistData = monthWiseData.drop(notStateDistIndex, inplace = False)
        
        #add week cases of all distric for state
        monthSum = monthSum + np.array(monthWiseStateDistData['cases'])    
     
    
    """Overall week month list calculation"""
    overAllWeekSum = overAllWeekSum + weekSum
    overAllMonthSum= overAllMonthSum + monthSum
    
    """ Calculate peak for  weekSum monthSum"""  
    peak1_weekid, peak2_weekid = peaks(np.array(range(0,148)), weekSum,80,False)
    peak1_monthid, peak2_monthid = peaks(np.array(range(0,17)), monthSum,10,False)
    

    

    df = pd.DataFrame(columns=['stateid','wave1_weekid', 'wave2_weekid', 'wave1_monthid', 'wave2_monthid'])
    
    df.loc[0] = [state,peak1_weekid+1,peak2_weekid+1,peak1_monthid+1,peak2_monthid+1]
    statePeaks = pd.concat([statePeaks, df], ignore_index = True)
    
 
peak1_weekid, peak2_weekid = peaks(np.array(range(0,148)), overAllWeekSum,80,False)
peak1_monthid, peak2_monthid = peaks(np.array(range(0,17)), overAllMonthSum,10,False)

df = pd.DataFrame(columns=['id','wave1_weekid', 'wave2_weekid', 'wave1_monthid', 'wave2_monthid'])

df.loc[0] = ['IN',peak1_weekid+1 ,peak2_weekid+1,peak1_monthid+1,peak2_monthid+1]

overallPeaks = pd.concat([overallPeaks, df], ignore_index = True)


# In[ ]:





# In[10]:


""" SAVE FILE FOR WEEK MONTH OVERALL PEAKS """
districtPeaks.to_csv("Data/district-peaks.csv",index=False)
statePeaks.to_csv("Data/state-peaks.csv",index=False)
overallPeaks.to_csv("Data/overall-peaks.csv",index=False)


# In[ ]:





# In[ ]:




