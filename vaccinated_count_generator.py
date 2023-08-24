#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
from datetime import timedelta, date


# In[2]:


import warnings
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


# In[3]:


# Load vaccination data modified in previous analysis
vaccineData = pd.read_csv("Data/vaccineData-modified.csv",low_memory=False,index_col=False)


# In[4]:


dateSecondHeaderList = ['Total_Individuals_Registered','Sessions','Sites','First_Dose_Administered',
                        'Second_Dose_Administered','Male_Individuals_Vaccinated','Female_Individuals_Vaccinated',
                        'Transgender_Individuals_Vaccinated','Covaxin_Doses_Administered','CoviShield_Doses_Administered']


# In[ ]:





# In[5]:


# get district to state dictionary
with np.load("Data/distToState.npz",allow_pickle=True) as data:
    distToState = data['arr_0'].reshape(1)[0] 

# get all states
states = vaccineData['State_Code'].unique()    
states.sort()  
districts =  list(distToState.keys())
districts.sort()


# In[7]:


vaccineData.head(2)


# In[6]:


# add missing dates from 15/03/2020 to 15/01/2021 and remove dates after 14/08/2021
missingDates = []

startdate = date(2020, 3, 15)
for i in range(0,307):
    day = startdate + timedelta(i)
    missingDates.append(day.strftime("%d/%m/%Y"))
    
df = pd.DataFrame(index = range(0,719))    
for date in missingDates:
    df['{}'.format(date)]   = 0
    df['{}.1'.format(date)] = 0
    df['{}.2'.format(date)] = 0
    df['{}.3'.format(date)] = 0
    df['{}.4'.format(date)] = 0
    df['{}.5'.format(date)] = 0
    df['{}.6'.format(date)] = 0
    df['{}.7'.format(date)] = 0
    df['{}.8'.format(date)] = 0
    df['{}.9'.format(date)] = 0
    
vaccineData= pd.concat([vaccineData,df],axis=1)
cols = vaccineData.columns.tolist()
cols = cols[0:6] + cols[2896:] + cols[6:2896] 
cols = cols[0:5186]
vaccineData = vaccineData[cols]


# In[8]:


# get required districts
index = []
for dist in districts:
    i = vaccineData[ vaccineData['District_Key'] == dist].index
    index.extend(i)

notDistIndex = np.setdiff1d(vaccineData.index,index)
vaccineData = vaccineData.drop(notDistIndex, inplace = False)
vaccineData.sort_values(by = 'District_Key', inplace = True,ignore_index=True)
vaccineData = vaccineData.drop(['S_No'],axis=1)


# In[9]:


## funtion get perticular type of data for each day -- var 'at' based on 'dateSecondHeaderList' list defined above 
def getDailyData(data,at):
    List = []
    for i in range(0,len(data),10):
        List.append(data[i+at])
    return List

## Funtion to correct error in cummlative data
### if error present it replace day_i data with day_i+1 data
def getErrorCorrected(data):
    for i in range(len(data)-1,-1,-1):
        if  ( i !=0 ) & (data[i] < data[i-1]):
            data[i-1] = data[i]
    return data        


## Funtion to get non cummlative data
def getNonCumulativeData(data):
    List = [0] * len(data)
    List[0] = data[0]
    for i in range(1,len(data)):
        List[i] = data[i] - data[i-1]
    return List 

## funtion to convert dayly data to weekly data
def getWeeklyData(data):
    List = [0] * 74
    data = np.array(data)
    data = np.array_split(data,74)
    for i in range(0,74):
        List[i] = data[i].sum()
    return np.array(List) 

## Funtion to convert dayly data to monthly data
def getMonthlyData(data):
    monthDays = [31,30,31,30,31,31,30,31,30,31,31,28,31,30,31,30,31]
    List = [0]*17
    pt = 0
    for i, j in zip(range(0,17), monthDays ):
        Sum = 0
        for k in range(j):
            Sum = Sum + data[pt+k]
        pt=pt+j
        List[i] = Sum
        
    return np.array(List)    


# In[12]:


# dataframe for district data
district_vaccinated_count_week = pd.DataFrame(data=None, index=None,columns=['districtid','timeid','dose1','dose2'])
district_vaccinated_count_month = pd.DataFrame(data=None, index=None,columns=['districtid','timeid','dose1','dose2'])
district_vaccinated_count_overall = pd.DataFrame(data=None, index=None,columns=['districtid','timeid','dose1','dose2'])

# dataframe for state data
state_vaccinated_count_week = pd.DataFrame(data=None, index=None,columns=['stateid','timeid','dose1','dose2'])
state_vaccinated_count_month = pd.DataFrame(data=None, index=None,columns=['stateid','timeid','dose1','dose2'])
state_vaccinated_count_overall = pd.DataFrame(data=None, index=None,columns=['stateid','timeid','dose1','dose2'])


# calculate dose data

for state in states:
    stateDose1Week = np.array([0]*74)
    stateDose2Week = np.array([0]*74)
    stateDose1Month = np.array([0]*17)
    stateDose2Month = np.array([0]*17)
    stateDose1OverAll = 0
    stateDose2OverAll = 0
    
    #get data for state 
    index = vaccineData[ vaccineData['State_Code'] == state].index
    notStateIndex = np.setdiff1d(vaccineData.index,index)
    stateWiseData = vaccineData.drop(notStateIndex, inplace = False)
    stateWiseData.sort_values(by = 'District_Key', inplace = True,ignore_index=True)
    districsInState = stateWiseData.District_Key.unique()
    
    ### for every district in state 
    for dist in districsInState:
        index = stateWiseData[ stateWiseData['District_Key'] == dist].index
        notStateIndex = np.setdiff1d(stateWiseData.index,index)
        distWiseData = stateWiseData.drop(notStateIndex, inplace = False)
        distWiseData.sort_values(by = 'District_Key', inplace = True,ignore_index=True)
        
        alldata = distWiseData.values.tolist()[0][5:]
        
        nonCumulativeDose1Daily = getNonCumulativeData( getErrorCorrected( getDailyData(alldata,3) ) )
        nonCumulativeDose2Daily = getNonCumulativeData( getErrorCorrected( getDailyData(alldata,4) ) )
        
        #week
        dose1week = getWeeklyData( nonCumulativeDose1Daily )
        dose2week = getWeeklyData( nonCumulativeDose2Daily )
        stateDose1Week = stateDose1Week + dose1week
        stateDose2Week = stateDose2Week + dose2week
        
        #month
        dose1month = getMonthlyData( nonCumulativeDose1Daily )
        dose2month = getMonthlyData( nonCumulativeDose2Daily )
        stateDose1Month = stateDose1Month + dose1month
        stateDose2Month = stateDose2Month + dose2month
        
        #overall
        dose1Overall = np.array(dose1month).sum()
        dose2Overall = np.array(dose2month).sum()
        stateDose1OverAll = stateDose1OverAll + dose1Overall
        stateDose2OverAll = stateDose2OverAll + dose2Overall
        
        #save district data
        df_week = pd.DataFrame(data=None, index=None,columns=['districtid','timeid','dose1','dose2'])
        df_month = pd.DataFrame(data=None, index=None,columns=['districtid','timeid','dose1','dose2'])
        df_overall = pd.DataFrame(data=None, index=None,columns=['districtid','timeid','dose1','dose2'])
        
        df_week['districtid'] = [dist]*74
        df_week['timeid'] = range(1,75)
        df_week['dose1'] = dose1week
        df_week['dose2'] = dose2week
    
        df_month['districtid'] = [dist]*17
        df_month['timeid'] = range(1,18)
        df_month['dose1'] = dose1month
        df_month['dose2'] = dose2month
    
        df_overall['districtid'] = [dist]*1
        df_overall['timeid'] = 1
        df_overall['dose1'] = dose1Overall
        df_overall['dose2'] = dose2Overall
        
        district_vaccinated_count_week   = pd.concat([district_vaccinated_count_week, df_week], ignore_index = True)
        district_vaccinated_count_month  = pd.concat([district_vaccinated_count_month, df_month], ignore_index = True)
        district_vaccinated_count_overall= pd.concat([district_vaccinated_count_overall, df_overall], ignore_index = True)
        
    s_week = pd.DataFrame(data=None, index=None,columns=['stateid','timeid','dose1','dose2'])
    s_month = pd.DataFrame(data=None, index=None,columns=['stateid','timeid','dose1','dose2'])
    s_overall = pd.DataFrame(data=None, index=None,columns=['stateid','timeid','dose1','dose2'])
    
    s_week['stateid'] = [state]*74
    s_week['timeid'] = range(1,75)
    s_week['dose1'] = stateDose1Week
    s_week['dose2'] = stateDose2Week
    
    s_month['stateid'] = [state]*17
    s_month['timeid'] = range(1,18)
    s_month['dose1'] = stateDose1Month
    s_month['dose2'] = stateDose2Month
    
    s_overall['stateid'] = [state]*1
    s_overall['timeid'] = 1
    s_overall['dose1'] = stateDose1OverAll
    s_overall['dose2'] = stateDose2OverAll
        


# In[13]:


#save file
district_vaccinated_count_week.to_csv("Data/district_vaccinated_count_week.csv",index=False)
district_vaccinated_count_month.to_csv("Data/district_vaccinated_count_month.csv",index=False)
district_vaccinated_count_overall.to_csv("Data/district_vaccinated_count_overall.csv",index=False)


state_vaccinated_count_week.to_csv("Data/state_vaccinated_count_week.csv",index=False)
state_vaccinated_count_month.to_csv("Data/state_vaccinated_count_month.csv",index=False)
state_vaccinated_count_overall.to_csv("Data/state_vaccinated_count_overall.csv",index=False)

# save vaccineData with new added dates
vaccineData.to_csv("Data/vaccineData-modified-v2.csv",index=False)


# In[ ]:





# In[ ]:





# In[ ]:




