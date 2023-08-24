#!/usr/bin/env python
# coding: utf-8

# In[27]:


import numpy as np
import pandasql as sql
import pandas as pd
import pickle
import datetime


# In[3]:


# Load Required Data

## load vaccine data
vaccineData = pd.read_csv("Data/vaccineData-modified-v2.csv",low_memory=False,index_col=False)

## load census data
censusData = pd.read_csv("Data/CensusData_modified.csv",low_memory=False,index_col=False)

## load district name
### All districts
with np.load("Data/districts.npz",allow_pickle=True) as dictdata:
    districts = dictdata['arr_0']

### census districts    
censusDistrict = sql.sqldf("SELECT DISTINCT Keys FROM censusData WHERE Level = 'DISTRICT' ", globals()).values.T[0]    
    
## load state name
states = vaccineData.State_Code.unique()


# In[16]:


# Some Funtion
def getDailyData(data,at):
    List = []
    for i in range(0,len(data),10):
        List.append(data[i+at])
    return List

def getRate(data):
    return dose1[-1]-dose1[-7]


# In[52]:


# dataframe for ratio data 
complete_vaccination = pd.DataFrame(data=None, index=None,columns=['stateid', 'populationleft', 'rateofvaccination', 'date'])


# In[53]:


''' Finding Date Of complete vaccination'''
startDate = '14/08/2021'

for state in states:
    vaccinated = 0
    rate = 0
    population = 0
    
    #get data for state 
    index = vaccineData[ vaccineData['State_Code'] == state].index
    notStateIndex = np.setdiff1d(vaccineData.index,index)
    stateWiseData = vaccineData.drop(notStateIndex, inplace = False)
    stateWiseData.sort_values(by = 'District_Key', inplace = True,ignore_index=True)
    districsInState = stateWiseData.District_Key.unique()
    
    for dist in districsInState:
        #check if district is present in population data. If not we ignore that district
        if dist in censusDistrict:
            dict_vaccinated = 0
            dict_rate = 0
            dict_population = 0
            
            index = stateWiseData[ stateWiseData['District_Key'] == dist].index
            notStateIndex = np.setdiff1d(stateWiseData.index,index)
            distWiseData = stateWiseData.drop(notStateIndex, inplace = False)
            distWiseData.sort_values(by = 'District_Key', inplace = True,ignore_index=True)
        
            alldata = distWiseData.values.tolist()[0][5:]
            
            # get data from dataframe
            dose1 = getDailyData(alldata,3)
            dict_vaccinated = dose1[-1]
            dict_population = censusData.loc[censusData['Keys'] == dist].TOT_P.iloc[0]
            
            dict_rate = getRate(dose1)
            
            # calculate data for state
            vaccinated = vaccinated + dict_vaccinated       # total people vaccinated with dose1 
            rate = rate + dict_rate                         # rate of vaccination per week
            population = population + dict_population       # population of state ignoring district which are absent in 2011 census
  
    population_left = population - vaccinated
    numberOfWeekNeeded = population_left/rate
    
    #Calculate date for complete vaccination
    d = datetime.datetime.strptime(startDate, "%d/%m/%Y")
    endDate = d + datetime.timedelta(weeks=numberOfWeekNeeded)
    date = endDate.strftime('%d/%m/%Y')
    
    ## Add state data to dataframe
    stateDf = pd.DataFrame(data=None, index=None,columns=['stateid', 'populationleft', 'rateofvaccination', 'date'])
    stateDf.loc[0] = [state,population_left ,rate,date]
    complete_vaccination = pd.concat([complete_vaccination, stateDf], ignore_index = True)    
    
    


# In[55]:


# Save File
complete_vaccination.to_csv("Data/complete_vaccination.csv",index=False)


# In[29]:





# In[ ]:





# In[ ]:





# In[ ]:




