#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandasql as sql
import pandas as pd
import pickle


# In[2]:


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


# In[14]:


# Some Funtion
def getDailyData(data,at):
    List = []
    for i in range(0,len(data),10):
        List.append(data[i+at])
    return List

def ratio(a,b):
    return (a/b) 


# In[28]:


# dataframe for ratio data 
district_vaccinated_dose_ratio = pd.DataFrame(data=None, index=None,columns=['districtid', 'vaccinated_dose1_ratio', 'vaccinated_dose2_ratio'])
state_vaccinated_dose_ratio = pd.DataFrame(data=None, index=None,columns=['stateid', 'vaccinated_dose1_ratio', 'vaccinated_dose2_ratio'])
overall_vaccinated_dose_ratio = pd.DataFrame(data=None, index=None,columns=['id', 'vaccinated_dose1_ratio', 'vaccinated_dose2_ratio'])


# In[29]:



''' Finding Ratio of  dose1:population & dose2:population '''
overall_Dose1 = 0
overall_Dose2 = 0
overall_Population = 0


# In[30]:


for state in states:
    
    state_Dose1 = 0
    state_Dose2 = 0
    state_Population = 0
    
    #get data for state 
    index = vaccineData[ vaccineData['State_Code'] == state].index
    notStateIndex = np.setdiff1d(vaccineData.index,index)
    stateWiseData = vaccineData.drop(notStateIndex, inplace = False)
    stateWiseData.sort_values(by = 'District_Key', inplace = True,ignore_index=True)
    districsInState = stateWiseData.District_Key.unique()
    
    for dist in districsInState:
        #check if district is present in population data. If not we ignore that district
        if dist in censusDistrict:
            dose1 = 0
            dose2 = 0
            population = 0
            
            index = stateWiseData[ stateWiseData['District_Key'] == dist].index
            notStateIndex = np.setdiff1d(stateWiseData.index,index)
            distWiseData = stateWiseData.drop(notStateIndex, inplace = False)
            distWiseData.sort_values(by = 'District_Key', inplace = True,ignore_index=True)
        
            alldata = distWiseData.values.tolist()[0][5:]
            
            # get data from dataframe
            ## As data is cumulative take last date data for vaccination data
            dose1 = getDailyData(alldata,3)[-1]
            dose2 = getDailyData(alldata,4)[-1]
            population = censusData.loc[censusData['Keys'] == dist].TOT_P.iloc[0]
            
            # find Ratio for dictict
            vaccinated_dose1_ratio = ratio(dose1,population)
            vaccinated_dose2_ratio = ratio(dose2,population)
            
            # Add district data to dataframe
            distDf = pd.DataFrame(data=None, index=None,columns=['districtid', 'vaccinated_dose1_ratio', 'vaccinated_dose2_ratio'])
            distDf.loc[0] = [dist,vaccinated_dose1_ratio ,vaccinated_dose2_ratio]
            district_vaccinated_dose_ratio = pd.concat([district_vaccinated_dose_ratio, distDf], ignore_index = True)
            
            # Find total dose and population for state
            state_Dose1 = state_Dose1 + dose1
            state_Dose2 = state_Dose2 + dose2
            state_Population = state_Population + population            
    
    # state
    ## find Ratio for state
    vaccinated_dose1_ratio = ratio(state_Dose1,state_Population)
    vaccinated_dose2_ratio = ratio(state_Dose2,state_Population)
    
    ## Add state data to dataframe
    stateDf = pd.DataFrame(data=None, index=None,columns=['stateid', 'vaccinated_dose1_ratio', 'vaccinated_dose2_ratio'])
    stateDf.loc[0] = [state,vaccinated_dose1_ratio ,vaccinated_dose2_ratio]
    state_vaccinated_dose_ratio = pd.concat([state_vaccinated_dose_ratio, stateDf], ignore_index = True)    
    
    # Find total dose and population for state
    overall_Dose1 = overall_Dose1 + state_Dose1
    overall_Dose2 = overall_Dose2 + state_Dose2
    overall_Population = overall_Population + state_Population        
 


# In[31]:


# overall

# find overall Ratio
vaccinated_dose1_ratio = ratio(overall_Dose1,overall_Population)
vaccinated_dose2_ratio = ratio(overall_Dose2,overall_Population)


# add data in dataframe
overall_vaccinated_dose_ratio.loc[0] = ['IN',vaccinated_dose1_ratio ,vaccinated_dose2_ratio]


# In[35]:


# sort by dose1 ratio
district_vaccinated_dose_ratio.sort_values(by = 'vaccinated_dose1_ratio', inplace = True,ignore_index=True)
state_vaccinated_dose_ratio.sort_values(by = 'vaccinated_dose1_ratio', inplace = True,ignore_index=True)
overall_vaccinated_dose_ratio.sort_values(by = 'vaccinated_dose1_ratio', inplace = True,ignore_index=True)


# In[40]:


# Save File
district_vaccinated_dose_ratio.to_csv("Data/district_vaccinated_dose_ratio.csv",index=False)
state_vaccinated_dose_ratio.to_csv("Data/state_vaccinated_dose_ratio.csv",index=False)
overall_vaccinated_dose_ratio.to_csv("Data/overall_vaccinated_dose_ratio.csv",index=False)


# In[ ]:




