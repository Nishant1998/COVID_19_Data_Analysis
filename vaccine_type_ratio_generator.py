#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandasql as sql
import pandas as pd
import pickle


# In[52]:


# Load Required Data

## load vaccine data
vaccineData = pd.read_csv("Data/vaccineData-modified-v2.csv",low_memory=False,index_col=False)

## load district name
with np.load("Data/districts.npz",allow_pickle=True) as dictdata:
    districts = dictdata['arr_0']
    
## load state name
states = vaccineData.State_Code.unique()


# In[53]:


# Some Funtion
def getDailyData(data,at):
    List = []
    for i in range(0,len(data),10):
        List.append(data[i+at])
    return List

def ratio(a,b):
    return (a/b) 


# In[54]:


# dataframe for ration data 
district_vaccine_type_ratio = pd.DataFrame(data=None, index=None,columns=['districtid', 'vaccine_ratio'])
state_vaccine_type_ratio = pd.DataFrame(data=None, index=None,columns=['stateid', 'vaccine_ratio'])
overall_vaccine_type_ratio = pd.DataFrame(data=None, index=None,columns=['id', 'vaccine_ratio'])


# In[55]:


''' Finding Ratio of  CoviShield:Covaxin '''
overall_CoviShield = 0
overall_Covaxin = 0


# In[56]:


for state in states:
    
    state_CoviShield = 0
    state_Covaxin = 0
    
    #get data for state 
    index = vaccineData[ vaccineData['State_Code'] == state].index
    notStateIndex = np.setdiff1d(vaccineData.index,index)
    stateWiseData = vaccineData.drop(notStateIndex, inplace = False)
    stateWiseData.sort_values(by = 'District_Key', inplace = True,ignore_index=True)
    districsInState = stateWiseData.District_Key.unique()
    
    for dist in districsInState:
        CoviShield = 0
        Covaxin = 0
        
        #get data for district
        index = stateWiseData[ stateWiseData['District_Key'] == dist].index
        notStateIndex = np.setdiff1d(stateWiseData.index,index)
        distWiseData = stateWiseData.drop(notStateIndex, inplace = False)
        distWiseData.sort_values(by = 'District_Key', inplace = True,ignore_index=True)
        
        alldata = distWiseData.values.tolist()[0][5:]
        
        # get data from dataframe
        ## As data is cumulative take last date data for vaccination data
        CoviShield = getDailyData(alldata,9)[-1]
        Covaxin = getDailyData(alldata,8)[-1]
        
        ### Find ration of to vaccine
        try:
            vacc_ratio = ratio(CoviShield,Covaxin)
        except Exception as e:
            if e.args[0] == 'division by zero':
                vacc_ratio = np.nan
                
        # Add district data to dataframe
        distDf = pd.DataFrame(data=None, index=None,columns=['districtid', 'vaccine_ratio'])
        distDf.loc[0] = [dist,vacc_ratio]
        district_vaccine_type_ratio = pd.concat([district_vaccine_type_ratio, distDf], ignore_index = True)     
        
        # Find total vaccine data in state
        state_CoviShield = state_CoviShield + CoviShield
        state_Covaxin = state_Covaxin + Covaxin
      
    # STATEs
    ## Find ration of to vaccine
    try:
        vacc_ratio = ratio(state_CoviShield,state_Covaxin)
    except Exception as e:
        if e.args[0] == 'division by zero':
            vacc_ratio = np.nan
            
    ## Add state data to dataframe
    stateDf = pd.DataFrame(data=None, index=None,columns=['stateid', 'vaccine_ratio'])
    stateDf.loc[0] = [state,vacc_ratio]
    state_vaccine_type_ratio = pd.concat([state_vaccine_type_ratio, stateDf], ignore_index = True)
    
    # Find overall total vaccine data
    overall_CoviShield = overall_CoviShield + state_CoviShield
    overall_Covaxin = overall_Covaxin + state_Covaxin
        


# In[57]:


# Overall

## Find ration of to vaccine
try:
    vacc_ratio = ratio(overall_CoviShield,overall_Covaxin)
except Exception as e:
    if e.args[0] == 'division by zero':
        vacc_ratio = np.nan
            
## Add state data to dataframe
Df = pd.DataFrame(data=None, index=None,columns=['id', 'vaccine_ratio'])
Df.loc[0] = ['IN',vacc_ratio]
overall_vaccine_type_ratio = pd.concat([overall_vaccine_type_ratio, Df], ignore_index = True)


# In[ ]:





# In[58]:


# sort by final ratio
district_vaccine_type_ratio.sort_values(by = 'vaccine_ratio', inplace = True,ignore_index=True)
state_vaccine_type_ratio.sort_values(by = 'vaccine_ratio', inplace = True,ignore_index=True)
overall_vaccine_type_ratio.sort_values(by = 'vaccine_ratio', inplace = True,ignore_index=True)


# In[61]:


# Add NA for empty ratio
district_vaccine_type_ratio.fillna('NA', inplace=True)
state_vaccine_type_ratio.fillna('NA', inplace=True)
overall_vaccine_type_ratio.fillna('NA', inplace=True)


# In[63]:


# Save File
district_vaccine_type_ratio.to_csv("Data/district_vaccine_type_ratio.csv",index=False)
state_vaccine_type_ratio.to_csv("Data/state_vaccine_type_ratio.csv",index=False)
overall_vaccine_type_ratio.to_csv("Data/overall_vaccine_type_ratio.csv",index=False)


# In[ ]:





# In[ ]:




