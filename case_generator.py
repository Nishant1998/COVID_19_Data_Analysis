#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import pickle


# In[2]:


# import covid data
covidData = pd.read_csv("Data/districts-modified.csv",low_memory=False,index_col=False)


# In[3]:


def updateNewConfirmed(List):
    ini = List[0]
    for i in range(0,len(List)):
        if List[i] == 0:
            List[i] = ini
        else:
            ini = List[i]
    return List    

def getStartingDate():
    List = []
    for i in range(15,32):List.append('2020-03-'+str(i))
    for i in range(1,26):
        if i < 10:
            List.append('2020-04-0'+str(i))
        else:
             List.append('2020-04-'+str(i))
    return List
    
    


# In[4]:


#get all date in covid data
date = covidData['Date'].unique()


# In[5]:


# remove date after 14 Aug 2021

dateRemove = date[476:]               # Make list of date after 14 Aug 2021
date = date[:476]                     # date before 14 Aug 2021
startingDate = getStartingDate()      # get date from 15 March
# remove data after 14 Aug 2021
for day in dateRemove:
    covidData.drop(covidData[ covidData['Date'] == day].index, inplace = True)


# In[6]:


# add new column for cases Confirmed Today
covidData['Number_Of_Cases'] = 0


# In[8]:


districts = covidData['District_Key'].unique()
districts.sort()
np.savez("Data/districts", districts)


# In[9]:


# modify data with missing dates and calculate number of cases at that dates
for district in districts:
    #get district wise table
    index = covidData[ covidData['District_Key'] == district].index
    notDistIndex = np.setdiff1d(covidData.index,index)
    districWiseData = covidData.drop(notDistIndex, inplace = False)



    # Change Cases
    initialCase = districWiseData.loc[index[0],'Confirmed']
    districWiseData.loc[index[0],'Number_Of_Cases'] = initialCase

    for i in range(1,len(index)):
        a = districWiseData.loc[index[i-1],'Confirmed']
        b = districWiseData.loc[index[i],'Confirmed']
        districWiseData.loc[index[i],'Number_Of_Cases'] = b-a

    # Find missing dates
    presentDate = districWiseData['Date']
    missingDate = np.setdiff1d(date,presentDate)

    # get default value

    sc = districWiseData.loc[index[0],'State_Code']
    s  = districWiseData.loc[index[0],'State']
    dk = districWiseData.loc[index[0],'District_Key']
    d  = districWiseData.loc[index[0],'District']

    newRow = {'Date' :'0000-00-00','State_Code':sc,'State':s, 'District_Key':dk, 'District':d, 
          'Confirmed':0,'Recovered':0,'Deceased':0,'Other':0,'Tested':0,'Number_Of_Cases':0}

    for i in missingDate:
        newRow['Date'] = i
        districWiseData = districWiseData.append(newRow, ignore_index = True)
    
    #sort dataframe by date
    districWiseData.sort_values(by = 'Date', inplace = True,ignore_index=True)

    newConf = updateNewConfirmed(list(districWiseData['Confirmed']))
    districWiseData['Confirmed'] = newConf

    #add strting dates
    for i in startingDate:
        newRow['Date'] = i
        districWiseData = districWiseData.append(newRow, ignore_index = True)
    
    #sort dataframe by date
    districWiseData.sort_values(by = 'Date', inplace = True,ignore_index=True) 

    # remove original data and all new modified data
    covidData.drop(index, inplace = True)
    covidData = pd.concat([covidData, districWiseData], ignore_index = True)


# In[ ]:





# In[ ]:





# In[11]:


# Analysis week/month/overall for all districts
cases_week = pd.DataFrame(data=None, index=None,columns=['districtid','timeid','cases'])
cases_month = pd.DataFrame(data=None, index=None,columns=['districtid','timeid','cases'])
cases_overall = pd.DataFrame(data=None, index=None,columns=['districtid','timeid','cases'])


for district in districts:
    #get district wise table
    index = covidData[ covidData['District_Key'] == district].index
    notDistIndex = np.setdiff1d(covidData.index,index)
    districWiseData = covidData.drop(notDistIndex, inplace = False)

    #sort table by date
    districWiseData.sort_values(by = 'Date', inplace = True,ignore_index=True)

    #
    week = [0]*74
    month = [0]*17
    overall = 0
    #extract day wise case
    dayWiseCase = list(districWiseData['Number_Of_Cases'])
    monthDays = [31,30,31,30,31,31,30,31,30,31,31,28,31,30,31,30,31]
    
    #for i in range(0,len(dayWiseCase),7):
    for i, j in zip(range(0,len(week)), range(0,len(dayWiseCase),7) ):
        case_sum = 0
        for k in range(7):
            case_sum = case_sum + dayWiseCase[j+k]
        week[i] = case_sum  
    
    
    pt = 0
    for i, j in zip(range(0,len(month)), monthDays ):
        case_sum = 0
        for k in range(j):
            case_sum = case_sum + dayWiseCase[pt+k]
        pt=pt+j
        month[i] = case_sum  
   
    overall = np.array(dayWiseCase).sum()
    
    # make table
    df_week = pd.DataFrame(data=None, index=None,columns=['districtid','timeid','cases'])
    df_month = pd.DataFrame(data=None, index=None,columns=['districtid','timeid','cases'])
    df_overall = pd.DataFrame(data=None, index=None,columns=['districtid','timeid','cases'])
    
    df_week['districtid'] = [district]*74
    df_week['timeid'] = range(1,75)
    df_week['cases'] = week
    
    df_month['districtid'] = [district]*17
    df_month['timeid'] = range(1,18)
    df_month['cases'] = month
    
    df_overall['districtid'] = [district]*1
    df_overall['timeid'] = 1
    df_overall['cases'] = overall
    
    cases_week = pd.concat([cases_week, df_week], ignore_index = True)
    cases_month = pd.concat([cases_month, df_month], ignore_index = True)
    cases_overall = pd.concat([cases_overall, df_overall], ignore_index = True)
        


# In[12]:


# Save File
cases_week.to_csv("Data/cases_week.csv",index=False)
cases_month.to_csv("Data/cases_month.csv",index=False)
cases_overall.to_csv("Data/cases_overall.csv",index=False)
covidData.to_csv("Data/districts-modified-v2.csv",index=False)
np.savez("Data/distToState", dict(zip(covidData['District_Key'],covidData['State_Code'])))


# In[13]:


with open("Data/distToDistKey.pkl", "wb") as file:
    pickle.dump(dict(zip(covidData['District'],covidData['District_Key'])), file)


# In[ ]:




