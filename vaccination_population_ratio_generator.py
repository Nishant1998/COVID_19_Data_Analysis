#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandasql as sql
import pandas as pd
import pickle


# In[2]:


#load census data
data = pd.read_csv("Data/CensusData.csv",low_memory=False,index_col=False)
censusData = data.copy()

# load vaccine data
vaccineData = pd.read_csv("Data/vaccineData-modified-v2.csv",low_memory=False,index_col=False)


# In[3]:


# load district data
with np.load("Data/districts.npz",allow_pickle=True) as dictdata:
    districts = dictdata['arr_0']         


# In[4]:


# dictionary for district name to district keys
file = open("Data/distToDistKey.pkl", "rb")
dtdk = pickle.load(file)  
# dictionary for district name to key according to name in census data 
file = open("Data/censusNameToKey.pkl", "rb")
ndtdk1 = pickle.load(file)


# In[5]:


''' Preprocessing census data '''


# In[6]:


dateSecondHeaderList = ['Total_Individuals_Registered','Sessions','Sites','First_Dose_Administered',
                        'Second_Dose_Administered','Male_Individuals_Vaccinated','Female_Individuals_Vaccinated',
                        'Transgender_Individuals_Vaccinated','Covaxin_Doses_Administered','CoviShield_Doses_Administered']


# In[7]:


## find duplicate district name in census data
d = data[['District','Level','Name']]
d2 = d[ d['District'] != 0]
d2 = d2.drop_duplicates()


# In[8]:


d2[d2.Name.duplicated()] # duplicat at (90,531) (131,546) (96,1284) (735,1626) (403,1641) (1317,1758) 
# Hamirpur,Pratapgarh, Bilaspur, Aurangabad, Raigarh, Bijapur district have same name two state


# In[9]:


# getting respective district keys for it name
names = np.array(d2.Name)
code = [""]*640


# In[10]:


for i in range(0,640):
    name =  names[i]
    if type(dtdk.get(name)) != type(dtdk.get('NoneType')):
        code[i] = dtdk.get(name)
    
        
for i in range(0,640):
    name =  names[i]
    if type(ndtdk1.get(name)) != type(ndtdk1.get('NoneType')):
        code[i] = ndtdk1.get(name)  
       


# In[11]:


# duplicat at (27,167) (130,172) (29,405) (234,514) (402,519) (416,556)  correct duplicate
# correct duplicat distric name ith correct name
code[27] = 'HP_Hamirpur'
code[130] = 'RJ_Pratapgarh'
code[405] = 'CT_Bilaspur'
code[234] = 'BR_Aurangabad'
code[519] = 'MH_Raigad'
code[556] = ''


# In[12]:


len(np.setdiff1d(districts,np.array(code))) # 61 district are missing from census data, which are in intersection made on quetion 1


# In[ ]:





# In[13]:


# Add new column Keys
## new clumn represent state and district keys
keys = ['']*len(censusData)
censusData.insert(0, "Keys", keys, True)


# In[14]:


## state key in keys column
stateCode = ['JK','HP','PB','CH','UT','HR','DL','RJ','UP','BR','','AR','NL','MN','MZ','TR','ML','AS','WB','JH','OR','CT','MP','GJ','DN','DN','MH','AP','KA','','LD','KL','TN','PY','']
for i, row in censusData.iterrows():
    distId = row[2]
    key = code[distId-1]
    if distId == 0:
        stateId = row[1]
        if stateId == 0:
            censusData.at[i,'Keys'] = 'IN'
        else:
            censusData.at[i,'Keys'] = stateCode[stateId-1]
    else:    
        censusData.at[i,'Keys'] = str(key)


# In[15]:


# combine mumbai and mumbai suburban
col = censusData.columns.to_list()
for i in range(10,95):
    a = censusData.iloc[1638][i]
    b = censusData.iloc[1635][i]
    censusData.at[1638,col[i]] = a+b


# In[16]:


# combine delhi
for i in range(1,95):
    a = censusData.iloc[288][i]
    censusData.at[291,col[i]] = a
censusData.at[291,'Keys'] = 'DL_Delhi'  
censusData.at[291,'Level'] = 'DISTRICT' 


# In[17]:


# seprate J&K and Ladakh
for i in range(10,65):
    a = censusData.iloc[12][i]
    b = censusData.iloc[13][i]
    c = censusData.iloc[3][i]
    censusData.at[3,col[i]] = c-(a+b)
    censusData.at[11,col[i]] = a+b
censusData.at[11,col[0]] = 'LA'
censusData.at[11,col[7]] = 'STATE'
censusData.at[11,col[8]] = 'Ladakh'
censusData.at[11,col[9]] = 'Total'


# In[18]:


# delete distric which are not in vaccine and covid data
# delete rural and urban data only keep total data for district.
for i, row in censusData.iterrows():
    if row[0] == '':
        censusData.at[i,col[0]] = np.nan
    if row[9] == 'Rural' or row[9] == 'Urban':
        censusData.at[i,col[9]] = np.nan


# In[19]:


#drop unwanted rows 
censusData = censusData.dropna()
#drop columns
censusData.drop(censusData.iloc[:, 17:], inplace = True, axis = 1)
censusData = censusData[['Keys', 'Level', 'Name', 'No_HH', 'TOT_P', 'TOT_M', 'TOT_F', 'P_06', 'M_06', 'F_06']]


# In[20]:


# correct key
censusData.at[1506,col[0]] = 'GJ_Porbandar'


# In[21]:


states = sql.sqldf("SELECT DISTINCT Keys FROM censusData WHERE Level = 'STATE' ", globals()).values.T[0]
censusDistrict = sql.sqldf("SELECT DISTINCT Keys FROM censusData WHERE Level = 'DISTRICT' ", globals()).values.T[0]
print("Number of state:{}".format(len(states)))
print("Number of district:{}".format(len(censusDistrict)))


# In[22]:


censusData.to_csv('Data/CensusData_modified.csv',index=False)


# In[ ]:





# #  Analysis data to find Ratios

# In[23]:


# Some Funtion
## funtion to get specific data for daily  
def getDailyData(data,at):
    List = []
    for i in range(0,len(data),10):
        List.append(data[i+at])
    return List

def ratio(a,b):
    return (a/b) 


# In[24]:


## Get District in vaccination data
vaccDistrict = vaccineData.District_Key.unique()


# In[25]:


# dataframe for ratio data 
district_vaccination_population_ratio = pd.DataFrame(data=None, index=None,columns=['districtid', 'vaccinationratio', 'populationratio', 'ratioofratios'])
state_vaccination_population_ratio = pd.DataFrame(data=None, index=None,columns=['stateid', 'vaccinationratio', 'populationratio', 'ratioofratios'])
overall_vaccination_population_ratio = pd.DataFrame(data=None, index=None,columns=['id', 'vaccinationratio', 'populationratio', 'ratioofratios'])


# In[26]:





## Variable for overall analysis
overall_MaleVaccinated = 0
overall_FemaleVaccinated = 0

overall_totalMale = 0
overall_totalFemale = 0


# In[27]:


# for every states
for state in states:
    
    # Variable for state wise analysis
    state_MaleVaccinated = 0
    state_FemaleVaccinated = 0
    
    state_totalMale = 0
    state_totalFemale = 0

    
    #get data for state 
    index = vaccineData[ vaccineData['State_Code'] == state].index
    notStateIndex = np.setdiff1d(vaccineData.index,index)
    stateWiseData = vaccineData.drop(notStateIndex, inplace = False)
    stateWiseData.sort_values(by = 'District_Key', inplace = True,ignore_index=True)
    districsInState = stateWiseData.District_Key.unique()
    
    for dist in districsInState:
        #check if district is present in population data. If not we ignore that district
        if dist in censusDistrict:
            MaleVaccinated = 0
            FemaleVaccinated = 0
            
            index = stateWiseData[ stateWiseData['District_Key'] == dist].index
            notStateIndex = np.setdiff1d(stateWiseData.index,index)
            distWiseData = stateWiseData.drop(notStateIndex, inplace = False)
            distWiseData.sort_values(by = 'District_Key', inplace = True,ignore_index=True)
        
            alldata = distWiseData.values.tolist()[0][5:]
    
            # get data from dataframe
            ## As data is cumulative take last date data for vaccination data
            MaleVaccinated = getDailyData(alldata,5)[-1]
            FemaleVaccinated = getDailyData(alldata,6)[-1]
            totalVaccinated = MaleVaccinated + FemaleVaccinated
            
            ## get population data for district
            totalMale = censusData.loc[censusData['Keys'] == dist].TOT_M.iloc[0]
            totalFemale = censusData.loc[censusData['Keys'] == dist].TOT_F.iloc[0]
            totalPopulation = totalMale + totalFemale
            
            # find Ratio for dictict
            vaccinationRatio = ratio(FemaleVaccinated,MaleVaccinated)
            populationRatio = ratio(totalFemale,totalMale)
            vaccToPopRatio = ratio(vaccinationRatio,populationRatio)
            
            # Add district data to dataframe
            distDf = pd.DataFrame(data=None, index=None,columns=['districtid', 'vaccinationratio', 'populationratio', 'ratioofratios'])
            distDf.loc[0] = [dist,vaccinationRatio ,populationRatio,vaccToPopRatio]
            district_vaccination_population_ratio = pd.concat([district_vaccination_population_ratio, distDf], ignore_index = True)
            
            # Find total vaccinated in state
            state_MaleVaccinated = state_MaleVaccinated + MaleVaccinated
            state_FemaleVaccinated = state_FemaleVaccinated + FemaleVaccinated
            state_TotalVaccinated = state_MaleVaccinated + state_FemaleVaccinated
            
            # Find Total Population in state
            state_totalMale = state_totalMale + totalMale
            state_totalFemale = state_totalFemale + totalFemale
            
            
    # State        
    ## get population data for state
    totalMale = state_totalMale
    totalFemale = state_totalFemale
    totalPopulation = totalMale + totalFemale
    
    # find Ratio for state
    vaccinationRatio = ratio(state_FemaleVaccinated,state_MaleVaccinated)
    populationRatio = ratio(totalFemale,totalMale)
    vaccToPopRatio = ratio(vaccinationRatio,populationRatio)
    
    # Add state data to dataframe
    stateDf = pd.DataFrame(data=None, index=None,columns=['stateid', 'vaccinationratio', 'populationratio', 'ratioofratios'])
    stateDf.loc[0] = [state,vaccinationRatio ,populationRatio,vaccToPopRatio]
    state_vaccination_population_ratio = pd.concat([state_vaccination_population_ratio, stateDf], ignore_index = True)
    
    # Find total vaccinated overall
    overall_MaleVaccinated = overall_MaleVaccinated + state_MaleVaccinated
    overall_FemaleVaccinated = overall_FemaleVaccinated + state_FemaleVaccinated
    overall_TotalVaccinated = overall_MaleVaccinated + overall_FemaleVaccinated
            
    # Find Total Population overall
    overall_totalMale = overall_totalMale + state_totalMale
    overall_totalFemale = overall_totalFemale + state_totalFemale
    
    
            
            


# In[28]:


# overall
## get population data for state
totalMale = overall_totalMale
totalFemale = overall_totalFemale
totalPopulation = totalMale + totalFemale
    
# find Ratio for state
vaccinationRatio = ratio(overall_FemaleVaccinated,overall_MaleVaccinated)
populationRatio = ratio(totalFemale,totalMale)
vaccToPopRatio = ratio(vaccinationRatio,populationRatio)

# add data in dataframe
overall_vaccination_population_ratio.loc[0] = ['IN',vaccinationRatio ,populationRatio,vaccToPopRatio]


# In[29]:


# sort by final ratio
district_vaccination_population_ratio.sort_values(by = 'ratioofratios', inplace = True,ignore_index=True)
state_vaccination_population_ratio.sort_values(by = 'ratioofratios', inplace = True,ignore_index=True)
overall_vaccination_population_ratio.sort_values(by = 'ratioofratios', inplace = True,ignore_index=True)


# In[30]:


# Save File
district_vaccination_population_ratio.to_csv("Data/district_vaccination_population_ratio.csv",index=False)
state_vaccination_population_ratio.to_csv("Data/state_vaccination_population_ratio.csv",index=False)
overall_vaccination_population_ratio.to_csv("Data/overall_vaccination_population_ratio.csv",index=False)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




