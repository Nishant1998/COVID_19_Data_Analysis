#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import json
import re
import pandasql as sql
from collections import OrderedDict


# # Preprocessing Vaccination Data

# In[2]:


## load vaccineData
vaccineData = pd.read_csv("Data/cowin_vaccine_data_districtwise.csv",low_memory=False,index_col=False)


# In[3]:


## sub Header of vaccination every day data
dateSecondHeaderList = ['Total_Individuals_Registered','Sessions','Sites','First_Dose_Administered',
                        'Second_Dose_Administered','Male_Individuals_Vaccinated','Female_Individuals_Vaccinated',
                        'Transgender_Individuals_Vaccinated','Covaxin_Doses_Administered','CoviShield_Doses_Administered']


# In[4]:


## drop sub header
vaccineData =  vaccineData.drop([0])
vaccineData.rename(columns = {'Cowin Key':'Cowin_Key', 'S No':'S_No'}, inplace = True)


# In[5]:


#print("Vaccination Data.")
#print("Number of states : {}".format(len(vaccineData.State_Code.unique())))
#print("Number of unique districts : {}".format(len(vaccineData['District_Key'].unique())))
#print("Number of districts : {}".format(len(vaccineData.District_Key)))
duplicatDistricts = vaccineData[vaccineData['District_Key'].duplicated()]['District_Key'].tolist()
#print("Number of duplicated districts : {}".format(len(duplicatDistricts)))
'''
Vaccination Data.
Number of states : 36
Number of unique districts : 729
Number of districts : 754
Number of duplicated districts : 25

'''

# In[6]:


# Replace NAN value with zero
vaccineData = vaccineData.fillna(0)


# In[7]:


# Merge duplicate district
for district in duplicatDistricts:
    npa1 = np.array(vaccineData.loc[ vaccineData['District_Key'] == district])
    npa1[0][6:] = npa1[0][6:].astype(int) + npa1[1][6:].astype(int) # mearge data
    

    # drop original data
    vaccineData = vaccineData[ vaccineData.District_Key !=  district]

    #reset index
    a = vaccineData.set_index('S_No',drop=True)
    b = a.reset_index()
    c = b.drop(['S_No'], axis = 1)
    c.insert(loc=0, column='S_No', value=list(range(0,len(c))))   # add S_No column at start
    npa1[0][0] = len(c.index)
    c.loc[len(c.index)] = npa1[0] 
    vaccineData = c


# In[8]:



#print("After removing duplicate district:")
#print("Number of states : {}".format(len(vaccineData.State_Code.unique())))
#print("Number of unique districts : {}".format(len(vaccineData['District_Key'].unique())))
#print("Number of districts : {}".format(len(vaccineData.District_Key)))
duplicatDistricts = vaccineData[vaccineData['District_Key'].duplicated()]['District_Key'].tolist()
#print("Number of duplicated districts : {}".format(len(duplicatDistricts)))
'''
After removing duplicate district:
Number of states : 36
Number of unique districts : 729
Number of districts : 729
Number of duplicated districts : 0
'''

# # Preprocessing Covid Data

# In[9]:


## Load covid data
covidData = pd.read_csv("Data/districts.csv",low_memory=False,index_col=False)


# In[10]:


## insert new column for State_code and District_keys
covidData.insert(loc=1, column='State_Code', value='')
covidData.insert(loc=3, column='District_Key', value='')


# In[11]:

'''
print("Covid Data. ")
print("Number of states : {}".format(len(covidData.State.unique())))
print("Number of districts : {}".format(  len(covidData.District.unique())))
Covid Data. 
Number of states : 36
Number of districts : 643
'''

# In[12]:


## make dictionary
### State to State Code Distionary
stateCodeDict = dict(zip(vaccineData.State.unique(), vaccineData.State_Code.unique()))
### District Keys to District  Distionary
districtKeyDict = dict(zip(vaccineData.District_Key.unique(),vaccineData.District))

## Update state code in table
for i in stateCodeDict:
    covidData.loc[covidData['State'] == i, 'State_Code'] = stateCodeDict.get(i)

## update distric key in table
for i in districtKeyDict:
    dist = districtKeyDict.get(i)
    covidData.loc[covidData['District'] == dist, 'District_Key'] = i


# In[13]:


## Some correction for district with same name
correction = [['CT','HP_Bilaspur','CT_Bilaspur'],['BR'	,'MH_Aurangabad','BR_Aurangabad'],['CT'	,'UP_Balrampur','CT_Balrampur'],['HP'	,'UP_Hamirpur','HP_Hamirpur'],['RJ','UP_Pratapgarh','RJ_Pratapgarh']]

for i in range(len(covidData)):
    for j in range(0,5):
        code = correction[j][0]
        key = correction[j][1]
        update = correction[j][2]
        if (covidData.loc[i, "State_Code"] == code) & (covidData.loc[i, "District_Key"] == key):
            covidData.loc[i, "District_Key"] = update
            break
        


# In[14]:


## Find Distict with no district key assign
result = sql.sqldf("SELECT DISTINCT District FROM covidData WHERE District_Key = '' ", globals())

## Delhi is one district in covid data. Rename delhi code to DL_Delhi
covidData.loc[covidData['District'] == 'Delhi', 'District_Key'] = 'DL_Delhi'


# In[15]:


## Delhi is one district in covid data
## Merge 11 parts Delhi in vaccination data to one deli district
npa1 = np.array(vaccineData.loc[ vaccineData['State_Code'] == 'DL'])
count = vaccineData.loc[ vaccineData['State_Code'] == 'DL'].shape[0]
npa1[0][6:] = npa1[0][6:].astype(int)
for i in range(1,count):
    npa1[0][6:] += npa1[i][6:].astype(int)# mearge data

npa1[0][3]='DL_Delhi'
npa1[0][4]='Delhi'
npa1[0][5]='Delhi'

## drop original data
vaccineData = vaccineData[ vaccineData.State_Code !=  'DL']

## reset index
a = vaccineData.set_index('S_No',drop=True)
b = a.reset_index()
c = b.drop(['S_No'], axis = 1)
c.insert(loc=0, column='S_No', value=list(range(0,len(c))))   # add S_No column at start
npa1[0][0] = len(c.index)
c.loc[len(c.index)] = npa1[0] 
vaccineData = c


# In[17]:


## FIND Intersect Between Vaccination and Covid DISTRICTS

# In[19]:


#print("District intersection.")

vaccineDistric = vaccineData['District_Key'].unique()
covidDistric = covidData['District_Key'].unique()
districIntersect = np.intersect1d(vaccineDistric,covidDistric)
# "covidDistric - vaccineDistric" Set diffrence
districSetDiffrence =  np.setdiff1d(covidDistric,vaccineDistric)

#print("Total unique distric in vaccine data : {}".format(len( vaccineDistric )))
#print("Total unique distric in covid data : {}".format(len( covidDistric )))
#print("Total distric intersect : {}".format(len(districIntersect)))
#print("Total distric covidData but not in vaccineData: {}".format(len(np.setdiff1d(covidDistric,vaccineDistric))))
#print("Total distric vaccineData but not in covidData: {}".format(len(np.setdiff1d(vaccineDistric,covidDistric))))

'''
District intersect.
Total unique distric in vaccine data : 719
Total unique distric in covid data : 632
Total distric intersect : 631
Total distric covidData but not in vaccineData: 1
Total distric vaccineData but not in covidData: 88
'''

#36 unknown districts


# In[ ]:





# # Preprocessing Neighbor Districts json

# In[30]:


## load neighbor data
with open('Data/neighbor-districts.json') as json_data:
    data = json.load(json_data)


# In[31]:


neighborDictData  = data
neighborKey = neighborDictData.keys()
neighborKey = np.array(list(neighborKey))
#print("Number of District in Neighbor Data: {}".format(neighborKey.size))


# In[32]:


## remove Geo code and other
neighborModified = []
for i in range(neighborKey.size):
    name = re.sub("/Q[\d]+", "", neighborKey[i])    # removing geocode
    name = re.sub("_district", "", name)            # removing 'district' prefix
    name = re.sub("_", " ", name)                   # replace _ with space
    neighborModified.append(name)


# In[33]:


## seprate state code from district name from District Intersection
districWithoutState = []
for i in range(districIntersect.size):
    name = re.sub("\w{2}_", "", districIntersect[i])
    districWithoutState.append(name.lower())    


# In[34]:


### Making dictionary district name to district keys
dicDistricCode = dict(zip(districWithoutState, districIntersect))


# In[35]:


### Dictionary for name correction
correctionDict = {'lahul and spiti': 'Lahaul and Spiti' ,
 'sahibzada ajit singh nagar': 'S.A.S. Nagar',
 'the nilgiris': 'Nilgiris' ,
 'palghat': 'Palakkad' , 
 'chamarajanagar': 'Chamarajanagara',
 'kochbihar': 'Cooch Behar',
 'purba champaran': 'East Champaran',
 'pashchim champaran': 'West Champaran',
 'maldah': 'Malda',
 'marigaon': 'Morigaon',
 'ri-bhoi': 'Ribhoi',
 'jalor': 'Jalore',
 'banas kantha': 'Banaskantha',
 'rae bareilly': 'Rae Bareli',
 'tirunelveli kattabo': 'Tirunelveli',
 'thoothukudi': 'Thoothukkudi',
 'pattanamtitta': 'Pathanamthitta',
 'sivagangai': 'Sivaganga',
 'tiruchchirappalli': 'Tiruchirappalli',
 'tiruvanamalai': 'Tiruvannamalai',
 'kanchipuram': 'Kancheepuram',
 'sri potti sriramulu nellore': 'S.P.S. Nellore',
 'muktsar': 'Sri Muktsar Sahib',
 'jhunjhunun': 'Jhunjhunu',
 'rajauri': 'Rajouri',
 'badgam': 'Budgam',
 'baramula': 'Baramulla',
 'shopian': 'Shopiyan',
 'fategarh sahib': 'Fatehgarh Sahib',
 'shaheed bhagat singh nagar': 'Shahid Bhagat Singh Nagar',
 'firozpur': 'Ferozepur',
 'sri ganganagar': 'Ganganagar',
 'sabar kantha': 'Sabarkantha',
 'dhaulpur': 'Dholpur',
 'mahesana': 'Mehsana',
 'panch mahal': 'Panchmahal',
 'nandubar': 'Nandurbar',
 'nav sari': 'Navsari',
 'the dangs': 'Dang',
 'jyotiba phule nagar': 'Amroha',
 'kheri': 'Lakhimpur Kheri',
 'sharawasti': 'Shrawasti',
 'siddharth nagar': 'Siddharthnagar',
 'mahrajganj': 'Maharajganj',
 'sait kibir nagar': 'Sant Kabir Nagar', 
 'ashok nagar': 'Ashoknagar',
 'faizabad': 'Ayodhya',
 'sant ravidas nagar': 'Bhadohi',
 'kaimur (bhabua)': 'Kaimur',
 'kodarma': 'Koderma',
 'pakaur': 'Pakur',
 'hugli': 'Hooghly',
 'puruliya': 'Purulia',
 'purbi singhbhum': 'East Singhbhum',
 'seraikela kharsawan': 'Saraikela-Kharsawan',
 'baleshwar': 'Balasore',
 'pashchimi singhbhum': 'West Singhbhum',
 'janjgir-champa': 'Janjgir Champa',
 'bemetara': 'Bametara',
 'kabirdham': 'Kabeerdham',
 'gondiya': 'Gondia',
 'dantewada': 'Dakshin Bastar Dantewada',
 'narsimhapur': 'Narsinghpur',
 'sonapur': 'Subarnapur',
 'baudh': 'boudh',
 'anugul': 'Angul',
 'debagarh': 'Deogarh',
 'jajapur': 'Jajpur',
 'jagatsinghapur': 'Jagatsinghpur',
 'bid': 'beed',
 'komram bheem': 'Komaram Bheem',
 'belgaum': 'Belagavi',
 'bangalore rural': 'Bengaluru Rural',
 'bangalore urban': 'Bengaluru Urban',
 'tumkur': 'Tumakuru',
 'bellary': 'Ballari',
 'yadagiri': 'Yadgir',
 'medchal–malkajgiri': 'Medchal Malkajgiri',
 'rangareddy': 'Ranga Reddy',
 'ysr': 'Y.S.R. Kadapa',
 'south salmara-mankachar': 'South Salmara Mankachar',
 'east karbi anglong': 'Karbi Anglong',
 'aizwal': 'Aizawl',
 'bishwanath': 'Biswanath',
 'pakke kessang/None': 'Pakke Kessang',
 'sepahijala': 'Sipahijala',
 'devbhumi dwaraka': 'Devbhumi Dwarka'}


# In[36]:


#### make correction spelling
for i in range(0,len(neighborModified)):
    name = neighborModified[i]
    if name in correctionDict:
        correct_name = correctionDict.get(name)
        neighborModified[i] = correct_name.lower()
    


# In[37]:


## exctract geo code

geoCodeList = []    
neighborKey[643] = 'pakke_kessang/None/Q0'
geoCodePattern = r'Q[\d]+'
for i in range(0,len(neighborKey)):
    a = str(neighborKey[i])
    geoCodeList.append( re.search(geoCodePattern, a ).group())
    
## Make Dictionary for geocode to names
neighborKey[643] = 'pakke_kessang/None'
geoCodeToName = dict(zip(geoCodeList,neighborKey))


# In[38]:



## Funtion  
def replaceList(list,dictionary):
    result = []
    for i in range(len(list)):
        for code, name in dictionary.items():
            list[i] = list[i].replace(name,code)
    return list  

## Funtion to get 'key' from 'value' in dictionary
def getKey(value,dictionary):
    keyList = list(dictionary.keys())
    valList = list(dictionary.values())
    my_dict = dict(zip(valList,keyList))
    return my_dict.get(value)

## Funtion to replace district name with district_key 
def replaceListWithKey(list,dictionary):
    result = []
    for i in range(len(list)):
        for code, key in dictionary.items():
            list[i] = list[i].replace(code,key)
    return list 


# In[42]:


## get json with only geocode
jsonDict = neighborDictData.copy()

for i in list(jsonDict.keys()):
    sublist = jsonDict.pop(i)
    newlist =replaceList(sublist,geoCodeToName)
    key  = getKey(i,geoCodeToName)
    jsonDict[key] = newlist


# In[ ]:





# In[46]:


dicDistricCode = dict(zip(districWithoutState, districIntersect))

#len(np.intersect1d(neighborModified,districWithoutState))
#np.setdiff1d(districWithoutState,neighborModified) ----> unmatched city
mykeylist = []
for i in range(0,723):
    code = list(geoCodeToName.keys())[i]
    name = neighborModified[i]
    if name in dicDistricCode.keys():
        mykey = dicDistricCode.get(name)
        mykeylist.append(mykey)
    else:
        mykeylist.append(code)
        


# In[ ]:





# In[47]:


correction = [[434, 'CT_Bilaspur'],[392,'BR_Aurangabad'],[376, 'CT_Balrampur'],[16, 'HP_Hamirpur'],[262, 'RJ_Pratapgarh']]
for i in range(0,len(correction)):
    index = correction[i][0]
    change = correction[i][1]
    mykeylist[index-1] = change


# In[48]:



# In[49]:


# Replace json Qcode to distric key
geoCodeToKey = dict(zip( list(geoCodeToName.keys()),mykeylist ))
#replace geocode with district key
for i in list(jsonDict.keys()):
    sublist = jsonDict.pop(i)
    newlist =replaceListWithKey(sublist,geoCodeToKey)
    key  = geoCodeToKey.get(i)
    jsonDict[key] = newlist


# In[50]:


# funtion to combine 
def mergeJsonDistric(list, jsonDict,disName):
    myset = set()
    for dictKey in list:
        a = jsonDict.pop(dictKey)
        for i in a:
            myset.add(i) 
    
    jsonDict[disName] = setToList(myset)
    return jsonDict
#set to list
def setToList(Set):
    list = []
    for i in Set:
        list.append(i)
    return list 

#remove Geo Code from list 
def removeQcodeFromList(list,qcodelist):
    code = qcodelist
    for i in list:
        if i in code:
            list.remove(i)
    return list 

#sort Dictionary
def sortDict(Dict):
    dict1 = OrderedDict(sorted(Dict.items()))
    for key in dict1.keys():
        List = dict1.get(key)
        List.sort()
        dict1[key] = List
    return dict(dict1)
    


# In[51]:


# combine 
mergeMunbai = ['Q2085374','Q2341660']
mergeDelhi = ['Q107941','Q107960','Q429329','Q693367','Q766125' ,'Q549807','Q987','Q83486','Q25553535','Q2379189','Q2061938']

jsonDict = mergeJsonDistric(mergeMunbai, jsonDict,'MH_Mumbai')
jsonDict = mergeJsonDistric(mergeDelhi, jsonDict,'DL_Delhi')


# In[52]:


jsonDict['MH_Mumbai'] = ['MH_Thane']
jsonDict['DL_Delhi'] = ['UP_Gautam Buddha Nagar', 'HR_Jhajjar', 'HR_Gurugram', 'UP_Baghpat', 'UP_Ghaziabad', 'HR_Sonipat', 'HR_Faridabad']


# In[53]:


len(set(geoCodeToName.keys()))


# In[54]:


#rename geo code for delhi and mumbai
for i in jsonDict.keys():
    List = jsonDict.get(i)
    for j in range(0,len(List)):
        if List[j] in mergeMunbai:
            List[j] = 'MH_Mumbai'
        if List[j] in mergeDelhi: 
            List[j] = 'DL_Delhi'
    List = setToList(set(List))
    jsonDict[i] = List


# In[55]:


# remove Unknown Distric
removelist = []
for i in jsonDict.keys():
    if i in list(geoCodeToName.keys()):
        removelist.append(i)

for i in removelist:
    jsonDict.pop(i)
    
for i in jsonDict.keys():
    mylist = jsonDict.get(i)
    qcodelist = list(geoCodeToName.keys())
    jsonDict[i] = removeQcodeFromList(mylist,qcodelist)
    


# In[56]:


jsonDict["MN_Churachandpur"] = []
jsonDict["AS_Dibrugarh"] = ["AR_Tirap", "AR_Longding"]


# In[57]:


jsonDict = sortDict(jsonDict)


# In[58]:


# Save File
with open("Data/neighbor-districts-modified.json", "w") as outfile:
    json.dump(jsonDict, outfile)
    


# In[59]:


# Save District.csv after modification
covidData['Tested'] =  covidData['Tested'].fillna(0)
covidData.to_csv("Data/districts-modified.csv",index=False)
covidData = pd.read_csv("Data/districts-modified.csv",low_memory=False,index_col=False)
covidData = covidData.dropna()
covidData.to_csv("Data/districts-modified.csv",index=False)
vaccineData.to_csv("Data/vaccineData-modified.csv",index=False)


# In[ ]:





# In[ ]:




