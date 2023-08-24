#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import csv


# In[2]:


# Opening modified neighbor districts json file
with open('Data/neighbor-districts-modified.json') as json_data:
    data = json.load(json_data)


# In[3]:


# Making graph representation in edgeList format
edgeList = []
for i in data.keys():
    List = data.get(i)
    for j in List:
        edge = [i,j]
        edgeList.append(edge)


# In[4]:


# Saving edgeList Graph representation in csv file. 
# Where every row represent an edge between two node in column i and j.
header = ['i', 'j']
with open('Data/edge-graph.csv', 'w', encoding='UTF8') as file:
    writer = csv.writer(file)

    # Header for two node i and j
    writer.writerow(header)
    # Add edge at every row
    for i in edgeList:
        writer.writerow(i)


# In[ ]:




