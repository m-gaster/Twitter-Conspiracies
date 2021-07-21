#!/usr/bin/env python
# coding: utf-8

# In[223]:


import pandas as pd
import numpy as np

from collections import defaultdict
from ast import literal_eval


# # Data Processing

# ### Import OG df

# In[224]:


cols_to_use = ['ID',
 'Num CT Tweets - HT',
 'Num CT Tweets - LINK',
 'Coordinates - ELECTION 2020',
 'State+County - ELECTION 2020',
 'FIPS - ELECTION 2020',
 'Weighted CT Score',
 'Consistent Location',
 'All User FIPS from user_location',
 'All User FIPS from place',
 'All User FIPS from coordinates',
 'ALL LOCATIONS']


# In[225]:


df = pd.read_csv(r"C:/Users/crackcocaine69xxx/Python Stuff/594/GeoCoV19 Data/Final-GeoCov-Output.csv",
                 usecols=cols_to_use)


# ### Literally evaluate columns with lists in them

# In[226]:


def literally_evaluate(element):
    if type(element) == str:
        try:
            return literal_eval(element)
        except:
            return element
    else:
        return element


# In[227]:


convert_cols = [
 'Coordinates - ELECTION 2020',
 'State+County - ELECTION 2020',
 'FIPS - ELECTION 2020',
 'Consistent Location',
 'All User FIPS from user_location',
 'All User FIPS from place',
 'All User FIPS from coordinates',
 'ALL LOCATIONS']

for col in convert_cols:
    df[col] = df[col].apply(literally_evaluate)


# ### Fix "All User FIPS from ..." fields

# In[228]:


def split_list(list_of_states_and_fips):
    try:
        fips = [x for x in list_of_states_and_fips if type(x)==int]

        states = [x for x in list_of_states_and_fips if type(x)==str]

        return fips, states
    except:
        return list_of_states_and_fips, list_of_states_and_fips


# In[229]:


for loc_type in ['All User FIPS from user_location', 'All User FIPS from place', 'All User FIPS from coordinates']:
    
    df[loc_type], df[loc_type.replace('FIPS', 'STATE')] = zip(*df[loc_type].apply(split_list))
    

df['ALL LOCATIONS - FIPS'], df['ALL LOCATIONS - STATE'] = zip(*df['ALL LOCATIONS'].apply(split_list))


# ## Fix "Consistent Location" field

# ### First, collapse lists of locations into unique values

# In[230]:


def settify(element):
    try:
        return list(set(element))
    except:
        return element


# In[231]:


for col in ['All User FIPS from user_location', 'All User FIPS from place',
       'All User FIPS from coordinates', 'ALL LOCATIONS',
       'All User STATE from user_location', 'All User STATE from place',
       'All User STATE from coordinates', 'ALL LOCATIONS - FIPS',
       'ALL LOCATIONS - STATE']:
    
    df[col] = df[col].apply(settify)


# ### Now create new "consistent location" fields

# In[180]:


def is_consistent_loc(location):
    
    if type(location) == list:
        
        return len(location)==1
    
    else:
        
        return location


# In[181]:


df['Consistent GeoCov19 STATE'] = df['ALL LOCATIONS - STATE'].apply(is_consistent_loc)

df['Consistent GeoCov19 FIPS'] = df['ALL LOCATIONS - FIPS'].apply(is_consistent_loc)


# In[232]:


def replace_empty_list_w_nan(element):
    if element==[]:
        return np.nan
    
    else:
        return element
        


# In[233]:


for col in df.columns:
    df[col] = df[col].apply(replace_empty_list_w_nan)


# In[234]:


def is_consistent_loc(location):
    
    if type(location) == list:
        
        return len(location)==1
    
    else:
        
        return location


# In[235]:


df['Consistent GeoCov19 STATE'] = df['ALL LOCATIONS - STATE'].apply(is_consistent_loc)

df['Consistent GeoCov19 FIPS'] = df['ALL LOCATIONS - FIPS'].apply(is_consistent_loc)


# In[236]:


def check_consistent_FIPS_across_geocov_election(row):
    '''
    returns True iff row['Consistent GeoCov19 FIPS'] location is consistent and does not clash with row['FIPS - ELECTION 2020']ion location,
        or if row['Consistent GeoCov19 FIPS'] is nan and row['FIPS - ELECTION 2020'] has a location
    
    returns nan if neither have a location
    
    returns False if locations clash
    '''
    if row['Consistent GeoCov19 FIPS']==False:
        return False
    
    elif (pd.isnull(row['FIPS - ELECTION 2020']) and pd.isnull(row['Consistent GeoCov19 FIPS'])):
        return np.nan
    
    elif (not pd.isnull(row['FIPS - ELECTION 2020']) and row['Consistent GeoCov19 FIPS']==True):
        if row['FIPS - ELECTION 2020'] == row['ALL LOCATIONS - FIPS'][0]:
            return True
        else:
            return False
    
    else:
        return True
    
    


# In[237]:


def check_consistent_STATE_across_geocov_election(row):
    '''
    returns True iff row['Consistent GeoCov19 STATE'] location is consistent and does not clash with row['State+County - ELECTION 2020']ion location,
        or if row['Consistent GeoCov19 STATE'] is nan and row['State+County - ELECTION 2020'] has a location
    
    returns nan if neither have a location
    
    returns False if locations clash
    '''
    if row['Consistent GeoCov19 STATE']==False:
        return False
    
    elif (pd.isnull(row['State+County - ELECTION 2020']) and pd.isnull(row['Consistent GeoCov19 STATE'])):
        return np.nan
    
    elif (type(row['State+County - ELECTION 2020'])==tuple and row['Consistent GeoCov19 STATE']==True):
        if row['State+County - ELECTION 2020'][0] == row['ALL LOCATIONS - STATE'][0]:
            return True
        else:
            return False
    
    else:
        return True
    
    


# In[238]:


df['Consistent FIPS'] = df.apply(check_consistent_FIPS_across_geocov_election, axis=1)

df['Consistent STATE'] = df.apply(check_consistent_STATE_across_geocov_election, axis=1)


# In[254]:


def get_final_element_from_STATE_list(row):
    
    if row['Consistent STATE']==True:
        
        try:
            return row['ALL LOCATIONS - STATE'][0]
        except:
            return row['State+County - ELECTION 2020'][0]
    
    else:
        
        return np.nan

def get_final_element_from_FIPS_list(row):
    
    if row['Consistent FIPS']==True:
    
        try:
            return row['ALL LOCATIONS - FIPS'][0]
        except:
            return row['FIPS - ELECTION 2020']
    
    else:
        
        return np.nan


# In[255]:


df['Usable FIPS'] = df.apply(get_final_element_from_FIPS_list, axis=1)

df['Usable STATE'] =  df.apply(get_final_element_from_STATE_list, axis=1)


# In[265]:


df['FIPS from Coordinates'] = ~df['All User FIPS from coordinates'].isnull()


# In[269]:


df[ ~(df['Usable FIPS'].isnull() & df['Usable STATE'].isnull()) ][['ID', 'Usable FIPS', 'Usable STATE']].to_csv(r"C:/Users/crackcocaine69xxx/Python Stuff/594/GeoCoV19 Data/FINAL-CLEANED-GEOLOCATED-USERS.csv", index=False)


# In[ ]:





# In[ ]:





# ### # users with FIPS data

# In[274]:


len(df[~df['Usable FIPS'].isnull()])


# ### # users with STATE data

# In[271]:


len(df[(~df['Usable STATE'].isnull())])


# ### # users with FIPS *or* STATE data

# In[272]:


len(df[(~df['Usable STATE'].isnull()) | (~df['Usable FIPS'].isnull())])

