
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
import matplotlib.pyplot as plt
import os
import json
import ast


# In[2]:


stock = '\A'


# In[3]:


data = pd.DataFrame()
for file in os.listdir("C:\\Users\darre\Desktop\ISS\Sem 3\BIG DATA ENGINEERING\CA\stocks"+stock):
    full_filename = "%s/%s" % ("C:\\Users\darre\Desktop\ISS\Sem 3\BIG DATA ENGINEERING\CA\stocks"+stock, file)
    with open(full_filename,'r') as fi:
        dict = json.load(fi)
        data_temp = json_normalize(dict)
        data = data.append(data_temp)


# In[4]:


# Create DateTime as Index
data['DateTime'] = data['date'] + ' ' + data['minute']
data = data[['marketAverage','DateTime']]
data.index = pd.to_datetime(data['DateTime'])
data = data.drop(['DateTime'], axis=1)

# Drop missing data where marketAverage = 0
data = data[  (data['marketAverage'] > 0)  ]
data.head()


# In[5]:


#divide into train and validation set
train = data[:int(0.7*(len(data)))]
valid = data[int(0.7*(len(data))):]

#plotting the data
train['marketAverage'].plot()
valid['marketAverage'].plot()


# In[6]:


#building the model
from pyramid.arima import auto_arima
model = auto_arima(train, trace=True, error_action='ignore', suppress_warnings=True)
model.fit(train)

forecast = model.predict(n_periods=len(valid))
forecast = pd.DataFrame(forecast,index = valid.index,columns=['Prediction'])

#plot the predictions for validation set
plt.plot(train, label='Train')
plt.plot(valid, label='Valid')
plt.plot(forecast, label='Prediction')
plt.show()


# In[7]:


#calculate rmse
from math import sqrt
from sklearn.metrics import mean_squared_error

rms = sqrt(mean_squared_error(valid,forecast))
print(rms)

