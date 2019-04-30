import pandas  as  pd
import numpy as np 

data = pd.read_csv('data.csv')

data = data[['year' , 'month' , 'profit']]

data.to_csv('newdata.csv')