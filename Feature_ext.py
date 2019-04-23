import pandas as pd 




MonthDataFrame = pd.read_sas("c:\\Data\\allmonth.sas7bdat" , chunksize=None , iterator= False)
MonthDataFrame = MonthDataFrame.iloc[: , 0:6]

YearDataFrame = pd.read_sas('c:\\Data\\allyear.sas7bdat' , chunksize= None , iterator= False)

YearDataFrame = YearDataFrame[['year' ,'ws_id' , 'book' , 'mktcap_Dec' , 'cap_size']]

YearDataFrame.loc[: , 'bookToMktcap'] = YearDataFrame.apply(lambda row : row['book'] / row['mktcap_Dec'] , axis=1)
YearDataFrame.loc[: , 'Value'] = YearDataFrame.apply(lambda row:  'Growth ' if (row['bookToMktcap'] <1) else 'Value' , axis=1)
YearDataFrame = YearDataFrame.drop(YearDataFrame[YearDataFrame.year < 1986].index)

item = 0
for index ,row in YearDataFrame.iterrows() :
    if(row['Value'] == "Value"):
        item=item+1




print(item)
print(len(YearDataFrame) - item)
print('________________')




print(YearDataFrame.shape)

print(YearDataFrame.head())

print(MonthDataFrame.shape)
print(MonthDataFrame.head())

#book - ������������� �������� - ������� ������

#��������� MKT , HML  , SMB  � ������� ������ 
#������������� � ������ ����� �� mktcap(������������� ��������) � �� booktoMktcap
#HML = 1/2(Small Value - Big Value) - 1/2(Small Growth - Big Growth)
#��� �������� HML ���� �������� �� ������� � ������ �������� return �� �������� � 12 ������
#��� �� � ���� ������������� � ������� ���� ������� �� Big-Small �� ������������� � �� Value-Neutral-Growth �� B/M)




