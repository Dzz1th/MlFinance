import pandas as pd
import numpy 

#Получаем Данные за прошлый год и за текущий(месячный датафрейм)
        #Сортируем компании в YearDataFrame по BE/MC
        #Делим компании на value , medium , growth в соотношении 30%/40%/30%
        #Так же делим по капитализации на small , Big(70-30) (это будет в исходных данных)
        #Выбираем компании из любой категории (например value small)
        #Взвешиваем компании по капитализации(????)
        #Считаем доли капитализации компании к общей капитализации категории
        #Считаем доходности порфтеля за июнь
        #Ребалансируем веса как w_(t-1) * (1 + price_return)
        #итерируемся по нашей бд по всем месяцам, когда доходим до июня - меняем год(в базе за месяц нужны данные
        # от июня до июня)

# def BuildPortfolio(YearDataFrame  , MonthDataFrame ):
#        YearDataFrame.sort_values(by = ['bookToMktcap'] , ascending = True)
#        GrowthQuantile = YearDataFrame.quantile(q=0.3)['bookToMktcap']
#        MidQuantile = YearDataFrame.quantile(q=0.7)['bookToMktcap']

#        GrowthDataFrame = YearDataFrame[YearDataFrame['bookToMktcap'] < GrowthQuantile]
#        MidDataFrame = YearDataFrame[YearDataFrame['bookToMktcap'] < MidQuantile and YearDataFrame['bookToMktcap'] >= GrowthQuantile]
#        ValueDataFrame = YearDataFrame[YearDataFrame['bookToMktcap'] >= MidQuantile]

#        SmallValueDataFrame = ValueDataFrame[ValueDataFrame['Size']] # ????????






MonthDataFrame = pd.read_sas("c:\\Data\\allmonth.sas7bdat" , chunksize=None , iterator= False)
MonthDataFrame = MonthDataFrame.iloc[: , 0:6]

YearDataFrame = pd.read_sas('c:\\Data\\allyear.sas7bdat' , chunksize= None , iterator= False)

YearDataFrame = YearDataFrame[['year' ,'ws_id' , 'book' , 'mktcap_Dec' , 'cap_size']]

YearDataFrame.loc[: , 'bookToMktcap'] = YearDataFrame.apply(lambda row : row['book'] / row['mktcap_Dec'] , axis=1)
#YearDataFrame.loc[: , 'Value'] = YearDataFrame.apply(lambda row:  'Growth' if (row['bookToMktcap'] <1) else 'Value' , axis=1)
YearDataFrame = YearDataFrame.drop(YearDataFrame[YearDataFrame.year < 1986].index)


del YearDataFrame['cap_size']


CapitalMedian = YearDataFrame.quantile(0.5)
print(CapitalMedian)
#YearDataFrame.loc[: , 'cap_size'] = YearDataFrame.apply(lambda row : 'Small' if([row['mktcap_Dec'] <CapitalMedian]) else 'Large' , axis=1)
        




# print(YearDataFrame.shape)

# print(YearDataFrame.head())

# print(MonthDataFrame.shape)
# print(MonthDataFrame.head())

#book - ������������� �������� - ������� ������

#��������� MKT , HML  , SMB  � ������� ������ 
#������������� � ������ ����� �� mktcap(������������� ��������) � �� booktoMktcap
#HML = 1/2(Small Value - Big Value) - 1/2(Small Growth - Big Growth)
#��� �������� HML ���� �������� �� ������� � ������ �������� return �� �������� � 12 ������
#��� �� � ���� ������������� � ������� ���� ������� �� Big-Small �� ������������� � �� Value-Neutral-Growth �� B/M)




