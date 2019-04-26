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

def SliceYearDataFrameToCategories(YearDataFrame):
        YearDataFrame.sort_values(by = ['bookToMktcap'] , ascending = True)
        GrowthQuantile = YearDataFrame.quantile(q=0.3)
        MidQuantile = YearDataFrame.quantile(q=0.7)

        GrowthDataFrame = YearDataFrame[YearDataFrame['bookToMktcap'] < GrowthQuantile['bookToMktcap']]
        MidDataFrame = YearDataFrame[YearDataFrame['bookToMktcap'] < MidQuantile['bookToMktcap'] and YearDataFrame['bookToMktcap'] >= GrowthQuantile['bookToMktcap']]
        ValueDataFrame = YearDataFrame[YearDataFrame['bookToMktcap'] >= MidQuantile['bookToMktcap']]
        SmallValueDataFrame = ValueDataFrame[ValueDataFrame['cap_size']=="Small"]
        return SmallValueDataFrame

def SliceMonthDataFrame(MonthDataFrame , year):
        DataFrame = MonthDataFrame[((MonthDataFrame['year'] == year) & (MonthDataFrame['month'] >= 6.0 )) | ((MonthDataFrame['year']==year+1) & (MonthDataFrame['month']<=6.0))]
        return DataFrame

def SliceYearDataFrame(YearDataFrame , year):
        DataFrame = YearDataFrame[YearDataFrame['year'] == year]
        return DataFrame


        
def InitializeWeight(SlicedDataFrame):
        mktcap_Dec_Summ = SlicedDataFrame['mktcap_Dec'].sum()
        weight = {}

        for index , row in SlicedDataFrame.iterrows():
                weight[row['ws_id']] = row['mktcap_Dec'] / mktcap_Dec_Summ
        
        return weight

def EvalMonthPortfolioProfit(weight , SlicedMonthDataFrame):
        profit=0
        for key , value in weight.items():
                try:
                        row = SlicedMonthDataFrame.loc[SlicedMonthDataFrame['ws_id']==key]
                        row = row['price']
                        price = row.tolist()
                        #print('Price in eval month' , price)
                        if(len(price) != 0):
                                profit = profit + price[0]*value
                except KeyError:
                        profit += 0
        return profit

def BalanceWeight(weight):
        weight_value_sum = 0
        new_weight = {}
        for key , value in weight.items():
                #print(value)
                weight_value_sum += value
        for key, value in weight.items():
                if(weight_value_sum == 0):
                        new_weight[key] = 0
                else: new_weight[key] = weight[key] / weight_value_sum
        
        return new_weight

def RebuildMonthPortfolio(weight , SlicedMonthDataFrame):
        new_weight = {}
        for key , value in weight.items():
                try:
                        row = SlicedMonthDataFrame.loc[SlicedMonthDataFrame['ws_id']==key]
                        row = row['price']
                        price = row.tolist()
                        #print('price int rebuild' , price)
                        if(len(price) == 0):
                                new_weight[key] = 0
                        else:
                                new_weight[key] = weight[key] * (1 + price[0])
                except KeyError:
                        new_weight[key] = 0
        new_weight = BalanceWeight(new_weight)
        return new_weight

def BuildPortfolio(YearDataFrame , MonthDataFrame):
        FirstIndex = 1986
        LastIndex = 2017
        ReturnableDataFrame = pd.DataFrame(data = [[0 ,0 , {} , 0]] , columns=['year' , 'month' , 'weight' , 'profit'])
        for year in range(FirstIndex , LastIndex):
                SlicedMonthDataFrame = SliceMonthDataFrame(MonthDataFrame , year)
                SlicedYearDataFrame = SliceYearDataFrame(YearDataFrame , year)
                weight = InitializeWeight(SlicedYearDataFrame)
                for i in range(0 , 13):
                        if(i <= 6):
                                month = i+6
                                DataFrame = SlicedMonthDataFrame[(SlicedMonthDataFrame['year']==year) &(SlicedMonthDataFrame['month']==month)]
                        else:
                                month = i-6
                                DataFrame = SlicedMonthDataFrame[(SlicedMonthDataFrame['year']==year+1) &(SlicedMonthDataFrame['month']==month)]
                        profit = EvalMonthPortfolioProfit(weight , DataFrame)
                        series = pd.Series(index=['year' , 'month' , 'weight' , 'profit'] , data = [year , month  , weight , profit] )
                        df = pd.DataFrame([series])
                        ReturnableDataFrame = ReturnableDataFrame.append(df)
                        weight = RebuildMonthPortfolio(weight , DataFrame)
        return ReturnableDataFrame

def main():       
        MonthDataFrame = pd.read_sas("c:\\Data\\allmonth.sas7bdat" , chunksize=None , iterator= False)
        MonthDataFrame = MonthDataFrame.iloc[: , 0:6]

        YearDataFrame = pd.read_sas('c:\\Data\\allyear.sas7bdat' , chunksize= None , iterator= False)

        YearDataFrame = YearDataFrame[['year' ,'ws_id' , 'book' , 'mktcap_Dec' , 'cap_size']]

        YearDataFrame.loc[: , 'bookToMktcap'] = YearDataFrame.apply(lambda row : row['book'] / row['mktcap_Dec'] , axis=1)
        YearDataFrame = YearDataFrame.drop(YearDataFrame[YearDataFrame.year < 1986].index)

        del YearDataFrame['cap_size']

        CapitalMedian = YearDataFrame.quantile(0.5)
        YearDataFrame.loc[: , 'cap_size'] = YearDataFrame.apply(lambda row : 'Small' if([row['mktcap_Dec'] <CapitalMedian['mktcap_Dec']]) else 'Large' , axis=1)


        DataFrame = BuildPortfolio(YearDataFrame , MonthDataFrame)
        print(DataFrame)
        DataFrame.to_csv('data.csv')

main()