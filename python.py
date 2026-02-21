import pandas as pd
df=pd.read_csv('Data/Test.csv')
df.to_csv('Data/Test_Main.csv')

print(df.head())



