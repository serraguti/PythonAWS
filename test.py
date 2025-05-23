import pandas as pd

#creamos un dataframe con el fichero CSV
df = pd.read_csv("data/got.csv")
#imprimimos el dataframe
print(df.head(10))
