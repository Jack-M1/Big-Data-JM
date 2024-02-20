import pandas as pd
import numpy as np

datasetTbD = pd.read_csv("Trips_by_Distance.csv")
datasetFullTrips = pd.read_csv("Trips_Full Data")
date = "1/1/2019"
level = "National"
#print(dataset.loc["National","01/01/2019", "Population Staying at Home"])
#print(dataset.loc["Level"=="National", "Population Staying at Home"])

#levelFilter = dataset.query("Population Staying at Home < 7000000")
levelFilter = datasetTbD.loc[datasetTbD["Level"]=="National"]
levelFilter2 = levelFilter.loc[levelFilter["Date"] == "1/1/2019"]
#print(levelFilter2.iloc[0,6])


month, day, year = date.split('/')
day = int(day) 

#include code for grabbing value of first date here


filter = datasetTbD.loc[datasetTbD["Date"]==date]
value = filter.iloc[0,6]
int(value)
for i in range(6): # Stays as 6 as only counts for the succeeding week after the date
    day += 1 # Increments the day in the date to gain values for the week
    print(day)
    newDate = f"{month}/{day}/{year}"
    filter = datasetTbD.loc[datasetTbD["Date"]==newDate]
    tempValue = filter.iloc[0,6]
    int(tempValue)
    value += tempValue
    print(round(value/7)) # Round the existing value and produce the average of the week





