import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from dask import dataframe as dd
import time
from sklearn.linear_model import LinearRegression
from dask_ml.model_selection import train_test_split

def question1A_Part1():
    start_time=time.time()
    dataset = pd.read_csv("Trips_by_Distance.csv")
    columnsToDrop = ["State FIPS", "State Postal Code", "County FIPS", "County Name"]
    dataset.drop(columnsToDrop, axis=1, inplace=True)
    dataset.dropna(inplace=True)
    print(dataset.duplicated())
    dataset["Date"] = pd.to_datetime(dataset["Date"])
    dataset = dataset.query("`Level`=='National'")
    dataset = dataset.groupby(pd.Grouper(key = "Date", freq = "W"))
    dataset = dataset["Population Staying at Home"].mean().round().reset_index()
    print(dataset) # Comment out when needed

    serialTime=time.time()-start_time
    print("Timing for serial question 1A:")
    print(serialTime)
    
    dataset["Week"] = (dataset.index+1).astype(int)
    plt.plot(dataset["Week"], dataset["Population Staying at Home"])
    plt.xlabel("Weeks Over Dataset")
    plt.ylabel("Population Staying at Home")
    plt.xticks(rotation=80)
    plt.title("Question 1A Part 1")
    plt.tight_layout()
    plt.show()

def question1A_Part2():
    dataset = pd.read_csv("Trips_Full Data.csv")
    dataset.dropna(inplace=True)
    print(dataset.duplicated())
    colDistances = ["Trips <1 Mile", "Trips 1-25 Miles", "Trips 1-3 Miles", "Trips 10-25 Miles", "Trips 100-250 Miles", "Trips 100+ Miles", "Trips 25-100 Miles", "Trips 25-50 Miles", "Trips 250-500 Miles", "Trips 3-5 Miles", "Trips 5-10 Miles", "Trips 50-100 Miles", "Trips 500+ Miles"]
    averageDistances = []
    print("Average Number of Trips:")
    for i in range(len(colDistances)):
        averageDistances.append(round(dataset[colDistances[i]].mean()))
        print(colDistances[i])
        print(averageDistances[i])
    

    plt.bar(colDistances, averageDistances)
    plt.xlabel("Distance Groups")
    plt.ylabel("Average Number of Trips")
    plt.xticks(rotation=80)
    plt.title("Question 1A Part 2")
    plt.tight_layout()
    plt.show()

def question1B():
    start_time=time.time()
    dataset = pd.read_csv("Trips_by_Distance.csv")
    columnsToDrop = ["State FIPS", "State Postal Code", "County FIPS", "County Name"]
    dataset.drop(columnsToDrop, axis=1, inplace=True)
    dataset.dropna(inplace=True)
    print(dataset.duplicated())
    dataset["Date"] = pd.to_datetime(dataset["Date"])
    dataset = dataset.query("`Level`=='National'")
    tripDistance1 = dataset.query("`Number of Trips 10-25` > 10000000")
    tripDistance2 = dataset.query("`Number of Trips 50-100` > 10000000")
    print("Number of Trips 10-25 > 10000000")
    print(tripDistance1)
    print("Number of Trips 50-100 > 10000000")
    print(tripDistance2)

    serialTime=time.time()-start_time
    print("Timing for serial question 1B:")
    print(serialTime)

    plt.scatter(tripDistance1["Date"], tripDistance1["Number of Trips 10-25"], s=3, alpha=0.5, label="Trips 10-25")
    plt.scatter(tripDistance2["Date"], tripDistance2["Number of Trips 50-100"], s=3, alpha=0.5, label="Trips 50-100")
    plt.xlabel("Date")
    plt.ylabel("Number of Trips")
    plt.xticks(rotation=80)
    plt.legend()
    plt.title("Question 1B")
    plt.tight_layout()
    plt.show()

def question1C():
    processList=[10,20]
    n_processors_time={}

    for i in processList:
        start_time=time.time()
        dataset = dd.read_csv("Trips_by_Distance.csv", dtype = {'County Name': 'object',
        'Number of Trips': 'float64',
        'Number of Trips 1-3': 'float64',
        'Number of Trips 10-25': 'float64',
        'Number of Trips 100-250': 'float64',
        'Number of Trips 25-50': 'float64',
        'Number of Trips 250-500': 'float64',
        'Number of Trips 3-5': 'float64',
        'Number of Trips 5-10': 'float64',
        'Number of Trips 50-100': 'float64',
        'Number of Trips <1': 'float64',
        'Number of Trips >=500': 'float64',
        'Population Not Staying at Home': 'float64',
        'Population Staying at Home': 'float64',
        'State Postal Code': 'object'})
        
        columnsToDrop = ["State FIPS", "State Postal Code", "County FIPS", "County Name"]
        dataset = dataset.drop(columnsToDrop, axis=1)
        dataset = dataset.dropna()
        duplicateCheck = ["Level", "Date", "Population Staying at Home", "Population Not Staying at Home", "Number of Trips", "Number of Trips <1", "Number of Trips 1-3", "Number of Trips 3-5", "Number of Trips 5-10", "Number of Trips 10-25", "Number of Trips 25-50", "Number of Trips 50-100", "Number of Trips 100-250", "Number of Trips 250-500", "Number of Trips >=500", "Row ID", "Week", "Month"]
        dataset = dataset.drop_duplicates(subset=duplicateCheck)
        dataset["Date"] = dd.to_datetime(dataset["Date"])
        dataset = dataset.query("`Level`=='National'")
        dataset = dataset.groupby(dataset["Week"])
        dataset = dataset["Population Staying at Home"].mean().round().reset_index()
        print(dataset) 
        dataset.compute()
        dask_time=time.time()-start_time
        n_processors_time[i]=dask_time
    print("Timing for each iteration of Question 1A:")
    print(n_processors_time)
    
    for i in processList:
        start_time=time.time()
        dataset = dd.read_csv("Trips_by_Distance.csv", dtype = {'County Name': 'object',
        'Number of Trips': 'float64',
        'Number of Trips 1-3': 'float64',
        'Number of Trips 10-25': 'float64',
        'Number of Trips 100-250': 'float64',
        'Number of Trips 25-50': 'float64',
        'Number of Trips 250-500': 'float64',
        'Number of Trips 3-5': 'float64',
        'Number of Trips 5-10': 'float64',
        'Number of Trips 50-100': 'float64',
        'Number of Trips <1': 'float64',
        'Number of Trips >=500': 'float64',
        'Population Not Staying at Home': 'float64',
        'Population Staying at Home': 'float64',
        'State Postal Code': 'object'})

        columnsToDrop = ["State FIPS", "State Postal Code", "County FIPS", "County Name"]
        dataset = dataset.drop(columnsToDrop, axis=1)
        dataset = dataset.dropna()
        duplicateCheck = ["Level", "Date", "Population Staying at Home", "Population Not Staying at Home", "Number of Trips", "Number of Trips <1", "Number of Trips 1-3", "Number of Trips 3-5", "Number of Trips 5-10", "Number of Trips 10-25", "Number of Trips 25-50", "Number of Trips 50-100", "Number of Trips 100-250", "Number of Trips 250-500", "Number of Trips >=500", "Row ID", "Week", "Month"]
        dataset = dataset.drop_duplicates(subset=duplicateCheck)
        dataset["Date"] = dd.to_datetime(dataset["Date"])
        dataset = dataset.query("`Level`=='National'")
        dataset = dataset.query("`Number of Trips 10-25` > 10000000")
        dataset = dataset.query("`Number of Trips 50-100` > 10000000")
        dataset.compute()
        dask_time=time.time()-start_time
        n_processors_time[i]=dask_time
    print("Timing for each iteration of question 1B:")
    print(n_processors_time)

def question1D():
    tripsByDistance = pd.read_csv("Trips_By_Distance.csv")
    tripsFullData = pd.read_csv("Trips_Full Data.csv")

    y_data = tripsByDistance
    y_data = y_data.query("`Level`=='National'")
    y_data = y_data[y_data["Week"]==31]
    y_data = y_data[y_data["Date"].str.contains("2019")]
    y_data = y_data["Number of Trips 10-25"]
    x_data = tripsFullData["Trips 1-25 Miles"]
    x_data = x_data.to_frame(name="Trips 1-25 Miles")
    y_data = y_data.to_frame(name="Number of Trips 10-25")
    #print(x_data)
    #print(y_data)

    model = LinearRegression().fit(x_data, y_data)
    r_sq = model.score(x_data, y_data)
    print("R squared of model is:")
    print(r_sq)

    #y_pred = model.predict(x_data)
    #print(y_pred)
    y_pred = model.intercept_ + model.coef_ * x_data
    print("The predicted values of the target variable:")
    print(y_pred)
    X_train, X_test, y_train, y_test = train_test_split(x_data, y_data, test_size=0.2, shuffle=True)
    print(X_train)
    print(X_test)

    plt.scatter(x_data, y_data)
    plt.xlabel("Trips 1-25 Miles Across 7 days")
    plt.ylabel("Number of Trips 10-25")
    plt.title("Question 1D")
    plt.show()

def question1E():
    dataset = pd.read_csv("Trips_Full Data.csv")
    colDistances = ["Trips <1 Mile", "Trips 1-25 Miles", "Trips 1-3 Miles", "Trips 10-25 Miles", "Trips 100-250 Miles", "Trips 100+ Miles", "Trips 25-100 Miles", "Trips 25-50 Miles", "Trips 250-500 Miles", "Trips 3-5 Miles", "Trips 5-10 Miles", "Trips 50-100 Miles", "Trips 500+ Miles"] # Change this to manual colums excluding trips which is currently included
    dataset[colDistances].plot(kind='bar', stacked=True)
    datetime = []
    for i in range(len(dataset["Date"])):
        splitDateTime = dataset["Date"][i]
        date, time = splitDateTime.split(" ")
        datetime.append(date)
        
    plt.xlabel("Date")
    plt.ylabel("Number of Travelers")
    plt.xticks(range(len(datetime)), datetime, rotation=45)
    plt.title("Question 1E")
    plt.legend(title="Trip Distance", loc="upper right")
    plt.tight_layout()
    plt.show()

question1D()