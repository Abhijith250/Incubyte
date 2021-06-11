# -*- coding: utf-8 -*-
"""cond
Created on Thu Jun 10 22:21:20 2021

@author: Abhi
"""

import pymysql
import csv
import pandas as pd
with open('data2.txt', 'r') as f:
    reader = csv.reader(f, delimiter='|')
    for row in reader:
        print(row)
        
con=pymysql.connect(
	host="localhost",
	user="root",
	password="",
	database="incubytes"
               )




def table_country(con):
        
            cur = con.cursor()
    
            #Creating a dataframe
            dataframe = pd.read_sql_query("SELECT * FROM TABLE_INCUBYTES",con)
    
            #list of unique countries
            unique = dataframe.Country.unique()
            print(unique)
            #iterating across country
            for country in unique:
                query = """CREATE TABLE IF NOT EXISTS {countryname} AS SELECT * FROM TABLE_INCUBYTES WHERE Country = '{country}' """.format(countryname="table_"+country,country=country)
                print(query)
                try:
                    cur.execute(query)
                except:
                    print("error")

       
def table(con):
            cur=con.cursor()
            st="TABLE_"+"INCUBYTES"
            var="CREATE TABLE IF NOT EXISTS "+st+"(Customer_name VARCHAR(255) NOT NULL ,Customer_id VARCHAR(18) NOT NULL, Opendate DATE NOT NULL, Last_consulted_date DATE NOT NULL, Vaccination_type CHAR(5) NOT NULL,Doctor_Consulted CHAR(255) NOT NULL,State CHAR(5) NOT NULL,Country CHAR(5) NOT NULL,Date_of_birth DATE NOT NULL,Active_Customer CHAR(1) NOT NULL)"
            cur.execute(var)
            #file operations
            with open('data2.txt', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                for row in reader:
                    customer_name=row[2]
                    customer_id=row[3]
                    opendate=row[4]
                    lastdate=row[5]
                    vaccinationtype=row[6]
                    doctor=row[7]
                    state=row[8]
                    country=row[9]
                    print(state)
                    dob=row[10]
                    active=row[11]
                    #intial insertion to database
                    try:
                        cur.execute("INSERT INTO TABLE_INCUBYTES (Customer_name,Customer_id,Opendate,Last_consulted_date,Vaccination_type,Doctor_Consulted,State,Country,Date_of_birth,Active_Customer) VALUES(% s, % s, % s, % s, % s, % s, % s, % s, % s, % s)",
                                    (
                                        customer_name,
                                        customer_id,
                                        opendate,
                                        lastdate,
                                        vaccinationtype,
                                        doctor,
                                        state,
                                        country,
                                        dob,
                                        active))
                        cur.execute(var)
                    except:
                        print("error")


#calling functions
table(con)
table_country(con)       
        
        
        
        