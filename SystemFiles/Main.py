import json, time
from dotenv import load_dotenv
from os import listdir, getenv
from DBMS import *
import pandas as pd
import csv

if __name__ == "__main__":
    databaseName = "SocialMedia"
    load_dotenv()

    dataDirectory = getenv("dataDirectory")
    saveDataDirectory = getenv("saveDataDirectory")

    run = True
    while run:
        data = None
        DBMS_System = None
        print("DBMS Options:\n1. MongoDB\n2. Oracle\n3. Neo4j\n4. Redis")
        option = int(input("Selection: "))

        # Initialization of DBMS
        match option:
            case 1:
                DBMS_System = MongoDB(getenv("mDBConnectionURL"), databaseName, saveDataDirectory, dataDirectory)
            case 2:
                DBMS_System= Oracle(getenv("oracleDns"), databaseName, saveDataDirectory, dataDirectory, getenv("oracleUser"), getenv("oraclePW"), getenv("oracleTableSchema"))
            case 3:
                DBMS_System= RedisDB(getenv("rdConnectionURL"), saveDataDirectory)
            case 3:
                DBMS_System = Neo4jDB(
                    getenv("NEO4J_URI"),
                    getenv("NEO4J_USERNAME"),
                    getenv("NEO4J_PASSWORD"),
                    getenv("NEO4J_DB_NAME"),
                    saveDataDirectory
                )
            
        print("CRUD Operations:\n1. Test Run Library\n2. Retrieve Library\n3. Test Run Social Media")
        crudOption = int(input("Selection: "))

        match crudOption:
            case 1:
                # Test run library option. Meaning in this test run it will help cover INSERT, UPDATE (of 1 day increemnt in ReturnDate for all inserted rows), and DELETE
                data = pd.read_csv(dataDirectory + '/loans.csv', dtype={'LoanID': int, 'BookID': int, 'MemberID': int}, parse_dates=['LoanDate', 'DueDate', 'ReturnDate'])
                
                # Reminder: In format [{'column1Name': value, 'column2Name':value, ...}, {}, ...]
                # The Dates are in TimeStamp format, not STR or VARCHAR. Change it for DBMS requirement should it be necessary accordingly
                data_dict = data.to_dict(orient='records') 

                DBMS_System.libraryRunTest(data_dict, "loans", 5)

            case 2:
                data = pd.read_csv(dataDirectory + '/loans.csv', dtype={'LoanID': int, 'BookID': int, 'MemberID': int}, parse_dates=['LoanDate', 'DueDate', 'ReturnDate'])
                data_dict = data.to_dict(orient='records') 

                DBMS_System.libraryRetrieveTest("loans", len(data_dict), 5)       

            case 3:
                data = pd.read_csv(dataDirectory + '/user_post_comments.csv', dtype={'PostCommentID': int, 'UserID': int, 'PostID': int, 'Content': str})
                data_dict = data.to_dict(orient='records') 

                DBMS_System.socialMediaRunTest(data_dict, "UserPostComment", 5)

        print("Process finished. Closing DBMS Connection.\n")
        DBMS_System.closeConn()

        if not input("Continue Testing? (Y/N)\n").strip().capitalize() == "Y":
            run = False