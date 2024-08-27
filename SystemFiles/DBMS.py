import json, time
from os import path
from datetime import datetime
from neo4j import GraphDatabase
import redis, cx_Oracle, pymongo
from faker import Faker

def postProcess(func):
    def wrapper(*args, **kwargs):
        # Pre-process inputs
        print(f"Running CRUD operation")

        # Call the decorated function and get the result
        resultInfo = func(*args, **kwargs)

        filename = f"{resultInfo['dbms']}_{resultInfo['tableName']}_{resultInfo['operation']}.json"
        filepath = path.join(resultInfo["saveDirectory"], filename)
        print(f"Saving Result Data as {filename}")

        # save the resultInfo list to the JSON file
        with open(filepath, "w") as f:
            json.dump(resultInfo, f, indent=4)

        return resultInfo
    return wrapper

# MongoDB
class MongoDB:
    def __init__(self, connUrl:str, dbName:str, sdDirectory:str,  dDirectory: str, user="", passw="") -> None:
        self.name = "MongoDB"

        self.connUrl = connUrl
        self.user = user
        self.passw = passw
        self.saveDataDirectory = sdDirectory
        self.dataDirectory = dDirectory

        # Establish a connection to the local MongoDB server
        self.client = pymongo.MongoClient(self.connUrl)

        # Access the database
        self.db = self.client[dbName]

        if dbName.lower() == 'library':
            self.collections = {
                "members": self.db["members"],
                "books": self.db["books"],
                "loans": self.db["loans"]
            }
        elif dbName.lower() == 'socialmedia':
            self.collections = {
                "UserPostComment": self.db["UserPostComment"]
            }

    def closeConn(self):
        # Close the client
        self.client.close()

    @postProcess
    def libraryRunTest(self, documentData:list[dict], collectionName:str, iterations:int):
        # note dData should be translated to bJSON if that matters for MongoDB
        dataToStore = []

        mainCollection = self.collections[collectionName]
        mainCollection.delete_many({}) # reset

        divisionFactor = len(documentData) // iterations
        iterSize = 0

        for i in range(iterations):
            run = i + 1
            print(f"Run {run}")
            
            iterSize += divisionFactor
            dataToInsert = documentData[:iterSize]

            inStartTime = time.time()
            result = mainCollection.insert_many(dataToInsert)
            inEndTime = time.time()

            update_query = {
                "$set": {
                    "ReturnDate": datetime.utcnow()
                }
            }

            upStartTime = time.time()
            mainCollection.update_many({}, update_query)
            upEndTime = time.time()

            delStartTime = time.time()
            result = mainCollection.delete_many({})
            delEndTime = time.time()

            dataToStore.append({
                "run":run, "qSize":iterSize,
                "inStartTime": inStartTime, "inEndTime": inEndTime, "inTime":round(abs(inEndTime - inStartTime), 2), 
                "upStartTime": upStartTime, "upEndTime": upEndTime, "upTime":round(abs(upStartTime - upEndTime), 2), 
                "delStartTime": delStartTime, "delEndTime": delEndTime, "delTime":round(abs(delStartTime - delEndTime), 2),                     
            })

        if result.acknowledged:
            print(f"Inserted")
        else:
            print("Insert operation failed")

        return {"saveDirectory":self.saveDataDirectory, "tableName": collectionName, "dbms": self.name, "operation":"runTest", "result":dataToStore}
    
    @postProcess
    def libraryRetrieveTest(self, collectionName:str, sizeOfData:int, iterations:int):
        dataToStore = []
        divisionFactor = sizeOfData // iterations

        startID = 1
        endID = 0
        iterSize = 0

        collection = self.db[collectionName]

        for i in range(iterations):
            run = i + 1
            iterSize += divisionFactor
            print(f"Run {run}")

            endID += divisionFactor
            print(startID, endID)

            # Retrieval Test 1: ID Range Query
            q1StartTime = time.time()
            query1 = {"LoanID": {"$gte": startID, "$lte": endID}}
            rows = list(collection.find(query1))  # to actually parse all data into system
            q1EndTime = time.time()

            # Retrieval Test 2: Title Search
            # q2StartTime = time.time()
            # pipeline = [
            #     {
            #         "$lookup": {
            #             "from": "books",
            #             "localField": "BookID",
            #             "foreignField": "BookID",
            #             "as": "book_details"
            #         }
            #     },
            #     {"$unwind": "$book_details"},
            #     {"$match": {"book_details.Title": {"$regex": "the", "$options": "i"}}}
            # ]
            # rows = list(collection.aggregate(pipeline))  # to actually parse all data into system
            # q2EndTime = time.time()

            dataToStore.append({
                "run": run,
                "qSize": iterSize,
                "q1StartTime": q1StartTime,
                "q1EndTime": q1EndTime,
                "q1Time": round(abs(q1StartTime - q1EndTime), 5),
                # "q2StartTime": q2StartTime,
                # "q2EndTime": q2EndTime,
                # "q2Time": round(abs(q2StartTime - q2EndTime), 2)
            })

        return {"saveDirectory": self.saveDataDirectory, "tableName": collectionName, "dbms": self.name, "operation": "search", "result": dataToStore}

    @postProcess
    def socialMediaRunTest(self, documentData:list[dict], collectionName:str, iterations:int):
        fake = Faker()

        # note dData should be translated to bJSON if that matters for MongoDB
        dataToStore = []

        mainCollection = self.collections[collectionName]
        mainCollection.delete_many({}) # reset

        divisionFactor = len(documentData) // iterations
        userIDs = list(range(1, len(documentData) + 1))

        text = fake.text(max_nb_chars=500 + 50)
        newText = text[:500].ljust(500)  
        
        iterSize = 0

        for i in range(iterations):
            run = i + 1
            print(f"Run {run}")
            
            iterSize += divisionFactor
            dataToInsert = documentData[:iterSize]
            
            update_query = {
                "$set": {
                    "Content": newText
                }
            }

            inStartTime = time.time()
            result = mainCollection.insert_many(dataToInsert)
            inEndTime = time.time()

            # Perform the bulk update
            upStartTime = time.time()
            mainCollection.update_many({}, update_query)
            upEndTime = time.time()

            delStartTime = time.time()
            result = mainCollection.delete_many({})
            delEndTime = time.time()

            dataToStore.append({
                "run":run, "qSize":iterSize,
                "inStartTime": inStartTime, "inEndTime": inEndTime, "inTime":round(abs(inEndTime - inStartTime), 2), 
                "upStartTime": upStartTime, "upEndTime": upEndTime, "upTime":round(abs(upStartTime - upEndTime), 2), 
                "delStartTime": delStartTime, "delEndTime": delEndTime, "delTime":round(abs(delStartTime - delEndTime), 2),                     
            })

        if result.acknowledged:
            print(f"Inserted")
        else:
            print("Insert operation failed")

        return {"saveDirectory":self.saveDataDirectory, "tableName": collectionName, "dbms": self.name, "operation":"runTest", "result":dataToStore}
    

class Oracle:
    def __init__(self, dsn:str, dbName:str, sdDirectory:str,  dDirectory: str, user="", passw="", tableSchema=""):
        self.name = "Oracle"

        self.dsn = dsn
        self.user = user
        self.password = passw

        self.tableSchema = tableSchema
        self.dbName = dbName

        self.saveDataDirectory = sdDirectory 
        self.dataDirectory = dDirectory

        self.connection = None
        self.cursor = None

        # Establish connection to the Oracle DB
        self.connection = cx_Oracle.connect(user=self.user, password=self.password, dsn=self.dsn)
        self.cursor = self.connection.cursor()

    def closeConn(self):
        # Close the cursor and connection
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    @postProcess
    def libraryRunTest(self, documentData:list[dict], tableName:str, iterations:int):
        newDocumentData = [(row['LoanID'], row['BookID'], row['MemberID'], row['LoanDate'], row['DueDate'], row['ReturnDate']) for row in documentData]
        dataToStore = []

        newTableName = f"{self.tableSchema}.{tableName}"

        # Reset the table by deleting all rows
        reset_query = f"DELETE FROM {newTableName}"
        # reset_query = f"TRUNCATE TABLE {newTableName}"

        self.cursor.execute(reset_query)
        self.connection.commit()

        divisionFactor = len(newDocumentData) // iterations
        iterSize = 0

        for i in range(iterations):
            run = i + 1
            print(f"Run {run}")

            iterSize += divisionFactor
            dataToInsert = newDocumentData[:iterSize]

            insertQuery = f"""
                INSERT INTO {newTableName} (LoanID, BookID, MemberID, LoanDate, DueDate, ReturnDate)
                VALUES (:LoanID, :BookID, :MemberID, :LoanDate, :DueDate, :ReturnDate)
            """

            # Insert data
            inStartTime = time.time()
            self.cursor.executemany(insertQuery, dataToInsert)
            self.connection.commit()
            inEndTime = time.time()

            # updateQuery = f"""
            #     UPDATE {newTableName}
            #     SET ReturnDate = CASE 
            #         WHEN ReturnDate IS NULL THEN NULL
            #         ELSE ReturnDate + INTERVAL '1' DAY
            #     END
            # """

            updateQuery = f"""
                UPDATE {newTableName}
                SET returnDate = SYSDATE
            """

            # Update records
            upStartTime = time.time()
            self.cursor.execute(updateQuery)
            self.connection.commit()
            upEndTime = time.time()

            time.sleep(1)

            # Delete all records
            delStartTime = time.time()
            self.cursor.execute(reset_query)
            self.connection.commit()
            delEndTime = time.time()

            dataToStore.append({
                "run": run,
                "qSize": iterSize,
                "inStartTime": inStartTime,
                "inEndTime": inEndTime,
                "inTime": round(abs(inEndTime - inStartTime), 2),
                "upStartTime": upStartTime,
                "upEndTime": upEndTime,
                "upTime": round(abs(upEndTime - upStartTime), 2),
                "delStartTime": delStartTime,
                "delEndTime": delEndTime,
                "delTime": round(abs(delEndTime - delStartTime), 2),
            })

        return {"saveDirectory": self.saveDataDirectory, "tableName": tableName, "dbms": self.name, "operation":"runTest", "result": dataToStore}

    @postProcess
    def libraryRetrieveTest(self, tableName:str, sizeOfData:int, iterations:int):
        print(sizeOfData)
        dataToStore = []
        divisionFactor = sizeOfData // iterations

        newTableName = f"{self.tableSchema}.{tableName}"

        startID = 1
        endID = 0
        iterSize = 0

        query1 = f"""
            SELECT * FROM {newTableName}
            WHERE LoanID BETWEEN :start_id AND :end_id
        """

        # qKeyWord = "the"
        # query2 = f"""
        #     SELECT * FROM {newTableName}
        #     WHERE BookID IN (
        #         SELECT BookID FROM SYS.BOOKS WHERE Title LIKE :keyword
        #     )
        # """

        for i in range(iterations):
            run = i + 1
            iterSize += divisionFactor
            print(f"Run {run}")

            endID += divisionFactor

            print(startID, endID)
            q1StartTime = time.time()
            self.cursor.execute(query1, [startID, endID])
            rows = self.cursor.fetchall() # to actually parse all data into system
            q1EndTime = time.time()

            # q2StartTime = time.time()
            # self.cursor.execute(query2, [f'%{qKeyWord}%'])
            # rows = self.cursor.fetchall() # to actually parse all data into system
            # q2EndTime = time.time()

            dataToStore.append({
                "run": run,
                "qSize": iterSize,
                "q1StartTime": q1StartTime,
                "q1EndTime": q1EndTime,
                "q1Time": round(abs(q1StartTime - q1EndTime), 5),
                # "q2StartTime": q2StartTime,
                # "q2EndTime": q2EndTime,
                # "q2Time": round(abs(q2StartTime - q2EndTime), 2)
            })

        return {"saveDirectory": self.saveDataDirectory, "tableName": tableName, "dbms": self.name, "operation":"search", "result": dataToStore}


class RedisDB:
    # Maintaince for redis is halted
    def __init__(self, connUrl:str, sdDirectory:str) -> None:
        self.name = "Redis"
        self.connUrl = connUrl
        self.saveDataDirectory = sdDirectory

        self.client = redis.Redis.from_url(self.connUrl)
        self.redisProcesseData = []

    def processRedisData(self, collectionName, jsonData:list[dict]):
        for i in range(jsonData):
            row = jsonData[i]
            self.redisProcesseData.append({"key":f"{collectionName}:{row['ID']}", "value":{
                        "name": row["name"],
                        "intakeYear": row["intakeYear"],
                        "age": row["age"],
                        "course": row["course"],
                        "sem": row["sem"]
                    }
                }
            )

    def closeConn(self):
        self.client.close()

    @postProcess
    def insertTest(self, documentData:dict, iterations:int):
        self.processRedisData("student", documentData)
        dataToStore = []

        self.client.flushdb()  # reset

        divisionFactor = len(documentData) // iterations
        iterSize = 0

        for i in range(iterations):
            run = i + 1
            print(f"Insert Run {run}")

            iterSize += divisionFactor
            dataToInsert = {str(key): json.dumps(value) for key, value in list(self.redisProcesseData.items())[:iterSize]}

            startTime = time.time()
            for key, value in dataToInsert.items():
                self.client.set(key, value)
            endTime = time.time()

            dataToStore.append({"run": run, "st": startTime, "et": endTime, "ft": round(abs(endTime - startTime), 2), "qSize": iterSize})

            if run != iterations:
                self.client.flushdb()  # reset

        return {"saveDirectory": self.saveDataDirectory, "op": "Insert", "dbms": self.name, "result": dataToStore}

    @postProcess
    def retrieveTest(self, documentData:dict, iterations:int):
        dataToStore = []

        keys = list(documentData.keys())
        divisionFactor = len(keys) // iterations
        iterSize = 0

        for i in range(iterations):
            run = i + 1
            print(f"Retrieve Run {run}")

            iterSize += divisionFactor
            keysToRetrieve = keys[:iterSize]

            startTime = time.time()
            for key in keysToRetrieve:
                self.client.get(key)
            endTime = time.time()

            dataToStore.append({"run": run, "st": startTime, "et": endTime, "ft": round(abs(endTime - startTime), 2), "qSize": iterSize})

        return {"saveDirectory": self.saveDataDirectory, "op": "Retrieve", "dbms": self.name, "result": dataToStore}

    @postProcess
    def updateTest(self, documentData:dict, iterations:int):
        dataToStore = []

        keys = list(documentData.keys())
        divisionFactor = len(keys) // iterations
        iterSize = 0

        for i in range(iterations):
            run = i + 1
            print(f"Update Run {run}")

            iterSize += divisionFactor
            keysToUpdate = keys[:iterSize]

            startTime = time.time()
            for key in keysToUpdate:
                new_value = json.dumps(documentData[key]) + "_updated"
                self.client.set(key, new_value)
            endTime = time.time()

            dataToStore.append({"run": run, "st": startTime, "et": endTime, "ft": round(abs(endTime - startTime), 2), "qSize": iterSize})

        return {"saveDirectory": self.saveDataDirectory, "op": "Update", "dbms": self.name, "result": dataToStore}

    @postProcess
    def deleteTest(self, documentData:dict, iterations:int):
        dataToStore = []

        keys = list(documentData.keys())
        divisionFactor = len(keys) // iterations
        iterSize = 0

        for i in range(iterations):
            run = i + 1
            print(f"Delete Run {run}")

            iterSize += divisionFactor
            keysToDelete = keys[:iterSize]

            startTime = time.time()
            for key in keysToDelete:
                self.client.delete(key)
            endTime = time.time()

            dataToStore.append({"run": run, "st": startTime, "et": endTime, "ft": round(abs(endTime - startTime), 2), "qSize": iterSize})

        return {"saveDirectory": self.saveDataDirectory, "op": "Delete", "dbms": self.name, "result": dataToStore}

    @postProcess
    def insertHashTest(self, hashKey:str, documentData:dict, iterations:int):
        dataToStore = []

        self.client.flushdb()  # reset

        for i in range(iterations):
            run = i + 1
            print(f"Insert Hash Run {run}")

            startTime = time.time()
            for field, value in documentData.items():
                self.client.hset(hashKey, field, value)
            endTime = time.time()

            dataToStore.append({"run": run, "st": startTime, "et": endTime, "ft": round(abs(endTime - startTime), 2), "qSize": len(documentData)})

            if run != iterations:
                self.client.flushdb()  # reset

        return {"saveDirectory": self.saveDataDirectory, "op": "InsertHash", "dbms": self.name, "result": dataToStore}

    @postProcess
    def retrieveHashTest(self, hashKey:str, iterations:int):
        dataToStore = []

        for i in range(iterations):
            run = i + 1
            print(f"Retrieve Hash Run {run}")

            startTime = time.time()
            self.client.hgetall(hashKey)
            endTime = time.time()

            dataToStore.append({"run": run, "st": startTime, "et": endTime, "ft": round(abs(endTime - startTime), 2), "qSize": 1})

        return {"saveDirectory": self.saveDataDirectory, "op": "RetrieveHash", "dbms": self.name, "result": dataToStore}
    
    # # Insert hash data
    # redis_db.insertHashTest(hash_key, document_data, iterations)

    # # Retrieve hash data
    # redis_db.retrieveHashTest(hash_key, iterations)

class Neo4jDB:
    def __init__(self, uri, username, password, db_name, sd_directory):
        self.name = "Neo4j"
        self.uri = uri
        self.username = username
        self.password = password
        self.db_name = db_name
        self.save_data_directory = sd_directory
        self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))

    def close_conn(self):
        self.driver.close()

    @postProcess
    def insertTest(self, documentData: list[dict], iterations: int):
        data_to_store = []

        with self.driver.session(database=self.db_name) as session:
            # Clear existing data
            session.run("MATCH (n) DETACH DELETE n")

            division_factor = len(documentData) // iterations
            iter_size = 0

            for i in range(iterations):
                run = i + 1
                print(f"Insert Run {run}")

                iter_size += division_factor
                data_to_insert = documentData[:iter_size]

                start_time = time.time()
                for student in data_to_insert:
                    session.run("""
                    CREATE (s:Student {
                        id: $ID,
                        name: $name,
                        intake_year: $intakeYear,
                        age: $age,
                        course: $course,
                        sem: $sem
                    })
                    """, {
                        "ID": student["ID"],
                        "name": student["name"],
                        "intakeYear": student["intakeYear"],
                        "age": int(student["age"]),
                        "course": student["course"],
                        "sem": int(student["sem"])
                    })
                end_time = time.time()

                data_to_store.append({
                    "run": run,
                    "st": start_time,
                    "et": end_time,
                    "ft": round(abs(end_time - start_time), 2),
                    "qSize": iter_size
                })

                if run != iterations:
                    # Clear data for the next iteration
                    session.run("MATCH (n) DETACH DELETE n")

        return {
            "saveDirectory": self.save_data_directory,
            "op": "Insert",
            "dbms": self.name,
            "result": data_to_store
        }

    @postProcess
    def retrieveTest(self, documentData: list[dict], iterations: int):
        data_to_store = []

        with self.driver.session(database=self.db_name) as session:
            division_factor = len(documentData) // iterations
            iter_size = 0

            for i in range(iterations):
                run = i + 1
                print(f"Retrieve Run {run}")

                iter_size += division_factor
                ids_to_retrieve = [student['ID'] for student in documentData[:iter_size]]

                start_time = time.time()
                session.run("""
                MATCH (s:Student)
                WHERE s.id IN $ids
                RETURN s
                """, {"ids": ids_to_retrieve})
                end_time = time.time()

                data_to_store.append({
                    "run": run,
                    "st": start_time,
                    "et": end_time,
                    "ft": round(abs(end_time - start_time), 2),
                    "qSize": iter_size
                })

        return {
            "saveDirectory": self.save_data_directory,
            "op": "Retrieve",
            "dbms": self.name,
            "result": data_to_store
        }

    @postProcess
    def updateTest(self, documentData: list[dict], iterations: int):
        data_to_store = []

        with self.driver.session(database=self.db_name) as session:
            division_factor = len(documentData) // iterations
            iter_size = 0

            for i in range(iterations):
                run = i + 1
                print(f"Update Run {run}")

                iter_size += division_factor
                data_to_update = documentData[:iter_size]

                start_time = time.time()
                for student in data_to_update:
                    session.run("""
                    MATCH (s:Student {id: $ID})
                    SET s.name = $name,
                        s.intake_year = $intakeYear,
                        s.age = $age,
                        s.course = $course,
                        s.sem = $sem
                    """, {
                        "ID": student["ID"],
                        "name": student["name"],
                        "intakeYear": student["intakeYear"],
                        "age": int(student["age"]),
                        "course": student["course"],
                        "sem": int(student["sem"])
                    })
                end_time = time.time()

                data_to_store.append({
                    "run": run,
                    "st": start_time,
                    "et": end_time,
                    "ft": round(abs(end_time - start_time), 2),
                    "qSize": iter_size
                })

        return {
            "saveDirectory": self.save_data_directory,
            "op": "Update",
            "dbms": self.name,
            "result": data_to_store
        }

    @postProcess
    def deleteTest(self, documentData: list[dict], iterations: int):
        data_to_store = []

        with self.driver.session(database=self.db_name) as session:
            division_factor = len(documentData) // iterations
            iter_size = 0

            for i in range(iterations):
                run = i + 1
                print(f"Delete Run {run}")

                iter_size += division_factor
                ids_to_delete = [student['ID'] for student in documentData[:iter_size]]

                start_time = time.time()
                session.run("""
                MATCH (s:Student)
                WHERE s.id IN $ids
                DETACH DELETE s
                """, {"ids": ids_to_delete})
                end_time = time.time()

                data_to_store.append({
                    "run": run,
                    "st": start_time,
                    "et": end_time,
                    "ft": round(abs(end_time - start_time), 2),
                    "qSize": iter_size
                })

        return {
            "saveDirectory": self.save_data_directory,
            "op": "Delete",
            "dbms": self.name,
            "result": data_to_store
        }