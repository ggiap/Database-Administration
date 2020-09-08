import os
import platform
from termcolor import colored
import mysql.connector
from mysql.connector import Error

def insert(connection, cursor, table, val):
    try:
        if table == "City":
            mySql_insert_query = "INSERT INTO " + table + "(Name, CountryCode, District, Population) VALUES (%s, %s, %s, %s)"
        elif table == "CountryLanguage":
            mySql_insert_query = "INSERT INTO " + table + " VALUES (%s, %s, %s, %s)"
        elif table == "Country":
            mySql_insert_query = "INSERT INTO " + table + " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        rec = []
        for value in val:
            rec.append(value)

        cursor.execute(mySql_insert_query, rec)
        connection.commit()
        print("Record inserted successfully into table: " + table)
        print("Record inserted, ID:", cursor.lastrowid)
    except Error as e:
        print("Failed to insert into table " + table + " {}".format(e))
        print('\n')

def select(cursor, table, select_clause, whereClause):
    try:
        sql_query = "SELECT " + select_clause + " FROM " + table + " WHERE " + whereClause
        cursor.execute(sql_query)
        cursorResult = cursor.fetchall()
        for row in cursorResult:
            print(row)
        if len(cursorResult) == 0:
            print("Record not found")

    except Error as e:
        print("Failed to read table data", e)

def delete(connection, cursor, table, whereClause):
    try:
        sql_Delete_query = "DELETE FROM " + table + " WHERE " + whereClause
        cursor.execute(sql_Delete_query)
        connection.commit()

        cursor.execute("SELECT * FROM " + table + " WHERE " + whereClause)
        records = cursor.fetchall()
        if len(records) == 0:
            print("\nRecord Deleted successfully ")

    except mysql.connector.Error as error:
        print("Failed to delete record from table: {}".format(error))

def update(connection, cursor, table, colName, colValue, whereClause):
    try:
        sql_update_query = "Update " + table + " set " + colName + " = '" + colValue + "' WHERE " + whereClause
        cursor.execute(sql_update_query)
        connection.commit()
        print("Record Updated successfully ")

    except mysql.connector.Error as error:
        print("Failed to update table record: {}".format(error))

def freestyle_sql_query(connection, cursor):
    try:
        sql_query = input("Enter your SQL query: ")
        print("")
        cursor.execute(sql_query)

        if sql_query.find("SELECT") != -1:
            cursorResult = cursor.fetchall() 

            if len(cursorResult) > 0:
                for r in cursorResult:
                    print(r)

        print("\nSQL query executed successfully")
    except Error as e:
        print("Could not execute SQL query ", e)

"""def create_table(cursor):
    try:
        sql_dropTable_query = "DROP TABLE IF EXISTS " + table
        sql_createTable_query = "CREATE TABLE " + table + "(id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(100) NOT NULL, action VARCHAR(30), onTable VARCHAR(50), timestamp TIMESTAMP);"
        cursor.execute(sql_dropTable_query)
        cursor.execute(sql_createTable_query)
        print("Table created successfully")
    except Error as e:
        print("Could not create the table ", e)"""

"""def create_trigger(connection, cursor, table, name, action):
    try:
        cursor.execute("DROP TRIGGER IF EXISTS " + name)
        cursor.execute("CREATE TRIGGER " + name + " AFTER " + action + " ON " + table + " FOR EACH ROW INSERT INTO Logs(username, action, onTable, timestamp) VALUES (CURRENT_USER(), '" + action + "', '" + table + "', NOW())")
        connection.commit()
        print("Trigger created successfully")
         
    except Error as e:
        print("Could not create trigger " + name + " ", e)"""

"""def create_procedure(cursor, table, whereClause):
    try:
        cursor.execute("DROP PROCEDURE IF EXISTS get_Dense_Populated_Cities")
        cursor.execute("CREATE PROCEDURE get_Dense_Populated_Cities() SELECT * FROM " + table + " WHERE " + whereClause)
        print("Stored procedure created succesfully")

    except Error as e:
        print("Could not create stored procedure ", e)"""
    
"""def call_procedure(cursor):
    try:
        cursor.execute("CALL get_Dense_Populated_Cities();")
        results = cursor.fetchall()
        for res in results:
            print(res)
        print("")
    except Error as e:
        print("Error while trying to call stored procedure ", e)"""

"""def transaction(cursor):
    try:
        #cursor.execute("SET AUTOCOMMIT = 0; LOCK TABLES City as C1 READ, City as C2 WRITE;")
        cursor.execute("START TRANSACTION; SELECT @A:=Population FROM City as C1 WHERE ID = 5; UPDATE City as C2 SET Population = @A WHERE ID = 1234567892; COMMIT;")
        #cursor.execute("UNLOCK TABLES;")
        print("Transaction ended successfully")
    except Error as e:
            print(e)"""

def find_available_tables(cursor):
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()

    return tables

def choose_table(tables, prompt):
    print(colored("Available tables in database: ", "cyan"))
    for t in tables:
        print(t)

    print("")
    return input(prompt)

def get_table_columns(cursor, chosen_table):
    cursor.execute("SHOW COLUMNS FROM " + chosen_table)
    columns = cursor.fetchall()

    print("")
    print(colored("Table " + chosen_table + " contains the following columns: ",'green'))
    for c in columns:
        print(c)
    
    print("\n")

    return columns

def choose_operation():
    print(" 1. Select")
    print(" 2. Update")
    print(" 3. Delete")
    print(" 4. Insert")
    print(" 5. Freestyle SQL query")
    print(" 6. Exit")
    """print(" 5. Create Table")
    print(" 6. Create Trigger")
    print(" 7. Create Procedure")
    print(" 8. Call Procedure")
    print(" 9. Transaction")
    print("10. Exit")"""

    choice = input(colored("Choose operation: ","green"))
    print("")

    return choice

def handle_chosen_operation(connection, cursor, operation_choice):
    # INSERT
    if operation_choice == "1":
        chosen_table = ""
        tables = find_available_tables(cursor)
        chosen_table = choose_table(tables, colored("Type the name of the table you want to operate on (or enter a space to exit): ", "green"))
        if chosen_table == " ":
            return
        chosen_table_columns = get_table_columns(cursor, chosen_table)

        select_clause = input("Select clause: ")
        where = input("Where clause: ")

        select(cursor, chosen_table, select_clause, where)

        print("")
        input("Press enter to continue...")
        print("")
        return
    
    # UPDATE
    if operation_choice == "2":
        chosen_table = ""
        tables = find_available_tables(cursor)
        chosen_table = choose_table(tables, colored("Type the name of the table you want to operate on (or enter a space to exit): ", "green"))
        if chosen_table == " ":
            return
        chosen_table_columns = get_table_columns(cursor, chosen_table)

        col = input("Set column: ")
        newValue = input("New value: ")
        where = input("Where clause: ")

        update(connection, cursor, chosen_table, col, newValue, where)

        print("")
        input("Press enter to continue...")
        print("")
        return

    # DELETE
    if operation_choice == "3":
        chosen_table = ""
        tables = find_available_tables(cursor)
        chosen_table = choose_table(tables, colored("Type the name of the table you want to operate on (or enter a space to exit): ", "green"))
        if chosen_table == " ":
            return
        chosen_table_columns = get_table_columns(cursor, chosen_table)

        where = input("Where clause: ")

        delete(connection, cursor, chosen_table, where)

        print("")
        input("Press enter to continue...")
        print("")
        return

    # INSERT
    if operation_choice == "4":
        chosen_table = ""
        tables = find_available_tables(cursor)
        chosen_table = choose_table(tables, colored("Type the name of the table you want to operate on (or enter a space to exit): ", "green"))
        if chosen_table == " ":
            return
        chosen_table_columns = get_table_columns(cursor, chosen_table)

        print("")
        val = []
        counter = 0
        if chosen_table == "City":
            counter = 1

        while counter < len(chosen_table_columns):
            rec = input("Enter a value for field " + str(chosen_table_columns[counter]) + ": ")
            val.append(rec)
            counter = counter + 1

        insert(connection, cursor, chosen_table, val)

        print("")
        input("Press enter to continue...")
        print("")
        return

    # FREESTYLE
    if operation_choice == "5":    
        while 1:
            if platform.system() == "Windows":
                os.system("cls")
            elif platform.system() == "Linux":
                os.system("clear")
            print("")
            print("1. SQL Query")
            print("2. Show table columns")
            print("3. Return to main menu")
            choice = input(colored("Choose operation: ", "green"))
            print("")
            
            if choice == "1":
                freestyle_sql_query(connection, cursor)
                break
            elif choice == "2":
                tables = find_available_tables(cursor)
                chosen_table = choose_table(tables, "Select a table from the list to see its columns: ")
                if chosen_table == " ":
                    return
                columns = get_table_columns(cursor, chosen_table)
            else:
                return

            print("")
            input("Press enter to continue...")
            print("")

        print("")
        input("Press enter to continue...")
        print("")

        return

    # EXIT  
    if operation_choice == "6":
        exit()

"""
    # CREATE TABLE
    if operation_choice == "5":
        print("")
        
        create_table(cursor)

        print("")
        input("Press enter to continue...")
        print("")
        return
        
    # CREATE TRIGGER
    if operation_choice == "6":
        print("")
        input("Press enter to continue...")
        print("")
        return

    # CREATE PROCEDURE
    if operation_choice == "7":
        print("")
        input("Press enter to continue...")
        print("")
        return

    # CALL PROCEDURE
    if operation_choice == "8":
        print("")
        input("Press enter to continue...")
        print("")
        return

    # TRANSACTION
    if operation_choice == "9":
        print("")
        input("Press enter to continue...")
        print("")
        return
"""

def main():
    try:
        if platform.system() == "Windows":
            os.system("cls")
        elif platform.system() == "Linux":
            os.system("clear")

        connection = mysql.connector.connect(host       = '"""Enter hostname here"""',
                                             database   = '"""Enter database name here"""',
                                             user       = '"""Enter username here"""',
                                             password   = '"""Enter password here"""')
        db_Info = connection.get_server_info()
        print("\nConnected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database:", record, '\n')

        while 1:
            operation_choice = ""

            operation_choice = choose_operation()
            handle_chosen_operation(connection, cursor, operation_choice)
            if platform.system() == "Windows":
                os.system("cls")
            elif platform.system() == "Linux":
                os.system("clear")

        # DEBUGGING STATEMENTS
        """
        insert(cursor, "City", [42898, "Kastoria", "GRE", "", 3000])
        insert(cursor, "City", ["Kastoria", "GRE", "", 3000])
        select(cursor, "City", "ID = 42898")
        update(cursor, "City", "Population", "6000", "ID = 42898")
        select(cursor, "City", "ID = 5")
        delete(cursor, "City", "ID = 42898")
    
        insert(cursor, "CountryLanguage", ("GRE", "Gree", True, 99.0))
        select(cursor, "CountryLanguage", "Percentage = 99")
        update(cursor, "CountryLanguage", "Percentage", "98.0", "Language = 'Gree'")
        delete(cursor, "CountryLanguage", "Language = 'Gree'")
    
        create_table(cursor, "Logs")
        create_trigger(cursor, "City", "georgiap_tr1", "INSERT")
        create_trigger(cursor, "City", "georgiap_tr2", "UPDATE")
        create_trigger(cursor, "City", "georgiap_tr3", "DELETE")
        create_trigger(cursor, "CountryLanguage", "georgiap_tr4", "INSERT")
        create_trigger(cursor, "CountryLanguage", "georgiap_tr5", "UPDATE")
        create_trigger(cursor, "CountryLanguage", "georgiap_tr6", "DELETE")
    
        create_procedure(cursor, "City", "Population > 5000000")
        call_procedure(cursor)
    
        transaction(cursor)
        """

    except Error as e:
        print("\nError while trying to connect to MySQL", e, '\n')
        exit()

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed\n")


if __name__ == "__main__":
    main()