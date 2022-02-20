import mysql.connector
import pandas
from mysql.connector import (connection)
from mysql.connector import errorcode
import logging
import smtplib
import csv
from email.message import EmailMessage
import tkinter
from tkinter import filedialog


class CrudOperation:
    """In this class we are going to connect with DB for doing CRUD Operation"""
    def __init__(self, database_name):
        """This is constructor of CrudOperation"""
        self.database_name = database_name
        self.connection = None
        self.logger = logging.getLogger()
        try:
            self.connection = mysql.connector.connect(user="Dhoni", password="dhoni07", host="127.0.0.1", port=3306,
                                                      database=self.database_name)
            # connection = connection.MySQLConnection(user="Dhoni", password="dhoni07", host="127.0.0.1", port=3306,
            # database="python")
            self.logger.debug("connection Established")
            self.cursor = self.connection.cursor()
        except mysql.connector.Error as e:
            if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                self.logger.critical("Error occurred in your username and password")
            elif e.errno == errorcode.ER_BAD_DB_ERROR:
                self.logger.error("Database Doesn't Exist")
            else:
                self.logger.critical(e)

    def create_table(self):
        """This function is used to create a table in mysql Database"""
        try:
            while True:
                table_name = input("please enter the Table Name")
                display_query = "show tables"
                self.cursor.execute(display_query)
                tables = self.cursor.fetchall()
                for i in tables:
                    str_i = ''.join(i)
                    print(str_i)
                    if str_i == table_name:
                        self.logger.info("Sorry Table already present in the Database")
                        choice = input("Do you want to create table with new name? press Y/N")
                        break
                    else:
                        continue
                else:
                    no_of_columns = int(input("please enter no of columns you want to create in table"))
                    create_query = f"create table {table_name}("
                    for i in range(no_of_columns):
                        add = input(f"please enter the details of record {i+1}:\nplease Enter column name and its constraints by leaving space inbetween")
                        create_query = create_query+add+','
                    new_create_query = create_query[0:-1]
                    query = new_create_query+')'
                    print(query)
                    self.cursor.execute(query)
                    self.logger.debug("Table created")
                    self.send_mail("CREATE",table_name,self.database_name)
                    column_lst = self.desc_table(table_name)
                    print(column_lst)
                    choice = " "
                if choice.upper() == 'Y':
                    continue
                elif choice.upper() == 'N':
                    self.logger.info("Table creation terminated by user")
                    break
                else:
                    break
            # create_query = f"""create table if not exists {table_name}(id integer not null unique,first_name varchar(20) not null,last_name varchar(20))"""
            # self.cursor.execute(create_query)
            # print("Query Executed successfully")
        except Exception as e:
            self.logger.critical(e)

    def insert_data(self, table_name):
        """This function is used to  insert data into particular table"""
        try:
            column_lst = self.desc_table(table_name)
            print(column_lst)
            self.printall_data(table_name)
            length_column = len(column_lst)
            query = f"insert into {table_name} values(%s)"
            new_query = query % (",".join("%s" for i in range(length_column)))
            print(new_query)
            lst_tuples = []
            tup = ()
            for i in range(length_column):
                value_i = input("Enter the value for {} column".format(column_lst[i]))
                tup = tup + (value_i,)
            lst_tuples.append(tup)

            self.cursor.executemany(new_query, lst_tuples)
            # id = int(input("Please Enter the student id"))
            # first_name = input("Please Enter the first_name of student")
            # last_name = input("Please Enter the last_name of student")
            # insert_query = f"""insert into {table_name}(id,first_name,last_name)values(%s,%s,%s)"""
            # values_query = (id, first_name, last_name)
            # self.cursor.execute(insert_query, values_query)
            print("Data inserted")
            self.logger.debug("Data inserted")
            print("calling mail method")
            self.send_mail("INSERT", table_name, self.database_name)
            self.printall_data(table_name)
        except Exception as e:
            self.logger.critical(e)

    def printall_data(self, table_name):
        """This function is used print all data from the specific table"""
        try:
            query = f"""select * from {table_name}"""
            self.cursor.execute(query)
            data = self.cursor.fetchall()
            for x in data:
                print(x)
        except Exception as e:
            self.logger.critical(e)

    def delete_data(self, table_name):
        """This function is  used to delete the record from particular table"""
        try:
            column_lst = self.desc_table(table_name)
            self.logger.debug("Before Delete")
            print(column_lst)
            self.printall_data(table_name)
            value_id = int(input("please enter the id to  delete the record"))
            delete_query = f"""Delete from {table_name} where {column_lst[0]}=%s"""
            value_query = [value_id]
            self.cursor.execute(delete_query, value_query)
            self.logger.debug("Data Deleted")
            self.send_mail("DELETE", table_name, self.database_name)
            print("After Delete")
            self.printall_data(table_name)
        except Exception as e:
            self.logger.critical(e)

    def update_data(self, table_name):
        """This function is used to  update the record in particular table"""
        try:
            column_lst = self.desc_table(table_name)
            self.logger.debug("Before update")
            print(column_lst)
            self.printall_data(table_name)
            column_name = input("Please enter the column name to modify the value")
            if column_name == column_lst[0]:
                self.logger.debug("sorry you cannot change primary column value")
            else:
                value = input(f"please enter the value for the column {column_name}")
                id = input("please enter the id ")
                update_query = f"""update {table_name} set {column_name} = %s where {column_lst[0]}=%s"""
                query_value = (value, id)
                self.cursor.execute(update_query, query_value)
                self.logger.debug("Data updated")
                self.send_mail("UPDATE", table_name, self.database_name)
                print("After Update")
                self.printall_data(table_name)

        except Exception as e:
            self.logger.critical(e)

    def send_mail(self, operation_code,table_name,dbname):
        """This function is mainly used for the purpose of send mail to user for every operation"""
        try:
            file_name = filedialog.askopenfilename()
            file_content = pandas.read_csv(file_name)
            for i in range(len(file_content)):
                print(file_content.iloc[i, 0], file_content.iloc[i, 1])
                if file_content.iloc[i, 0].upper() == operation_code.upper():
                    toaddr = file_content.iloc[i, 1]
                    username = toaddr.split("@")[0]
            msg = EmailMessage()
            msg['Subject'] = f"{operation_code} Operation was successful"
            msg['From'] = "vigneshpsv52@gmail.com"
            msg['To'] = toaddr
            msg.set_content(f"Hi {username},\nThis is mail is to intimate you that {operation_code} operation is performed successfully in table {table_name} on database {dbname}")

            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                with open("password.txt", 'r') as obj:
                    password = obj.read()
                smtp.login("vigneshpsv52@gmail.com", password)
                smtp.send_message(msg)
        except Exception as e:
            self.logger.critical(e)



    def desc_table(self, table_name):
        """This function is used to provide the description about the table"""
        try:
            lst = []
            query = f"desc {table_name}"
            self.cursor.execute(query)
            table_structure = self.cursor.fetchall()
            for struc in table_structure:
                lst.append(struc[0])
            return lst
        except Exception as e:
            self.logger.critical("Exception occurred {}".format(e))

    def commit_close(self):
        """In this function we are closing the connection and committing it"""
        if self.connection is not None:
            self.logger.debug("closing connection")
            self.connection.commit()
            self.connection.close()


def main():
    """This main method is used to ask user which operation would you like perform on which database and table"""
    try:
        LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
        logging.basicConfig(filename="08_feb17_2022_loggers.log", filemode='w', level=logging.DEBUG, format=LOG_FORMAT)
        dbname = input("PLEASE ENTER THE DATABASE NAME")
        cd = CrudOperation(dbname)
        if cd.connection is not None:
            while True:
                choice = int(input("PLEASE CHOOSE THE OPERATION WOULD YOU LIKE TO PERFORM"
                               "\nPRESS 1.CREATE TABLE\nPRESS 2.INSERT DATA\nPRESS 3.UPDATE DATA\nPRESS 4.DELETE DATA\n"))
                if choice == 1:
                    cd.logger.debug("creating table")
                    cd.create_table()

                elif choice == 2:
                    table_name = input("Please Enter the Table name")
                    cd.logger.debug("inserting data")
                    cd.insert_data(table_name)

                elif choice == 3:
                    table_name = input("Please Enter the Table name")
                    cd.logger.debug("updating data")
                    cd.update_data(table_name)

                elif choice == 4:
                    table_name = input("Please Enter the Table name")
                    cd.logger.debug("Deleting Data")
                    cd.delete_data(table_name)

                else:
                    print("Invalid Data, Please Try Again..!!")

                criteria = input("Do you like to continue If yes Then press Y If not then Press N (Y/N)?")
                if criteria.upper() == 'Y':
                    continue
                elif criteria.upper() == 'N':
                    break
                else:
                    print("Invalid Answer So Program Terminated")
                    break

    except Exception as e:
        cd.logger.critical(e)

    finally:
        cd.commit_close()


if __name__ == "__main__":
    main()
