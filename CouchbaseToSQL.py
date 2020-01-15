############################################################################
#
#   BETA: Work in progress!
#
#   This script loads a couchbase bucket in to a SQL database.
#   The basic functionality works.  Code cleanup and features need to be added.
#
#
#
############################################################################
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
from couchbase.exceptions import CouchbaseError
from couchbase.n1ql import N1QLQuery
from os import system, name
import pyodbc

#Requiremed modules
# pyodbc
# couchbase

system('cls')
system('clear')

bucket_name = 'beer-sample'


#This function will load a couchbase bucket in to a SQL Table
def couchbase_to_SQL(bucket_name):

    table_name  = 'cb_'+bucket_name

    #Connect to SQL Server
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost\SQLEXPRESS;'
                      'Database=PythonImport;'
                      'Trusted_Connection=yes;',autocommit=True)

    sqlcursor = conn.cursor()

    #Connect to couchbase
    cluster = Cluster('couchbase://localhost')
    authenticator = PasswordAuthenticator('user', 'password')
    cluster.authenticate(authenticator)
    c = cluster.open_bucket(bucket_name)

    #Grab all records
    q = N1QLQuery('SELECT * FROM `'+bucket_name+'` ')

    #This dictionary will contain columns for any tables created
    column_list_dict = {}
    
    conn.commit()

    #Loop to insert records to SQL table(s)
    try:
        for row in cluster.n1ql_query(q):
            rowval = row[bucket_name]
            process_couchbase_row (rowval,column_list_dict,table_name ,sqlcursor,conn)
    except:
        print('fail')
        print("Unexpected error:", sys.exc_info()[0])
        conn.rollback()
    
    conn.commit()



def process_couchbase_row(row,column_list_dict,table_name ,sqlcursor,conn):

    #Create table if this is the first iteration
    #Table is initialized with a single identity column
    if(table_name not in column_list_dict):
        dropsql = 'IF (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \'dbo\' AND  TABLE_NAME = \''+table_name+'\')) drop table ['+table_name+']'
        sqlcursor.execute(dropsql)
        sqlcursor.execute('Create table ['+table_name+'] (['+table_name+'ID] int identity(1,1));')
        column_list_dict[table_name] = []

    column_list = column_list_dict[table_name]        
        
    #Begin dynamic sql
    sql_insert = 'INSERT INTO ['+table_name +']('
    sql_values = 'VALUES ('
    sql_parameters = []

    #For each column, check if column exists, and add it to the table.
    for element in row:
        if(element not in column_list):
                column_list.append(element)
                sql_alter = 'Alter Table ['+table_name
                sql_alter = sql_alter + '] ADD ['+element+ '] NVARCHAR(MAX)'
                sqlcursor.execute(sql_alter)
                #print(sql_alter) 

        sql_insert = sql_insert +'['+ element + '],'
        sql_values = sql_values + '?,'

        #If the field in the json, is another json dictionary, then add records to a different table and grab the identity
        #In SQL, records would be linked by this column
        if(isinstance(row[element],dict)):
            sql_parameters.append(None)#Named column will be null
            sub_table_name = 'cb_'+element

            idcolumnname = sub_table_name+'ID'
            sql_insert = sql_insert + idcolumnname + ','
            sql_values = sql_values + '?,'

            if(idcolumnname not in column_list):
                column_list.append(idcolumnname)
                sql_alter = 'Alter Table ['+table_name
                sql_alter = sql_alter + '] ADD ['+idcolumnname+ '] int'
                sqlcursor.execute(sql_alter)
 
            inserted_row_identity = process_couchbase_row(row[element],column_list_dict,sub_table_name,sqlcursor,conn)
            sql_parameters.append(inserted_row_identity)
        elif(isinstance(row[element],list)):
            if(len(row[element]) >0):
                sql_parameters.append(row[element][0])
            else:
                sql_parameters.append(None)
        else:
            sql_parameters.append(row[element])
        #print(element)
                


    #remove final comma
    sql_insert = sql_insert[:-1]
    sql_values = sql_values[:-1]

    sql_insert = sql_insert + ')'
    sql_values = sql_values + ')'

    #Execute sql and return identity of inserted record.
    sqlcursor.execute(sql_insert+sql_values, sql_parameters)
    sqlcursor.execute("SELECT @@IDENTITY")
    sqlcursor.commit()
    idrow = sqlcursor.fetchone()
    return idrow[0]

#Run program
couchbase_to_SQL(bucket_name);




