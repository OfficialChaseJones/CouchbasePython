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
table_name  = 'couchbase_table'

#This function will load a couchbase bucket in to a SQL Table
def couchbase_to_SQL(bucket_name,table_name ):

    #Connect to SQL Server
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost\SQLEXPRESS;'
                      'Database=PythonImport;'
                      'Trusted_Connection=yes;',autocommit=True)

    sqlcursor = conn.cursor()
    #cursor.execute('SELECT * FROM db_name.Table')

    cluster = Cluster('couchbase://localhost')
    authenticator = PasswordAuthenticator('user', 'password')
    cluster.authenticate(authenticator)
    c = cluster.open_bucket(bucket_name)

    q = N1QLQuery('SELECT * FROM `'+bucket_name+'` ')

    column_list = []

    sqlcursor.execute('IF (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \'dbo\' AND  TABLE_NAME = \''+table_name+'\')) drop table '+table_name)
    sqlcursor.execute('Create table '+table_name+' (ID int identity(1,1));')
    conn.commit()

    try:
        for row in cluster.n1ql_query(q):
            process_couchbase_row (row,column_list,table_name ,sqlcursor,conn)
    except:
        print('fail')
        print("Unexpected error:", sys.exc_info()[0])
        conn.rollback()
    
    conn.commit()


#Process row
#   1) Determine if table needs columns added
#   2) Run insert
def process_couchbase_row(row,column_list,table_name ,sqlcursor,conn):

    #print( row)
    rowvalues = row[bucket_name]

    sql_insert = 'INSERT INTO '+table_name +'('
    sql_values = 'VALUES ('
    sql_parameters = []
    
    for element in rowvalues:
        if(element not in column_list):
                column_list.append(element)
                sql_alter = 'Alter Table '+table_name
                sql_alter = sql_alter + ' ADD '+element+ ' NVARCHAR(MAX)'
                sqlcursor.execute(sql_alter)
                #print(sql_alter) 

        sql_insert = sql_insert + element + ','
        sql_values = sql_values + '?,'

        if(isinstance(rowvalues[element],dict)):
            sql_parameters.append(str((rowvalues)[element]))
        elif(isinstance(rowvalues[element],list)):
            if(len(rowvalues[element]) >0):
                sql_parameters.append(rowvalues[element][0])
            else:
                sql_parameters.append(None)
        else:
            sql_parameters.append(rowvalues[element])
        #print(element)
                


    #remove final comma
    sql_insert = sql_insert[:-1]
    sql_values = sql_values[:-1]

    sql_insert = sql_insert + ')'
    sql_values = sql_values + ')'

    #print(sql_insert)
    #print(sql_values)
    #print (row)

    sqlcursor.execute(sql_insert+sql_values, sql_parameters)
    #cnxn.commit()


#Run program
couchbase_to_SQL(bucket_name,table_name);

def try_to_change_list_contents(the_list):
    print('got', the_list)
    the_list.append('four')
    print('changed to', the_list)


