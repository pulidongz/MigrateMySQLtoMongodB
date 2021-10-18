import mysql.connector
import pymongo
import datetime
import enum

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class MsgType(enum.Enum):
    HEADER = 1
    OKBLUE = 2
    OKCYAN = 3
    OKGREEN = 4
    WARNING = 5
    FAIL = 6
    ENDC = 7
    BOLD = 8
    UNDERLINE = 9

#Pretty Print Function
def prettyprint(msg_text, msg_type):
    if msg_type == MsgType.HEADER:
        print(f"{bcolors.HEADER}{msg_text}{bcolors.ENDC}")
    elif msg_type == MsgType.OKBLUE:
        print(f"{bcolors.OKBLUE}{msg_text}{bcolors.ENDC}")
    elif msg_type == MsgType.OKCYAN:
        print(f"{bcolors.OKCYAN}{msg_text}{bcolors.ENDC}")
    elif msg_type == MsgType.OKGREEN:
        print(f"{bcolors.OKGREEN}{msg_text}{bcolors.ENDC}")
    elif msg_type == MsgType.WARNING:
        print(f"{bcolors.WARNING}{msg_text}{bcolors.ENDC}")
    elif msg_type == MsgType.FAIL:
        print(f"{bcolors.FAIL}{msg_text}{bcolors.ENDC}")
    elif msg_type == MsgType.BOLD:
        print(f"{bcolors.BOLD}{msg_text}{bcolors.ENDC}")
    elif msg_type == MsgType.UNDERLINE:
        print(f"{bcolors.UNDERLINE}{msg_text}{bcolors.ENDC}")

#Function migrate_table 
def migrate_table(db, table_name, delete_existing_documents):
    #TODO: Sanitize table name to conform to MongoDB Collection naming restrictions
    #For example, the $ sign is allowed in MySQL table names but not in MongoDB Collection names
    mycursor = db.cursor(dictionary=True)
    mycursor.execute("SELECT * FROM " + table_name + ";")
    myresult = mycursor.fetchall()

    mycol = mydb[table_name]

    if delete_existing_documents:
        #delete all documents in the collection
        mycol.delete_many({})

    #insert the documents
    if len(myresult) > 0:
        x = mycol.insert_many(myresult)
        return len(x.inserted_ids)
    else:
        return 0

begin_time = datetime.datetime.now()
abort = False
prettyprint(f"Script started at: {begin_time}", MsgType.HEADER)

### ***Input DB credentials***###
delete_existing_documents = True
mysql_host="localhost"
mysql_user="pul"
mysql_password="admin"
mongodb_host = "mongodb://localhost:27017/"

# START OF LOOP THROUGH MySQL SCHEMA
mysql_dbs = [
    {
        "delete_existing_documents": delete_existing_documents,
        "mysql_host": mysql_host,
        "mysql_database": "cbewsl_commons_db",
        "mysql_schema": "cbewsl_commons_db",
        "mysql_user": mysql_user,
        "mysql_password": mysql_password,
        "mongodb_dbname": "test_cbewsl_commons_db"
    },
    {
        "delete_existing_documents": delete_existing_documents,
        "mysql_host": mysql_host,
        "mysql_database": "cbewsl_mar_collections",
        "mysql_schema": "cbewsl_mar_collections",
        "mysql_user": mysql_user,
        "mysql_password": mysql_password,
        "mongodb_dbname": "test_cbewsl_mar_collections"
    },
    {
        "delete_existing_documents": delete_existing_documents,
        "mysql_host": mysql_host,
        "mysql_database": "cbewsl_umi_collections",
        "mysql_schema": "cbewsl_umi_collections",
        "mysql_user": mysql_user,
        "mysql_password": mysql_password,
        "mongodb_dbname": "test_cbewsl_umi_collections"
    },
    {
        "delete_existing_documents": delete_existing_documents,
        "mysql_host": mysql_host,
        "mysql_database": "comms_db",
        "mysql_schema": "comms_db",
        "mysql_user": mysql_user,
        "mysql_password": mysql_password,
        "mongodb_dbname": "test_comms_db"
    },
    {
        "delete_existing_documents": delete_existing_documents,
        "mysql_host": mysql_host,
        "mysql_database": "senslopedb",
        "mysql_schema": "senslopedb",
        "mysql_user": mysql_user,
        "mysql_password": mysql_password,
        "mongodb_dbname": "test_senslopedb"
    }
]

### START OF RUN SCRIPT ###
for dbs in mysql_dbs:

    if (dbs['delete_existing_documents']):
        prettyprint("\nExisting documents will be deleted from "+dbs['mysql_database']+" collections", MsgType.FAIL)
        
    #MySQL connection
    prettyprint("Connecting to MySQL server...", MsgType.HEADER)
    mysqldb = mysql.connector.connect(
        host=mysql_host,
        database=dbs['mysql_database'],
        user=mysql_user,
        password=mysql_password
    )
    prettyprint("Connection to MySQL Server succeeded.", MsgType.OKGREEN)

    #MongoDB connection
    prettyprint("Connecting to MongoDB server...", MsgType.HEADER)
    myclient = pymongo.MongoClient(mongodb_host)
    mydb = myclient[dbs['mongodb_dbname']]
    prettyprint("Connection to MongoDB Server succeeded.", MsgType.OKGREEN)

    #Start migration
    prettyprint("Migration started...", MsgType.HEADER)

    dblist = myclient.list_database_names()
    if dbs['mongodb_dbname'] in dblist:
        prettyprint("The database exists.", MsgType.OKBLUE)
    else:
        prettyprint("The database does not exist, it is being created.", MsgType.WARNING)

    #Iterate through the list of tables in the schema
    table_list_cursor = mysqldb.cursor()
    table_list_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name;", (dbs['mysql_schema'],))
    tables = table_list_cursor.fetchall()

    total_count = len(tables)
    success_count = 0
    fail_count = 0

    for table in tables:
        try:
            prettyprint(f"Processing table: {table[0]}...", MsgType.OKCYAN)
            inserted_count = migrate_table(mysqldb, table[0], dbs['delete_existing_documents'])
            success_count += 1
            prettyprint(f"Processing table: {table[0]} completed. {inserted_count} documents inserted.", MsgType.OKGREEN)
        except Exception as e:
            fail_count += 1
            prettyprint(f"{e}", MsgType.FAIL)

    prettyprint("Migration completed.", MsgType.HEADER)
    prettyprint(f"{success_count} of {total_count} tables migrated successfully.", MsgType.OKGREEN)
    if fail_count > 0:
        prettyprint(f"Migration of {fail_count} tables failed. See errors above.", MsgType.FAIL)
    
end_time = datetime.datetime.now()
prettyprint(f"\nScript completed at: {end_time}", MsgType.HEADER)
prettyprint(f"Total execution time: {end_time-begin_time}", MsgType.HEADER)
