import sqlite3

data=[{'Name':'ankit','Institute':'ISI','Marks':0},
      {'Name':'kiio','Institute':'IIIT-D','Marks':100},
      {'Name':'Summi','Institute':'ECIL','Marks':100}]

# Full Data Load
def full_data_load(data):
    conn=sqlite3.connect('MyData.db') #connect to the database
    cursor=conn.cursor()
    # truncate the existing records
    cursor.execute("DELETE FROM MyData")
    #insert new data
    for record in data:
        cursor.execute("insert into MyData values (?,?,?)",(record['Name'],record['Institute'],record['Marks']))
    conn.commit()
    conn.close()

full_data_load(data)


# Incremental Data Load
data=[{'Name':'ankit','Institute':'ISI','Marks':0},{'Name':'Summi','Institute':'NA','Marks':100}]
def incremental_data_load(data):
    conn = sqlite3.connect('MyData')  # connect to the database
    cursor = conn.cursor()
    # truncate the existing records
    cursor.execute("DELETE FROM MyData.db")
    # insert new data
    for record in data:
        cursor.execute("insert into MyData values (?,?,?)", (record['Name'], record['Institute'], record['Marks']))
    conn.commit()
    conn.close()

incremental_data_load(data)