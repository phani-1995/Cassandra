from cassandra.cluster import Cluster
clstr=Cluster()
session=clstr.connect('students')

# Creating a table data
def create_table():
    try:
        qry= '''
        create table data (
           studentID int,
           name text,
           age int,
           marks int,
           primary key(studentID)
        );'''
        session.execute(qry)
    except:
        print("Table not created sucessfully")


#Updating the age of a student
def update_user():
    # TO DO: execute SimpleStatement that updates the age of one user
    qry2 = 'UPDATE data SET age=19 WHERE studentID=5;'
    session.execute(qry2)

def load_data():
    try:
        qry1 = "copy data(studentID, name, age, marks) from '/home/phani/Cassandra/students.csv' with header = true;"
        session.execute(qry1)
        print("Data loaded successfully")
    except:
        print("Data was not loaded")

#Deleting a age of one student
def del_stud():
    try:
        qry3 = ' DELETE age FROM data WHERE studentID=3;'
        session.execute(qry3)
        print("Deleted sucessfully")
    except:
        print("Data was not deleted")


if __name__ == "__main__":
    create_table()
    load_data()
    update_user()
    del_stud()









