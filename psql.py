import psycopg2

class MyDatabase():
    #Establishing connection to the database
    def __init__(self, db="Users", user="lubwama" , password ="lubwama1"):
        self.conn = psycopg2.connect(database=db, user=user, password=password)
        self.cur = self.conn.cursor()

    #The following  Function creates the user  table
    def create_users(self,sql_statement):
        self.cur.execute(sql_statement)
        print("Table created successfully")

    #The following function  creates the categories table
    def create_categories(self,sql_statement):
        self.cur.execute(sql_statement)
        return "Table created successfully"

    #The following function creates the tasks table
    def create_tasks(self,sql_statement):
        self.cursor.execute(sql_statement)
        return "Table created successfully"

    #The following function adds values  to the users table
    def add_users(self,sql_statement):
        self.cursor.execute(sql_statement)
        print("Records inserted successfully into the table")

    #The following function adds values  to the categories table
    def add_categories(self,sql_statement):
        self.cursor.execute(sql_statement)
        print("Records inserted successfully into the table")

     #The following function adds values  to the tasks table
    def add_tasks(self,sql_statement):
        self.cursor.execute(sql_statement)
        print("Records inserted successfully into the table")
    
    #The following  function queries the tasks and Users table
    def get_user_tasks(self,query):
        self.cursor.execute(query)
        user_tasks = self.cur.fetchall()
        print(user_tasks)

    #The following function queries the user table
    def get_user_by_id(self,query):
        self.cursor.execute(query)
        user = self.cur.fetchone()
        print(user)
    
    #The following queries categries using category id
    def get_category_by_id(self,query):
        self.cursor.execute(query)
        category = self.cur.fetchone()
        return category
    
    #The following function queries categries using task id
    def get_task_by_id(self,query): 
        self.cur.execute(query)
        task= self.cur.fetchone()
        return task
    
    #The following function queries the Users table
    def get_all_users(self,query):
        self.cur.execute(query)
        users = self.cur.fetchall()
        return users

    #The following function queries the Categories table
    def get_all_categories(self,query):
        self.cur.execute(query)
        users = self.cur.fetchall()
        return users
     
    #The following function queries the Tasks table
    def get_all_Tasks(self,query):
        self.cur.execute(query)
        tasks = self.cur.fetchall()
        return tasks
     
    #The following function updates details in users table
    def update_user_data(self,sql_statement):
        self.cur.execute(sql_statement)
        return "Table successfully updated"

    #The following function updates details in tasks table
    def update_user_tasks(self,sql_statement):
        self.cursor.execute(sql_statement)
        return "Table successfully updated"

     #The following function updates details in category table
    def update_user_category(self,sql_statement):
        self.cur.execute(sql_statement)
        return "Table successfully updated"

  
    def query(self, query):
        self.cur.execute(query)
        print("data successfully retrieved")

    def close(self):
        self.cur.close()
        self.conn.close()



        
if __name__ == '__main__':
  db = MyDatabase()
  db.query("SELECT * FROM Categories;")
  db.create_users("CREATE TABLE Users(user_id  VARCHAR(20) PRIMARY KEY,first_name VARCHAR(255) NOT NULL,last_name VARCHAR(255) NOT NULL,age int NOT NULL,email VARCHAR(255) NOT NULL,passw VARCHAR(255) NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
  db.create_categories("CREATE TABLE Categories(category_id VARCHAR(20) PRIMARY KEY,category_name VARCHAR(255) NOT NULL,title VARCHAR(55), descriptions VARCHAR(255),created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
  db.create_tasks("CREATE TABLE Tasks(task_id serial PRIMARY KEY,user_id INTEGER REFERENCES Users(user_id),category_id INTEGER REFERENCES Categories(category_id),title VARCHAR(255),description VARCHAR(255),done VARCHAR(50),created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
  db.add_users("INSERT INTO Users(user_id,first_name,last_name,age,email,passw) VALUES('U004', 'Lubwama','Isaac',14,'lubwama73@gmail.com','lubwama1')")
  db.add_categories("INSERT INTO Categories(category_id,category_name,title,descriptions) VALUES('C006','Engineering','Software Developer','develops software apps'")
  db.add_tasks("INSERT INTO Tasks(user_id,category_id,title,desciptions,done) VALUES('U003','C004','civil Engineer','constructing Roads','Pending')")
  db.get_user_tasks("SELECT * FROM Users INNER JOIN Tasks ON (Users.user_id = Tasks.task_id)")
  db.get_user_by_id("SELECT * FROM Users WHERE user_id = {}".format('U004'))
  db.get_category_by_id("SELECT * FROM Categories WHERE category_id = {}".format('C004'))
  db.get_task_by_id("SELECT * FROM  Tasks WHERE user_id = {}".format(7))
  db.get_all_users("SELECT * FROM Users;")
  db.get_all_categories("SELECT * FROM Categories;")
  db.get_user_tasks("SELECT * FROM Tasks;")
  db.update_user_data("UPDATE  Users SET email = 'beja62@gmail.com',age = 15 WHERE user_id = {} ".format("U009"))
  db.update_user_category("UPDATE Categories SET title = 'Analyst' WHERE category_id = 'C006';")
  db.update_user_tasks("UPDATE Tasks SET done = 'finished' WHERE task_id = 23;")

  db.close("UPDATE Tasks SET done = 'finished' WHERE task_id = 23")




