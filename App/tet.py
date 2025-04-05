# friends = "Kemi"
# username = input("Please enter name: ")
# if username == friends:
#    print("correct username")
# else:
#    print ("Incorrect username")
   
   
# students = [
#     {"name": "Alice", "age": 20, "grade": "A"},
#     {"name": "Bob", "age": 21, "grade": "B"},
#     {"name": "Charlie", "age": 19, "grade": "C"},
#     {"name": "David", "age": 22, "grade": "B"},
#     {"name": "Eve", "age": 20, "grade": "A"}
# ]

# for name,age,grade in students.values:
#     # name = stud["name"]
#     # age = stud["age"]
#     # # if name == "Alice":
#     print (f"My name is : {name}, i'm {age}. my grade:{grade}")
    
    
    
# name = "Nathan"
    
# # greetings = f"Hey, i'm {name}"

# greetings = "Hey, i'm {}"

# with_name = greetings.format(name)
    
# print (with_name)


# size_input = int(input("How big is your house(in square feet):"))
# square_meters = size_input / 10.8
# print(f"square feet is : {square_meters:.3f}")


# collections in python (list, turples, sets)

# lst = ["bob","nathan","bimbo"]
# turps = ("bob","nathan","bimbo")
# sets = {"bob","nathan","bimbo"}

# lst.append("Kemi")
# lst.remove("bimbo")
# # print(f"Element value in list: {lst[2]}")
# print(lst)

# turp = set(turps)
# turp.remove("bimbo")

# print(turp)

# # print(lst[0])

# Advance set operators 

# friends = {"bob","nathan","bimbo"}
# abroad = {"nathan","bimbo"}

# local_friend = friends.difference(abroad)

# print(local_friend)

# number = [10,6,3,5]

# user_input = input("Please enter 'Y' if would like to Play dice game..:").upper()
# print(f"user value : {user_input}")

# if user_input == "Y":
#    guess_number = int(input("Please guess dice number: "))
#    if guess_number in number:
#        print("You are correct. congrat..")
#    else:
#        print("You are incorrect")
# else: 
#     print("I'm not interested..")
    
    
# names = ["bob","nathan","bimbo", "Samantha"]

# # start_s = []
# start_s = [name for name in names if name.lower().startswith("s")]

# # for name in names:
# #     if name.startswith("n"):
# #        start_s.append(name)

# print(start_s)

# people = [
# 	# ("Bob", 42),
# 	("James", 24, "Artist"),
# 	("Harry", 32, "Lecturer")
# ]

# # for name, age, profession in people:
# # 	print(f"Name: {name}, Age: {age}, Profession: {profession}")

# for person in people:
# 	print(f"Name: {person[0]}, Age: {person[1]}, Profession: {person[2]}")
 
 
# def Netincome():
#     try:
#           GrossIncome = int(input("Please enter Monthly Gross income... "))
#           Income = GrossIncome * 12
          
#           if GrossIncome <= 0:
#              print (f"Invalid income entered..")
#              return 
        
#           if Income  > 120000:
#              Taxpay = Income * 0.32
#           else:
#               Taxpay = Income * 0.12
            
#           Netincome = Income - Taxpay 
            
#           print (f"Gross annual income: {Income} ...")
#           print (f"Your Annual Net income: {Netincome} ...")
#           print (f"Tax deduction: {Taxpay} ...")
        
#     except ValueError:
#           print("Invalid input. Please enter a numeric value.")
        
# Netincome()


# def net_income():
#     try:
#         gross_income = float(input("Please enter your Annual Gross Income: "))  
        
#         if gross_income <= 0:
#             print("Invalid income entered.")
#             return
        
#         # Apply tax rates
#         if gross_income > 120000:
#             tax_rate = 0.32
#         else:
#             tax_rate = 0.12
        
#         tax_paid = gross_income * tax_rate
#         net_income = gross_income - tax_paid

#         print(f"Gross Annual Income: {gross_income:.2f}")
#         print(f"Annual Net Income: {net_income:.2f}")
# #         print(f"Tax Deducted: {tax_paid:.2f}")

# #     except ValueError:
# #         print("Invalid input. Please enter a numeric value.")

# # net_income()

# def divide(dividend, divisor=5):
    
#     if divisor != 0:
#        return f"result: {(dividend * divisor)}"
#     else:
#         return "You are fool.."
#     # print( f"result: {(dividend / divisor)}")

# result = divide(15,divisor=4)
# print(result)

# def return_42():
#     # print(4)
#     return 42
    
# result = return_42()
# print(result)
# def add(x, y):
    
#     # print(x + y)
#     return x + y
    
# # add(5,6)

# result = add(5,6)

# print(result)


# result = lambda x,y: x + y
# print(result(4,5))

# user_list = [
#     ("john_doe", "pass123"),
#     ("alice", "sec"),     # password too short
#     ("bob", "qwerty")
# ]

# userinfo = {user[0]: user for user in user_list}
# print(userinfo["bob"])


# student = {
#     "name": "John",
#     "school": "Training",     
#     "grades" : (23,34,20,10)
# }

# def average_grades(student):
#     grades = student["grades"]
#     return sum(grades) / len(grades)

# avg_grade = average_grades(student)
# print(f"Average grade: {avg_grade:.2f}")


# def average_all_students(student):
#     total = 0
#     count = 0
#     for stud in student:
#         total += sum(stud["grades"])
#         count += len(stud["grades"])
        
#     return f"Average grades all student: {(total / count)}"

# print(average_all_students(student))

    
    
# print(multiply(1,2,3))

# def add(*args):
    
#     print(args)

# print(add(1,2,3))


# def add(x, y):
    
#     return x + y
 
# # num = [2, 3]  
# num = { "x": 13 , "y": 12}

# # result = add(*num)
# # print(add(y=num["y"], x=num["x"]))
# print(add(**num))

# def multiply(*args):
#     total = 2

#     for arg in args:
#         total = total * arg
#     return total 

# result = multiply(3,4)
# print(multiply(3,4))


# def apply(*args, operator):
    
#     if operator == "*":
#        return multiply(*args)
#     elif operator == "+":
#         return sum(args)
#     else: 
#         return "No valid operator apply"
       
# print(apply(1,2,3,4, operator="*"))


# def named(**kwargs):
#     print(kwargs)
    
# named(name="ufuoma", age =25)

# def named(**kwargs ):
#     print (kwargs )
    
# # details = {"name":"ufuoma", "age" : 25}

# # named(**details)

# def print_nicely(**kwargs):
#     named(**kwargs)
    
#     for arg, value in kwargs.items():
#         print(f"{arg} : {value}")

# print_nicely(name="bob", age=25)


#### Object Oriented Programming 

# class Student:
#     def __init__(self,name,grades):
#         self.name = name
#         self.grade = grades
    
#     def __str__(self):
#         return f"Student : {self.name} Total grades : {sum(self.grade)} " 
    
#     # def __str__(self):
#     #     return f"<Student : {self.name} >" 
    
#     def average_grade(self):
#         return f"Average grade : {sum(self.grade) / len(self.grade)}"

# students = Student("Ufuoma",(50,60.10))
# print(f"Name : {students.name} : {students.average_grade()}")
# print(students)

# class Store:
#       def __init__(self,name):
#           self.name = name
#           self.items = []
       
#       def add_items(self,name,price):
#         #   self.name = name
#           get_items ={
#                     "Name" : name,
#                     "Price": price
#                     } 
#           self.items.append(get_items)
#         #   return  self.items
     
#       def stock_price(self):
#         #   totalprice = 0
#         #   for item in self.items:
#         #       totalprice = totalprice + sum(item['Price'])
#         #       print (f"Name : {item['Name']}, Price : {totalprice}")
#         # Other method
#         totalprice = [sum(item['Price']) for item in self.items]
#         return totalprice
        
         

# stores = Store("Nathan")
# add_store = stores.add_items(stores.name, (25,55,77) )
# stock = stores.stock_price()
# print(f"Items: {stock}")
# # print(f"Items Name: {stock} ")


# get_items =[{
#                     "Name" : "Nathan",
#                     "Price": (25,55,77)
#                     } ]
# for item in get_items:
#     print(f" Name : {item['Name']} , Price : {item['Price']}  ")


# class Classtest:
#       def instance_method(self):
#         #   self.program = program
#           print(f"Welcome to Programing : {self}")
          
#       @classmethod
#       def class_method(cls,store):
#           cls.store = store
#           return cls.store + " - franchise"
          
#       @staticmethod
#       def static_method():
#           print("welcome to static method..")
      
          
# test = Classtest.class_method('Mcdonald')
# print(test)



# class Store:
#       def __init__(self,name):
#           self.name = name
#           self.items = []
       
#       def add_items(self,name,price):
#           self.items.append ({
#                     "name" : name,
#                     "price": price
#                     } )
     
#       def stock_price(self):
#            totalprice = 0
#            for item in self.items:
#                totalprice += item['price']
#            return totalprice 
       
#       @classmethod
#       def franchise(cls,store):
#           return f"Store name : {store.name} " + " - franchise" 
      
#       @staticmethod()
#       def store_details(store):
#           return f" Name: {store.name} , Total Stock Price: {store.stock_price()}"

#       @classmethod
#       def franchise(cls, store):
#           return f"Store name: {store.name} - Franchise"

#       @staticmethod
#       def store_details(store):
#            return f"Name: {store.name}, Total Stock Price: {store.stock_price()}"

# store = Store('McDonald')
# # add store 
# add_store = store.add_items("KFC", 160 )
# # Get stock price
# stockprice = store.stock_price()

# print(Store.franchise(store))

# print(Store.store_details(store))


# class Store:
#     def __init__(self, name):
#         self.name = name
#         self.items = []  # List to store items

#     def add_items(self, name, price):
#         self.items.append({
#             "name": name,
#             "price": price
#         })

#     def stock_price(self):
#         totalprice = sum(item['price'] for item in self.items)
#         return totalprice

#     @classmethod
#     def franchise(cls, store):
#         return f"Store name: {store.name} - Franchise"

#     @staticmethod
#     def store_details(store):
#         return f"Name: {store.name}, Total Stock Price: {store.stock_price()}"

# # Create store instance
# store = Store("McDonald's")

# # Add an item to the store
# store.add_items("Burger", 160)

# # Get stock price
# stockprice = store.stock_price()

# # Print franchise info
# print(Store.franchise(store))

# # Print store details
# print(Store.store_details(store))


# class Device:
      
#       def __init__(self, name, connected_by):
#            self.name = name 
#            self.connected_by = connected_by
#            self.connected = True
           
#       def __str__(self):
#            return f"Device {self.name!r} ({self.connected_by})"
       
#       def disconnect(self):
#           self.connected == False
#           print("You Disconnected..")
          
          
# # devices = Device("Printed","USB")
# # print(devices)
# # devices.disconnect()

# class Printer(Device):
     
#      def __init__(self, name, connected_by, capacity):
#           super().__init__(name, connected_by)
#           self.capacity = capacity
#           self.remaining_pages = capacity
          
#      def __str__(self):
#           return f"{super().__str__()} {self.remaining_pages} remaining pages.."
      
#      def print(self, pages):
#          if not self.connected:
#              print("Your device is not connected..")
#              return
#          print(f"Printing {pages} pages")
#          self.remaining_pages -= pages
# printer = Printer("Printer", "USB", 500)
# printer.print(50)
# print(printer)
# printer.disconnect()


# def divide(dividend, divisor):
#     if divisor == 0:
#         # print("Divisor cannot be 0..")
#         # return
#         raise ZeroDivisionError("Divisor cannot be 0..")
    
#     return dividend /divisor

# grades =[23,456,678]

# print("Welcome to average grades programming..")

# try:
#     average_grades = divide(sum(grades), len(grades))
#     print(f"The Average grades is {average_grades:.2f}")
# except ZeroDivisionError as e:
#       print(e)
#       print("There are no grades in your list..")
# else:
#     print("All student grades calcultated..")
# finally:
#       print("Divide Calculation ended..")
      
#### This is  first class function...

# def divide(dividend, divisor):
#     if divisor == 0:
#         raise ZeroDivisionError("Divisor cannot be 0..")
    
#     return dividend / divisor

# def calculate(*values, operator):
#     return operator(*values)

# result = calculate(23,34, operator=divide)
# print(result)

# def raisevalueerror():
#     pass      
     
# def student(name, grade):
#     return f"Student name: {name}, grade: {grade}"


# studentname = [
#     {"name": "Alice", "age": 20, "grade": "A"},
#     {"name": "Bob", "age": 21, "grade": "B"},
#     {"name": "Charlie", "age": 19, "grade": "C"},
#     {"name": "David", "age": 22, "grade": "B"},
#     {"name": "Eve", "age": 20, "grade": "A"}
# ]

# def searchname(studentname):
#     getname = [stud for stud in studentname if stud["grade"] == "A"]
#     print(getname)
#     # return f" name : {getname['name']} Age: {getname['age']}"

# result = searchname(studentname)
# print(result)



# def stud_grade(func, *values):
#     return func(*values)

# result = stud_grade(student,"Nathan",(25,25))
# print(result)

      
# import aiosmtplib
# from email.message import EmailMessage
# import asyncio  # Required to run the async function manually

# # ✅ Correct Outlook SMTP settings
# SMTP_SERVER = "smtp.gmail.com"
# SMTP_PORT = 587
# SMTP_USERNAME = "matovie22@gmail.com"
# SMTP_PASSWORD = "egbveryflhexedon"  # Use App Password if needed

# async def send_email_alert(subject: str, body: str, to_email: str):
#     msg = EmailMessage()
#     msg["From"] = SMTP_USERNAME
#     msg["To"] = to_email
#     msg["Subject"] = subject
#     msg.set_content(body)

#     try:
#         await aiosmtplib.send(
#             msg,
#             hostname=SMTP_SERVER,
#             port=SMTP_PORT,
#             start_tls=True,
#             username=SMTP_USERNAME,
#             password=SMTP_PASSWORD,
#         )
#         print("✅ Email alert sent successfully!")
#     except Exception as e:
#         print(f"❌ Error sending email: {e}")
        
# subject = "Bank Alert: Transaction Notification"
# to_email = "ufuomaodibo@hotmail.com"
# body = f"""
#             Dear customer,

#             A transaction has been made on your account

#             If this was not you, please contact our support team immediately.

#             Thank you,
#             Your Bank
#             """
# # http://localhost:8000/docs#/
# # Run the function manually for testing
# asyncio.run(send_email_alert(subject, body, to_email))

from datetime import datetime
# from fastapi import APIRouter, HTTPException
# from model.transaction import Transaction
# from dto.transaction import TransactionCreate
# from model.user import User
# from utils.email import send_email_alert
# from datetime import datetime

# router = APIRouter()

def prepare_transaction_email(user, new_transaction):
    
        to_email = user
        body = f"""   Dear ufuoma,

        A transaction has been made on your account
        
        
        Transaction Type: {new_transaction['type']}
        Amount: ${new_transaction['amount']:.2f}
        Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
       
        If this was not you, please contact our support team immediately.

        Thank you,
        Netbank Plc
        """
        return {to_email, body }
    
transaction ={
    "type" : "deposit",
    "amount": 200
}

print(prepare_transaction_email("natha@mail.com",transaction ))
    




    

