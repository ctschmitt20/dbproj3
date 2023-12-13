import csv
from mysql.connector import connect, Error
import re

import chardet
import glob



def main():
        #   uncomment the three lines of code below before running functions if you want to update SQL
    #   table... right now I have the randomized data stored in SQL, and no point in recreating
        #   the data every time I run the program

    make_schedule_table()
    make_student_table()
    make_enrollment_table()

    getClassData()

#Part 2 functions in action:

    # select_all_table("schedule")
    #select_all_byDpt("PSY")
    # select_all_byStartTime("9:00 AM")
    # select_all_social_sciences()
    # select_all_DCP()
    # select_all_byProf("S Hughes")

# Part 3 code
#     insert_student_table(1802380, "Cael", "Schmitt", "04", "CS", "BUS", "DS", "S Hughes")
#     insert_student_table(1802381, "Carl", "Smith", "04", "PSY", "", "", "L Atwater")
#     insert_student_table(1802382, "Stephen", "Hughes", "01", "BIO", "", "HIS", "P Carstens")
#     insert_student_table(1802383, "Bill", "Bamba", "02", "PHY", "BUS", "PSY", "M Stobb")
#     insert_student_table(1802384, "Barry", "Miller", "03", "ECO", "BUS", "DS", "D Westberg")
#
#     insert_enrollment_table(1, 1, "Active")
#     insert_enrollment_table(1, 2, "Waitlist")
#     insert_enrollment_table(1, 3, "Complete")
#     insert_enrollment_table(1, 4, "Active")
#
#     insert_enrollment_table(2, 5, "Active")
#     insert_enrollment_table(2, 6, "Waitlist")
#     insert_enrollment_table(2, 7, "Complete")
#     insert_enrollment_table(2, 8, "Active")
#
#     insert_enrollment_table(3, 5, "Active")
#     insert_enrollment_table(3, 2, "Waitlist")
#     insert_enrollment_table(3, 3, "Complete")
#     insert_enrollment_table(3, 1, "Active")
#
#     insert_enrollment_table(4, 1, "Active")
#     insert_enrollment_table(4, 12, "Waitlist")
#     insert_enrollment_table(4, 3, "Complete")
#     insert_enrollment_table(4, 9, "Active")

    # select_all_table("student")
    # select_all_table("enrollment")

# Part 4 Code
    randomizeData()

        # confirmation of tables:
    # select_all_table("enrollment")
    # select_all_table("student")
    select_all_table("schedule")

        # first three functions
    # get_num_students()
    # get_num_classes()
    # get_seniors()
        # three functions using two or more tables:
    # student_schedule("1")
    # class_enrollment("1")
    # major_of_interest("CS")

    insert_enrollment_table("1", "1", "Active")
    insert_enrollment_table("1", "2", "Active")
    insert_enrollment_table("1", "7", "Active")
    insert_enrollment_table("1", "3", "Active")
    insert_enrollment_table("1", "7", "Active")

def getClassData():
    numRE = re.compile(r"[0-9]+")
    prefixRE = re.compile(r"[A-Z]+")

    file = open("Course Schedule.csv")
    csvreader = csv.reader(file)
    rows = []
    for row in csvreader:
        rows.append(row)
        # print(repr(row))
    # print(len(rows))

    iterator = 1
    while iterator < len(rows):
        wanted = True
        blank_count = 0
        for i in rows[iterator]:
            # print(repr(i))
            if i == '':
                blank_count += 1
            # print(blank_count)
            if blank_count == 6 or i == "Course Number/Title" or rows[iterator][0] == '':
                wanted = False

        if wanted == True:
            # print("here")
            lab = False
            writing_emp = False
            wanted = True
            className = ""
            info = rows[iterator][0]
            # print(repr(info))
            originalInfo = info
            # info = info[0:13]
            info = info.split()
            # print(repr(info))
            nums = numRE.findall(info[0])
            if len(nums) != 0:
                dpt = prefixRE.findall(info[0])
                department = dpt[0]
                if "L" in info[0]:
                    lab = True
                courseNum = nums[0]

                if "W" in info[1]:
                    writing_emp = True
                    section = info[1].strip("W")
                else:
                    section = info[1]
                n = 2
                while n < len(info):
                    className = className + info[n] + " "
                    n += 1
            else:
                department = info[0]
                if "L" in info[1]:
                    lab = True
                    courseNum = info[1].strip("L")
                else:
                    courseNum = info[1]

                if "W" in info[2]:
                    writing_emp = True
                    section = info[2].strip("W")
                else:
                    section = info[2]
                n = 3
                while n < len(info):
                    className = className + info[n] + " "
                    n += 1
            professor = rows[iterator][1]
            days = rows[iterator][2]
            start = rows[iterator][3]
            end = rows[iterator][4]
            building = rows[iterator][5]
            credits = rows[iterator][6]
            preReq = ""
            try:
                preReq = rows[iterator][7]
            except:
                Error
            year = "2021"
            term = "Fall"
            # print(department, courseNum, section, lab, writing_emp, className, professor, days, start, end,
            #       building, credits, term, year)

            # SQL code:
            insert_schedule_table(department, courseNum, section, lab, writing_emp, className, professor, days, start,
                                  end,
                                  building, credits, term, year, preReq)
            # select_all_table("schedule") #shouldn't be here... should be below

        iterator += 1



def establish_connection():
    try:
        mydb = connect(
            host="localhost",
            user="root",
            password="Jordan23",  # Replace with yours
            database="DS230"
        )
    except Error as e:
        print(e)
    return mydb

def make_schedule_table():
    # print("Class Table")
    mydb = establish_connection()
    mycursor = mydb.cursor()

    string = """DROP table if exists schedule;
                
                CREATE TABLE schedule (
                    class_id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
                    department VARCHAR(10),
                    course_num VARCHAR(5),
                    section   VARCHAR(5),
                    lab VARCHAR(5),
                    writing_emp VARCHAR(5),
                    class_name   VARCHAR(80),
                    professor VARCHAR(50),
                    days VARCHAR(5),
                    start   VARCHAR(10),
                    end     VARCHAR(10),
                    building VARCHAR(30),
                    credits VARCHAR(5),
                    term   VARCHAR(10),
                    year   VARCHAR(4),
                    preReq VARCHAR(80)
                    
                );"""
    # print(string)
    try:
        mycursor.execute(string, multi= True)

        # mydb.commit()
    # for result in mycursor.fetchall():
    # 	print(result)
    except Error as e:
        print(e)


def insert_schedule_table(department, courseNum, section, lab, writing_emp, className, professor, days, start, end,
                  building, credits, term, year, preReq):
    # print("Inserting class")
    mydb = establish_connection()
    mycursor = mydb.cursor()

    string = """INSERT INTO schedule
                    (department, course_num, section, lab, writing_emp, class_name, professor, days,
                    start, end, building, credits, term, year, preReq)
                    VALUES"""

    values = department, courseNum, section, lab, writing_emp, className, professor, days, \
        start, end, building, credits, term, year, preReq
    string += str(values)
    # print(string)
    try:
        mycursor.execute(string)
        mydb.commit()
    # for result in mycursor.fetchall():
    # 	print(result)
    except Error as e:
        print(e)

def select_all_table(table_name):
    print("Selecting all from", table_name, "table")
    mydb = establish_connection()
    mycursor = mydb.cursor();
    query = "SELECT * FROM "
    query += table_name

    # print(query)
    try:
        mycursor.execute(query);
        for result in mycursor.fetchall():
            print(result)
    except Error as e:
        print(e)

#Part 2 functions:

def select_all_byDpt(department):
    mydb = establish_connection()
    mycursor = mydb.cursor();
    query = "SELECT * FROM schedule"
    query += "\nWHERE department = \"" + department +"\""

    # print(query)
    try:
        mycursor.execute(query);
        for result in mycursor.fetchall():
            print(result)
    except Error as e:
        print(e)

def select_all_byStartTime(start_time):
    mydb = establish_connection()
    mycursor = mydb.cursor();
    query = "SELECT * FROM schedule"
    query += "\nWHERE start = \"" + start_time +"\""

    print(query)
    try:
        mycursor.execute(query);
        for result in mycursor.fetchall():
            print(result)
    except Error as e:
        print(e)

def select_all_social_sciences():
    mydb = establish_connection()
    mycursor = mydb.cursor();
    query = "SELECT * FROM schedule"
    query += """\nWHERE
                    ( 
                    department="ANT" OR department="ECO" OR department="POL" OR department="PSY" or department="SOC"
                    )
             """

    # print(query)
    try:
        mycursor.execute(query);
        for result in mycursor.fetchall():
            print(result)
    except Error as e:
        print(e)

def select_all_DCP():
    mydb = establish_connection()
    mycursor = mydb.cursor();
    query = "SELECT * FROM schedule"
    query += """\nWHERE
                    ( 
                    SUBSTRING(course_num, 3, 1) = "6" OR SUBSTRING(course_num, 3, 1) = "7" OR SUBSTRING(course_num, 3, 1) = "8"
                    )
             """

    # print(query)
    try:
        mycursor.execute(query);
        for result in mycursor.fetchall():
            print(result)
    except Error as e:
        print(e)

def select_all_byProf(professor):
    mydb = establish_connection()
    mycursor = mydb.cursor();
    query = "SELECT * FROM schedule"
    query += "\nWHERE professor = \"" + professor +"\""

    # print(query)
    try:
        mycursor.execute(query);
        for result in mycursor.fetchall():
            print(result)
    except Error as e:
        print(e)

#Part 3 functions
def make_student_table():
    mydb = establish_connection()
    mycursor = mydb.cursor()


    string = """DROP table if exists student;

                CREATE TABLE student (
                    student_id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
                    student_number INT,
                    fname VARCHAR(30),
                    lname   VARCHAR(30),
                    class_year VARCHAR(15),
                    major1 VARCHAR(50),
                    major2   VARCHAR(50),
                    minor VARCHAR(50),
                    advisor VARCHAR(50)
                );"""
    try:
        mycursor.execute(string, multi=True)

        # mydb.commit()
    # for result in mycursor.fetchall():
    # 	print(result)
    except Error as e:
        print(e)


def insert_student_table(student_number, fname, lname, class_year, major1, major2, minor, advisor):
    # print("Inserting class")
    mydb = establish_connection()
    mycursor = mydb.cursor()

    string = """INSERT INTO student
                    (student_number, fname, lname, class_year, major1, major2, minor, advisor)
                    VALUES"""

    values = student_number, fname, lname, class_year, major1, major2, minor, advisor
    string += str(values)
    # print(string)
    try:
        mycursor.execute(string)
        mydb.commit()
    # for result in mycursor.fetchall():
    # 	print(result)
    except Error as e:
        print(e)

def make_enrollment_table():
    mydb = establish_connection()
    mycursor = mydb.cursor()


    string = """DROP table if exists enrollment;

                CREATE TABLE enrollment (
                    student_id INT NOT NULL,
                    course_id INT NOT NULL,
                    status ENUM('Active', 'WaitList', 'Complete'),
                    classSize INT,
                    PRIMARY KEY (student_id , course_id)
                );"""
    try:
        mycursor.execute(string, multi=True)

        # mydb.commit()
    # for result in mycursor.fetchall():
    # 	print(result)
    except Error as e:
        print(e)

def insert_enrollment_table(student_id, course_id, status):
    # print("Inserting class")
    mydb = establish_connection()
    mycursor = mydb.cursor()


    # add enrolled to the schedule
    # query for the enrolled number of the schedule for a class
    # query for the cap (classSize)
    # if enrollled number < classSize
    #   call this function with status as active
    #   else call this cuntion with status as waitlist

    # need add a check for a preReq   ------> last part!
    mydb = establish_connection()
    mycursor = mydb.cursor();
    query = "SELECT preReq FROM schedule"

    # tidy this line up
    query += "\nWHERE class_id = \"" + str(course_id) + "\""
    # print(query)

    needPreReq = False
    # print(query)
    try:
        mycursor.execute(query);
        for result in mycursor.fetchall():
            if result[0]=="":
                print("No preReq")
            else:
                # print("preReq: ", str(result[0]))
                needPreReq= True
                preReq = result[0]

            # print("course's preReq: ", str(result))
    except Error as e:
        print(e)

    found = False
    if needPreReq==True:
        mydb = establish_connection()
        mycursor = mydb.cursor();
        query = "SELECT course_id FROM enrollment"
        query += "\nWHERE student_id = \"" + str(student_id) + "\""
        try:
            mycursor.execute(query);
            for result in mycursor.fetchall():
                # print(repr(result[0]))
                # print(repr(preReq))
                if str(result[0])== preReq:
                    found = True
                    print("Required pre-requisite found! Enrolling student in the class")

            if (found==False):
                print("This student hasn't taken the required preReq of", preReq, "! Access denied")
                return
        except Error as e:
            print(e)

    if (needPreReq==False) or (needPreReq==True and found==True):
        string = """INSERT INTO enrollment
                        (student_id, course_id, status, classSize)
                        VALUES"""
        classSize = 100
        values = student_id, course_id, status, classSize
        string += str(values)
        # print(string)
        try:
            mycursor.execute(string)
            mydb.commit()
        # for result in mycursor.fetchall():
        # 	print(result)
        except Error as e:
            print(e)

# Part 4 function
def randomizeData():
    file = open("Registration.csv", encoding="Windows-1252")
    # code for figuring out the encoding above:
        # with open("Registration.csv", 'rb') as rawdata:
        #     result = chardet.detect(rawdata.read())
        # print(result['encoding'])
    csvreader = csv.reader(file)
    rows = []
    for row in csvreader:
        rows.append(row)

    fnList= []
    lnList = []

    skip =1
    for r in rows:
        if skip==1:
            skip+=1
            continue
        # print(r)
        # info = r.split(",")
        # print(info)
        if len(r)< 2:
            continue
        else:
            fnList.append(r[2])
            lnList.append(r[1])

    idDict = {}
    student_number = 1802300
    namesIt = len(fnList)-1

    class_dpt = ""
    class_num = ""
    class_section = ""

    skip =1
    for r in rows:
        if skip==1:
            skip+=1
            continue
        # info = r.split(",")
        if len(r)< 4:
            # print(r)
            classInfo = r[0].split()
            class_dpt = classInfo[0]
            class_num = classInfo[1]
            class_section = classInfo[2]
        else:
            # print(r)
            if (r[0] in idDict):
                # get student primary key
                stud_num = idDict[r[0]]
                # continue
            else:
                idDict[r[0]] = student_number
                fname= fnList[namesIt]
                lname= lnList[namesIt]
                class_year = r[3]
                major1 = r[4]
                major2 = r[5]
                minor  = r[6]
                advisor = r[7]
                insert_student_table(student_number, fname, lname, class_year, major1, major2, minor, advisor)

                stud_num = student_number
                namesIt -= 1
                student_number += 23

            #get course id
            mydb = establish_connection()
            mycursor = mydb.cursor();
            query = "SELECT * FROM schedule"

            #tidy this line up
            query += "\nWHERE department = \"" + class_dpt + "\" AND course_num = \"" + class_num + "\" AND section = \"" + class_section + "\""
            # print(query)

            # print(query)
            try:
                mycursor.execute(query);
                for result in mycursor.fetchall():
                    # print(result)
                    course_id = result[0]
            except Error as e:
                print(e)

            mydb = establish_connection()
            mycursor = mydb.cursor();
            query = "SELECT student_id FROM student"

            # tidy this line up
            query += "\nWHERE student_number = \"" + str(stud_num) + "\""
            # print(query)
            # use the results to extract the appropriate course_id
            # insert_enrollment_table(student_number, course_id, "Active")
            try:
                mycursor.execute(query);
                for results in mycursor.fetchall():
                    # print(results)
                    student_id = results[0]
            except Error as e:
                print(e)

            insert_enrollment_table(student_id, course_id, "Active")

def get_num_students():
    # Question: How many students are in the database?
    total = 0
    mydb = establish_connection()
    mycursor = mydb.cursor();
    query = "SELECT * FROM student"
    # print(query)
    try:
        mycursor.execute(query);
        for result in mycursor.fetchall():
            total+=1
    except Error as e:
        print(e)

    print("Question: How many students are in the database? Answer: There are currently", total, "students in the database")


def get_num_classes():
    # Question: How many students are in the database?
    total = 0
    mydb = establish_connection()
    mycursor = mydb.cursor();
    query = "SELECT * FROM schedule"
    # print(query)
    try:
        mycursor.execute(query);
        for result in mycursor.fetchall():
            total += 1
    except Error as e:
        print(e)

    print("Question: How many classes are being offered? Answer: There are currently", total,
          "classes being offered")

def get_seniors():
    # Question: How many students are in the database?
    total = 0
    mydb = establish_connection()
    mycursor = mydb.cursor();
    query = "SELECT * FROM student WHERE class_year = \"04\""
    # print(query)
    try:
        mycursor.execute(query);
        for result in mycursor.fetchall():
            total += 1
    except Error as e:
        print(e)

    print("Question: How many seniors are there? Answer: There are currently", total,
          "seniors")

def senior_class_status():
    # Question: What is the status of all classes that seniors have taken or want to take?
    mydb = establish_connection()
    mycursor = mydb.cursor();
    query = """SELECT s.fname, s.lname, e.status
                FROM student s JOIN enrollment e using (student_id) WHERE class_year = \"04\""""


    print("The status of all classes that seniors have taken or want to take:")
    # print(query)
    try:
        mycursor.execute(query);
        for result in mycursor.fetchall():
            print(result)
    except Error as e:
        print(e)

def student_schedule(studentID):
    # Question: What are all of the classes a student is taking? Provide the student's ID number
    mydb = establish_connection()
    mycursor = mydb.cursor();
    query = """
    SELECT s.student_id, s.fname, s.lname, e.course_id
    FROM enrollment as e JOIN student as s using (student_id)
    WHERE student_id =
    """
    query += "\"" + studentID + "\""
    # print(query)
    try:
        mycursor.execute(query);
        for result in mycursor.fetchall():
            print(result)
    except Error as e:
        print(e)



def class_enrollment(courseID):
    # Question: What does the attendance sheet look like for a specified course? Provide the course id number
    mydb = establish_connection()
    mycursor = mydb.cursor();
    query = """
    select c.class_name,s.student_id,s.fname,s.lname
    from schedule as c join student as s 
    where c.class_id =
       """
    query += "\"" + courseID + "\""
    print(query)
    try:
        mycursor.execute(query);
        for result in mycursor.fetchall():
            print(result)
    except Error as e:
        print(e)

def major_of_interest(major):
    #Question: What types of classes are students taking, provided that they are pursuing a specific major?
    mydb = establish_connection()
    mycursor = mydb.cursor();
    query = """
    SELECT s.fname,s.lname, e.course_id
    FROM  student as s join enrollment as e USING (student_id)
    WHERE s.major1 =
       """
    query += "\"" + major + "\""
    #print(query)
    try:
        mycursor.execute(query);
        for result in mycursor.fetchall():
            print(result)
    except Error as e:
        print(e)



main()


