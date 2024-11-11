import os
import re
import openpyxl
import sqlite3
import enmscripting
import datetime

#Функция добавлением данных в БД
def addind_to_db(in_data: list, in_date: str) -> bool:
    
    sql_query1 = '''CREATE TABLE IF NOT EXISTS request_date (
                                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                                 r_date DATE UNIQUE);'''
    
    sql_query2 = '''SELECT id
                      FROM request_date
                     WHERE r_date = "{}";'''
    
    sql_query3 = '''INSERT INTO request_date (r_date)
                    VALUES ("{}");'''

    sql_query4 = '''CREATE TABLE IF NOT EXISTS serial_number_data (
                                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                                 r_date INTEGER,
                                 network_element TEXT NOT NULL,
                                 equipment TEXT,
                                 serial_number TEXT,
                                 production_date TEXT,
                                 product_number TEXT,
                                 product_name TEXT,
                                 product_revision TEXT);'''
    
    sql_query5 = '''INSERT INTO serial_number_data (
                                r_date,
                                network_element,
                                equipment,
                                serial_number,
                                production_date,
                                product_number,
                                product_name,
                                product_revision)
                        VALUES ({},?,?,?,?,?,?,?);'''
    
    try:
        db_connection = sqlite3.connect('database\\1.db')
        db_cursor = db_connection.cursor()

        #создание/проверка на наличие БД
        db_cursor.execute(sql_query1)
        db_connection.commit()

        db_cursor.execute(sql_query4)
        db_connection.commit()
        
        #проверка даты на наличие в БД
        db_cursor.execute(sql_query2.format(in_date))
        db_connection.commit()

        answer = db_cursor.fetchone()
        
        if answer is None:
            
            db_cursor.execute(sql_query3.format(in_date))
            db_connection.commit()

            db_cursor.execute(sql_query2.format(in_date))
            db_connection.commit()
            
            date_id = db_cursor.fetchone()[0]

            sql_temp = sql_query5.format(date_id)
            db_cursor.executemany(sql_temp,in_data)
            db_connection.commit()

            result = True
            
        else:
            date_id = answer[0]

            result = False

        print(result)
                
        db_cursor.close()
        
    except sqlite3.Error as error:
        print("db error", error)
    
    finally:
        if (db_connection):
            db_connection.close()
            print("db connection closed")


    return result

#Функция парсинга строки
def line_parser(in_line: str) -> str:
    out_line = ""
    in_list = in_line.split("|")
    if re.match(r"M[OSCR]\d{4}", in_list[0]):
        if len(in_list) == 4:
            out_str = in_list[0] + "|" + in_list[2] + "|"
            t_str = in_list[3]
            if "null" in t_str:
                out_str = out_str + "ND|ND|ND|ND|ND"
            else:
                t_str = t_str.replace("{","").replace("}","").replace("\n","")
                t_list = t_str.split(", ")
                t_dict = {}
                for t_ls in t_list:
                    t_dict.update({t_ls.split("=")[0]:t_ls.split("=")[1]})
                out_str = out_str + t_dict["serialNumber"] + "|"
                out_str = out_str + t_dict["productionDate"] + "|"
                out_str = out_str + t_dict["productNumber"] + "|"
                out_str = out_str + t_dict["productName"] + "|"
                out_str = out_str + t_dict["productRevision"]
            out_line = out_str + "\n"
        elif len(in_list) == 5:
            out_str = in_list[0] + "|DU|"
            t_str = in_list[4]
            if "null" in t_str:
                out_str = out_str + "ND|ND|ND|ND|ND"
            else:
                t_str = t_str.replace("{","").replace("}","").replace("\n","")
                t_list = t_str.split(", ")
                t_dict = {}
                for t_ls in t_list:
                    t_dict.update({t_ls.split("=")[0]:t_ls.split("=")[1]})
                out_str = out_str + t_dict["serialNumber"] + "|"
                out_str = out_str + t_dict["productionDate"] + "|"
                out_str = out_str + t_dict["productNumber"] + "|"
                out_str = out_str + t_dict["productName"] + "|"
                out_str = out_str + t_dict["productRevision"]
            out_line = out_str + "\n"
        elif len(in_list)  == 9:
            if "RRU" in in_list[3]:
                out_str = in_list[0] + "|" +in_list[2]+ " " + in_list[3] + "|" + in_list[8].replace("\n","") + "|" + in_list[4] + "|" + in_list[6] + "|" + in_list[5] + "|" + in_list[7]
                out_line = out_str + "\n"
        else:
            pass
        
    return out_line

#Работа с ENM
def ENM_request(file_out_name: str):
    #ENM URL
    ENM7_url = "https://enm7.enm.tele2.ru/"
    ENM8_url = "https://enm8.enm.tele2.ru/"
    ENM14_url = "https://enm14.enm.tele2.ru/"

    #Если учетные данные совпадают между ENM заполните ENM_login/ENM_pass
    ENM_login = "usr"
    ENM_pass = "psw"

    #Если учетные данные  не совпадают между ENM заполните данные для каждого ENM
    ENM7_login = ""
    ENM7_pass = ""
    ENM8_login = ""
    ENM8_pass = ""
    ENM14_login = ""
    ENM14_pass = ""

    #Открытие сессий ENM
    if ENM_login == "" and ENM_pass == "":
        ENM7_Session = enmscripting.open(ENM7_url,ENM7_login,ENM7_pass)
        ENM8_Session = enmscripting.open(ENM8_url,ENM8_login,ENM8_pass)
        ENM14_Session = enmscripting.open(ENM14_url,ENM14_login,ENM14_pass)
    else:
        ENM7_Session = enmscripting.open(ENM7_url,ENM_login,ENM_pass)
        ENM8_Session = enmscripting.open(ENM8_url,ENM_login,ENM_pass)
        ENM14_Session = enmscripting.open(ENM14_url,ENM_login,ENM_pass)

    #CLI Command
    ENM7_command_list =[
        "cmedit get --collection All_ENM7_Collection AuxPlugInUnit.(productionDate, serialNumber, productNumber, productRevision, productName) -t",
        "cmedit get --collection All_ENM7_Collection AuxPlugInUnit.(productData) -t",
        "cmedit get --collection All_ENM7_Collection FieldReplaceableUnit.(productData) -t",
        "cmedit get --collection All_ENM7_Collection Slot=1 productData --netype=ERBS -t",
        "cmedit get --collection All_ENM7_Collection Slot=1 productData --netype=RBS -t"
        ]
    ENM8_command_list =[
        "cmedit get --collection All_ENM8_Collection AuxPlugInUnit.(productionDate, serialNumber, productNumber, productRevision, productName) -t",
        "cmedit get --collection All_ENM8_Collection AuxPlugInUnit.(productData) -t",
        "cmedit get --collection All_ENM8_Collection FieldReplaceableUnit.(productData) -t",
        "cmedit get --collection All_ENM8_Collection Slot=1 productData --netype=ERBS -t",
        "cmedit get --collection All_ENM8_Collection Slot=1 productData --netype=RBS -t"
        ]
    ENM14_command_list =[
        "cmedit get --collection All_ENM14_Collection AuxPlugInUnit.(productionDate, serialNumber, productNumber, productRevision, productName) -t",
        "cmedit get --collection All_ENM14_Collection AuxPlugInUnit.(productData) -t",
        "cmedit get --collection All_ENM14_Collection FieldReplaceableUnit.(productData) -t",
        "cmedit get --collection All_ENM14_Collection Slot=1 productData --netype=ERBS -t",
        "cmedit get --collection All_ENM14_Collection Slot=1 productData --netype=RBS -t"
        ]

    ENM_all_command_list =[
        "cmedit get * AuxPlugInUnit.(productionDate, serialNumber, productNumber, productRevision, productName) -t",
        "cmedit get * AuxPlugInUnit.(productData) -t",
        "cmedit get * FieldReplaceableUnit.(productData) -t",
        "cmedit get * Slot=1 productData --netype=ERBS -t",
        "cmedit get * Slot=1 productData --netype=RBS -t"
        ]

    ENM7_command_list = ENM_all_command_list
    ENM8_command_list = ENM_all_command_list
    ENM14_command_list = ENM_all_command_list
    
    with open("sn_report_csv\\" + file_out_name + ".txt", 'w') as file_out:

    #Chek ENM7
        cmd = ENM7_Session.command()
        for command in ENM7_command_list:
            response = cmd.execute(command)
            if len(response.get_output().groups()) > 0:
                table = response.get_output().groups()[0]
                for line in table:
                    s = ""
                    for cell in line:
                        s = s + cell.value() + "|"
                    s = s[0:-1]
                    file_out.write(line_parser(s))

    #Chek ENM8
        cmd = ENM8_Session.command()
        for command in ENM8_command_list:
            response = cmd.execute(command)
            if len(response.get_output().groups()) > 0:
                table = response.get_output().groups()[0]
                for line in table:
                    s = ""
                    for cell in line:
                        s = s + cell.value() + "|"
                    s = s[0:-1]
                    s_out = ""
                    s_out = line_parser(s)
                    file_out.write(s_out)

    #Chek ENM14
        cmd = ENM14_Session.command()
        for command in ENM14_command_list:
            response = cmd.execute(command)
            if len(response.get_output().groups()) > 0:
                table = response.get_output().groups()[0]
                for line in table:
                    s = ""
                    for cell in line:
                        s = s + cell.value() + "|"
                    s = s[0:-1]
                    s_out = ""
                    s_out = line_parser(s)
                    file_out.write(s_out)


    #ENM Session closed
    enmscripting.close(ENM7_Session)
    enmscripting.close(ENM8_Session)
    enmscripting.close(ENM14_Session)


def csv_to_exel(file_out_name: str):
    #EXCEL
    with open("sn_report_csv\\" + file_out_name + ".txt","r") as in_file:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = file_out_name
        i = 0
        for in_line in in_file:
            in_list = in_line.split("|")
            j = 1
            i = i + 1
            for t_str in in_list:
                ws.cell(i,j).value = t_str
                j = j + 1
            
        wb.save("sn_report_exel\\" + file_out_name + ".xlsx")
        
        report_path = "L:\\technical\\Эксплуатация_БС_UMTS\\Регламент_эксплуатация\\Распределение по группам\\Зона 3\\reports\\" + current_date.strftime("%Y") + "\\" + current_date.strftime("%m") + "\\"
        if not os.path.isdir(report_path):
            os.makedirs(report_path)
            print(report_path)
            wb.save(report_path + file_out_name + ".xlsx")
        else:
            print(report_path)
            wb.save(report_path + file_out_name + ".xlsx")
            
            
        report_path = "L:\\technical\\RNC_ALL\\sn_loader\\" + current_date.strftime("%Y") + "\\" + current_date.strftime("%m") + "\\"
        if not os.path.isdir(report_path):
            os.makedirs(report_path)
            print(report_path)
            wb.save(report_path + file_out_name + ".xlsx")
        else:
            print(report_path)
            wb.save(report_path + file_out_name + ".xlsx")
			
			
        wb.close()
    
#Current date
current_date = datetime.datetime.now()
file_out_name = current_date.strftime("%Y%m%d%H%M%S")

file_out_name = file_out_name + "_sn_report"

ENM_request(file_out_name)

csv_to_exel(file_out_name)

#работа с БД

with open("sn_report_csv\\" + file_out_name + ".txt","r") as in_file:
    in_data = list()
    for in_line in in_file:
        in_list = in_line.split("|")
        in_data.append(in_list)
    
    in_date = current_date.strftime("%Y-%m-%d")
    #in_date = "2022-07-28"
    if addind_to_db(in_data, in_date):
        print("SN added")
