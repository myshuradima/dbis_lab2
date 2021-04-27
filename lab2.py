# -*- coding: utf8 -*-

import psycopg2
import os
from queries_lab2 import select_out_id, count_rows, select_from_odata, insert_subjects, select_query
from config import dbname, password, port, username, host

if not os.listdir(path="dir"):
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=username,
            password=password
        )
        cursor = conn.cursor()
        cursor.execute(count_rows)
        N = cursor.fetchone()
        N = int(N[0])
        i = 0
        n = 0
        cursor.execute(select_out_id)

        while i < N:
            k = 0
            f = open('dir/out_id_file_' + str(n) + '.csv', 'w')
            if N - i > 2000:
                k = 0
            else:
                k = N - i
            while k < 2000:
                line = cursor.fetchone()
                f.write(line[0]+"\n")
                k = k + 1
            n = n + 1
            f.close()
            i = i + k
    except Exception as error:
        print("Error")
    finally:
        cursor.close()
        conn.close()
def download_address(address):
    try:
        conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=username,
        password=password
        )
        cursor = conn.cursor()
        cursor.execute("""SELECT MAX(subject_id) FROM subject;""")
        a = cursor.fetchone()[0]
        print(a)
        if a is None:
            cursor.execute(insert_subjects)
            cursor.close()
            conn.commit()
        with open(address, encoding="cp1251") as file:
            a = file.readline()
            i = 0
            while a:
                select_query = select_from_odata + "'" + a[0:-1] + "';"
                print(select_query)
                cursor = conn.cursor()
                cursor.execute(select_query)
                new_list = cursor.fetchone()
                i = i + 1
                print(i)

                # sex_type_name
                #print(1)
                if new_list[2]:
                    cursor.execute("SELECT sex_type_id FROM sex_type WHERE sex_type_name = '" + str(new_list[2]) + "';")
                    sex_type_id = cursor.fetchone()
                    if not sex_type_id:
                        cursor.execute("SELECT MAX(sex_type_id) FROM sex_type;")
                        sex_type_id = cursor.fetchone()
                        sex_type_id = sex_type_id[0]
                        if not sex_type_id:
                            cursor.execute("INSERT INTO sex_type(sex_type_id, sex_type_name) VALUES (%s, %s)", [1, new_list[2]])
                            sex_type_id = 1
                        else:
                            cursor.execute("INSERT INTO sex_type(sex_type_id, sex_type_name) VALUES (%s, %s)", [sex_type_id+1, new_list[2]])
                            sex_type_id = sex_type_id + 1
                    else:
                        sex_type_id = sex_type_id[0]
                else:
                    cursor.execute(
                        "SELECT sex_type_id FROM sex_type WHERE sex_type_name = 'None';")
                    sex_type_id = cursor.fetchone()
                    if not sex_type_id:
                        cursor.execute(
                            "INSERT INTO sex_type(sex_type_id, sex_type_name) VALUES (0, 'None')", )
                        sex_type_id = 0

                #region name
                #print(2)
                if new_list[3]:
                    cursor.execute("SELECT region_id FROM region WHERE region_name = '" + str(new_list[3]) + "';")
                    region_id = cursor.fetchone()
                    if not region_id:
                        cursor.execute("SELECT MAX(region_id) FROM region;")
                        region_id = cursor.fetchone()
                        region_id = region_id[0]
                        if not region_id:
                            cursor.execute("INSERT INTO region(region_id, region_name) VALUES (%s, %s)", [1, new_list[3]])
                            region_id = 1
                        else:
                            cursor.execute("INSERT INTO region(region_id, region_name) VALUES (%s, %s)",
                                           [region_id + 1, new_list[3]])
                            region_id = region_id + 1
                    else:
                        region_id = region_id[0]
                else:
                    cursor.execute(
                        "SELECT region_id FROM region WHERE region_name = 'None';")
                    region_id = cursor.fetchone()
                    if not region_id:
                        cursor.execute(
                            "INSERT INTO region(region_id, region_name) VALUES (0, 'None')", )
                        region_id = 0

                #area name
                #print(3)
                if new_list[4]:
                    a = str(new_list[4])
                    area_name = a.replace("'","`")
                    cursor.execute("SELECT area_id FROM area WHERE area_name = '" + str(area_name) + "';")

                    area_id = cursor.fetchone()
                    if not area_id:
                        cursor.execute("SELECT MAX(area_id) FROM area;")
                        area_id = cursor.fetchone()
                        area_id = area_id[0]
                        if not area_id:
                            cursor.execute("INSERT INTO area(area_id, area_name, region_id) VALUES (%s, %s, %s)", [1, area_name, region_id])
                            area_id = 1
                        else:
                            cursor.execute("INSERT INTO area(area_id, area_name, region_id) VALUES (%s, %s, %s)",
                                           [area_id + 1, area_name, region_id])
                            area_id = area_id + 1
                    else:
                        area_id = area_id[0]
                else:
                    cursor.execute(
                        "SELECT area_id FROM area WHERE area_name = 'None';")
                    area_id = cursor.fetchone()
                    if not area_id:
                        cursor.execute(
                            "INSERT INTO area(area_id, area_name, region_id) VALUES (0, 'None', 0)", )
                        area_id = 0

                # teretory type
                #print(4)
                if new_list[7]:
                    a = str(new_list[7])
                    teretory_type = a.replace("'", "`")
                    cursor.execute("SELECT teretory_type_id FROM teretory_type WHERE teretory_type_name = '" + str(teretory_type) + "';")

                    teretory_type_id = cursor.fetchone()
                    if not teretory_type_id:
                        cursor.execute("SELECT MAX(teretory_type_id) FROM teretory_type;")
                        teretory_type_id = cursor.fetchone()
                        teretory_type_id = teretory_type_id[0]

                        if not teretory_type_id:
                            cursor.execute("INSERT INTO teretory_type(teretory_type_id, teretory_type_name) VALUES (%s, %s)",
                                           [1, teretory_type])
                            teretory_type_id = 1
                        else:
                            cursor.execute("INSERT INTO teretory_type(teretory_type_id, teretory_type_name) VALUES (%s, %s)",
                                           [teretory_type_id + 1, teretory_type])
                            teretory_type_id = teretory_type_id + 1
                    else:
                        teretory_type_id = teretory_type_id[0]
                else:
                    cursor.execute(
                        "SELECT teretory_type_id FROM teretory_type WHERE teretory_type_name = 'None';")
                    teretory_type_id = cursor.fetchone()
                    if not teretory_type_id:
                        cursor.execute(
                            "INSERT INTO teretory_type(teretory_type_id, teretory_type_name) VALUES (0, 'None')")
                        teretory_type_id = 0

                # teretory
                #print(5)
                if new_list[5]:
                    a = str(new_list[5])
                    teretory = a.replace("'", "`")
                    cursor.execute("SELECT teretory_id FROM teretory WHERE teretory_name = '" + str(teretory) + "';")

                    teretory_id = cursor.fetchone()
                    if not teretory_id:
                        cursor.execute("SELECT MAX(teretory_id) FROM teretory;")
                        teretory_id = cursor.fetchone()
                        teretory_id = teretory_id[0]
                        if not teretory_id:
                            cursor.execute("INSERT INTO teretory(teretory_id, teretory_name, area_id, teretory_type_id) VALUES (%s, %s, %s, %s)",
                                [1, teretory, area_id, teretory_type_id])
                            teretory_id = 1
                        else:
                            cursor.execute("INSERT INTO teretory(teretory_id, teretory_name, area_id, teretory_type_id) VALUES (%s, %s, %s, %s)",
                                [teretory_id + 1, teretory, area_id, teretory_type_id])
                            teretory_id = teretory_id + 1
                    else:
                        teretory_id = teretory_id[0]
                else:
                    cursor.execute(
                        "SELECT teretory_id FROM teretory WHERE teretory_name = 'None';")
                    education_place_type_id = cursor.fetchone()
                    if not education_place_type_id:
                        cursor.execute(
                            "INSERT INTO teretory(teretory_id, teretory_name, area_id, teretory_type_id) VALUES (0, 'None', %s, %s)", [area_id, teretory_type_id])
                        education_place_type_id = 0

                # regtype name
                #print(6)
                if new_list[6]:
                    a = str(new_list[6])
                    reg_type = a.replace("'", "`")
                    cursor.execute("SELECT reg_type_id FROM reg_type WHERE reg_type_name = '" + str(reg_type) + "';")

                    reg_type_id = cursor.fetchone()
                    if not reg_type_id:
                        cursor.execute("SELECT MAX(reg_type_id) FROM reg_type;")
                        reg_type_id = cursor.fetchone()
                        reg_type_id = reg_type_id[0]
                        if not reg_type_id:
                            cursor.execute(
                                "INSERT INTO reg_type(reg_type_id, reg_type_name) VALUES (%s, %s)",
                                [1, reg_type])
                            reg_type_id = 1
                        else:
                            cursor.execute(
                                "INSERT INTO reg_type(reg_type_id, reg_type_name) VALUES (%s, %s)",
                                [reg_type_id + 1, reg_type])
                            reg_type_id = reg_type_id + 1
                    else:
                        reg_type_id = reg_type_id[0]
                else:
                    cursor.execute(
                        "SELECT reg_type_id FROM reg_type WHERE reg_type_name = 'None';")
                    education_place_type_id = cursor.fetchone()
                    if not education_place_type_id:
                        cursor.execute(
                            "INSERT INTO reg_type(reg_type_id, reg_type_name) VALUES (0, 'None')", )
                        education_place_type_id = 0

                # education place parent
                #print(7)
                education_place_parent = new_list[15]
                if education_place_parent:
                    a = str(new_list[15])
                    education_place_parent = a.replace("'", "`")

                    cursor.execute("SELECT education_place_parent_id FROM education_place_parent WHERE education_place_parent_name = '" + str(education_place_parent) + "';")

                    education_place_parent_id = cursor.fetchone()
                    if not education_place_parent_id:
                        cursor.execute("SELECT MAX(education_place_parent_id) FROM education_place_parent;")
                        education_place_parent_id = cursor.fetchone()
                        education_place_parent_id = education_place_parent_id[0]
                        if not education_place_parent_id:
                            cursor.execute(
                                "INSERT INTO education_place_parent(education_place_parent_id, education_place_parent_name) VALUES (%s, %s)",
                            [1, education_place_parent])
                            education_place_parent_id = 1
                        else:
                            cursor.execute(
                                "INSERT INTO education_place_parent(education_place_parent_id, education_place_parent_name) VALUES (%s, %s)",
                            [education_place_parent_id + 1, education_place_parent])
                            education_place_parent_id = education_place_parent_id + 1
                    else:
                        education_place_parent_id = education_place_parent_id[0]
                else:
                    cursor.execute(
                        "SELECT education_place_parent_id FROM education_place_parent WHERE education_place_parent_name = 'None';")
                    education_place_parent_id = cursor.fetchone()
                    if not education_place_parent_id:
                        cursor.execute(
                            "INSERT INTO education_place_parent(education_place_parent_id, education_place_parent_name) VALUES (0, 'None')",)
                        education_place_parent_id = 0

                # education place types
                #print(8)
                education_place_type = new_list[11]
                if education_place_type:
                    a = str(new_list[11])
                    education_place_type = a.replace("'", "`")

                    cursor.execute(
                            "SELECT education_place_type_id FROM education_place_types WHERE education_place_type_name = '" + str(
                            education_place_type) + "';")

                    education_place_type_id = cursor.fetchone()
                    if not education_place_type_id:
                        cursor.execute("SELECT MAX(education_place_type_id) FROM education_place_types;")
                        education_place_type_id = cursor.fetchone()
                        education_place_type_id = education_place_type_id[0]
                        if not education_place_type_id:
                            cursor.execute(
                                "INSERT INTO education_place_types(education_place_type_id, education_place_type_name) VALUES (%s, %s)",
                                [1, education_place_type])
                            education_place_type_id = 1
                        else:
                            cursor.execute(
                                "INSERT INTO education_place_types(education_place_type_id, education_place_type_name) VALUES (%s, %s)",
                                [education_place_type_id + 1, education_place_type])
                            education_place_type_id = education_place_type_id + 1
                    else:
                        education_place_type_id = education_place_type_id[0]
                else:
                    cursor.execute(
                        "SELECT education_place_type_id FROM education_place_types WHERE education_place_type_name = 'None';")
                    education_place_type_id = cursor.fetchone()
                    if not education_place_type_id:
                        cursor.execute(
                            "INSERT INTO education_place_types(education_place_type_id, education_place_type_name) VALUES (0, 'None')")
                        education_place_type_id = 0


                # class profile
                #print(9)
                class_profile = new_list[8]
                if class_profile:
                    a = str(new_list[8])
                    class_profile = a.replace("'", "`")

                    cursor.execute(
                            "SELECT class_profile_id FROM class_profile WHERE class_profile_name = '" + str(
                            class_profile) + "';")

                    class_profile_id = cursor.fetchone()
                    if not class_profile_id:
                        cursor.execute("SELECT MAX(class_profile_id) FROM class_profile;")
                        class_profile_id = cursor.fetchone()
                        class_profile_id = class_profile_id[0]
                        if not class_profile_id:
                            cursor.execute(
                                "INSERT INTO class_profile(class_profile_id, class_profile_name) VALUES (%s, %s)",
                                [1, class_profile])
                            class_profile_id = 1
                        else:
                            cursor.execute(
                                "INSERT INTO class_profile(class_profile_id, class_profile_name) VALUES (%s, %s)",
                                [class_profile_id + 1, class_profile])
                            class_profile_id = class_profile_id + 1
                    else:
                        class_profile_id = class_profile_id[0]
                else:
                    cursor.execute(
                        "SELECT class_profile_id FROM class_profile WHERE class_profile_name = 'None';")
                    class_profile_id = cursor.fetchone()
                    if not class_profile_id:
                        cursor.execute(
                            "INSERT INTO class_profile(class_profile_id, class_profile_name) VALUES (0, 'None')")
                        class_profile_id = 0


                # class language
                #print(10)
                class_language = new_list[9]
                if class_language:
                    a = str(new_list[9])
                    class_language = a.replace("'", "`")

                    cursor.execute(
                            "SELECT language_id FROM all_languages WHERE language_name = '" + str(
                            class_language) + "';")

                    class_language_id = cursor.fetchone()
                    if not class_language_id:
                        cursor.execute("SELECT MAX(language_id) FROM all_languages;")
                        class_language_id = cursor.fetchone()
                        class_language_id = class_language_id[0]
                        if not class_language_id:
                            cursor.execute(
                                "INSERT INTO all_languages(language_id, language_name) VALUES (%s, %s)",
                                [1, class_language])
                            class_language_id = 1
                        else:
                            cursor.execute(
                                "INSERT INTO all_languages(language_id, language_name) VALUES (%s, %s)",
                                [class_language_id + 1, class_language])
                            class_language_id = class_language_id + 1
                    else:
                        class_language_id = class_language_id[0]
                else:
                    cursor.execute(
                        "SELECT language_id FROM all_languages WHERE language_name = 'None';")
                    class_language_id = cursor.fetchone()
                    if not class_language_id:
                        cursor.execute(
                            "INSERT INTO all_languages(language_id, language_name) VALUES (0, 'None')")
                        class_language_id = 0


                # education place
                #print(11)
                education_place = new_list[10]
                if education_place:
                    a = str(new_list[10])
                    education_place = a.replace("'", "`")

                    cursor.execute(
                            "SELECT education_place_id FROM education_place WHERE education_place_name = '" + str(
                            education_place) + "';")

                    education_place_id = cursor.fetchone()
                    if not education_place_id:
                        cursor.execute("SELECT MAX(education_place_id) FROM education_place;")
                        education_place_id = cursor.fetchone()
                        education_place_id = education_place_id[0]
                        if not education_place_id:
                            cursor.execute(
                                "INSERT INTO education_place(education_place_id, education_place_name, teretory_id, education_place_type_id, education_place_parent_id) VALUES (%s, %s, %s, %s, %s)",
                                [1, education_place_type, teretory_id, education_place_type_id, education_place_parent_id])
                            education_place_id = 1
                        else:
                            cursor.execute(
                                "INSERT INTO education_place(education_place_id, education_place_name, teretory_id, education_place_type_id, education_place_parent_id) VALUES (%s, %s, %s, %s, %s)",
                                [education_place_id + 1, education_place_type, teretory_id, education_place_type_id, education_place_parent_id])

                            education_place_id = education_place_id + 1
                    else:
                        education_place_type_id = education_place_type_id[0]
                else:
                    cursor.execute(
                        "SELECT education_place_id FROM education_place WHERE education_place_name = 'None';")
                    education_place_id = cursor.fetchone()
                    if not education_place_id:
                        cursor.execute(
                            "INSERT INTO education_place(education_place_id, education_place_name, teretory_id, education_place_type_id, education_place_parent_id) VALUES (0, 'None', %s, %s, %s)", [teretory_id, education_place_type_id, education_place_parent_id])
                        education_place_id = 0


                #education place region name
                #print(12)
                if new_list[12]:
                    cursor.execute("SELECT region_id FROM region WHERE region_name = '" + str(new_list[12]) + "';")
                    education_place_region_id = cursor.fetchone()
                    if not education_place_region_id:
                        cursor.execute("SELECT MAX(region_id) FROM region;")
                        education_place_region_id = cursor.fetchone()
                        education_place_region_id = education_place_region_id[0]
                        if not education_place_region_id:
                            cursor.execute("INSERT INTO region(region_id, region_name) VALUES (%s, %s)", [1, new_list[12]])
                            education_place_region_id = 1
                        else:
                            cursor.execute("INSERT INTO region(region_id, region_name) VALUES (%s, %s)",
                                           [education_place_region_id + 1, new_list[12]])
                            education_place_region_id = education_place_region_id + 1
                    else:
                        education_place_region_id = education_place_region_id[0]
                else:
                    cursor.execute(
                        "SELECT region_id FROM region WHERE region_name = 'None';")
                    region_id = cursor.fetchone()
                    if not education_place_region_id:
                        cursor.execute(
                            "INSERT INTO region(region_id, region_name) VALUES (0, 'None')", )
                        education_place_region_id = 0

                # education place area name
                #print(13)
                if new_list[13]:
                    a = str(new_list[13])
                    area_name = a.replace("'","`")
                    cursor.execute("SELECT area_id FROM area WHERE area_name = '" + str(area_name) + "';")

                    education_place_area_id = cursor.fetchone()
                    if not education_place_area_id:
                        cursor.execute("SELECT MAX(area_id) FROM area;")
                        education_place_area_id = cursor.fetchone()
                        education_place_area_id = education_place_area_id[0]
                        if not education_place_area_id:
                            cursor.execute("INSERT INTO area(area_id, area_name, region_id) VALUES (%s, %s, %s)", [1, area_name, education_place_region_id])
                            education_place_area_id = 1
                        else:
                            cursor.execute("INSERT INTO area(area_id, area_name, region_id) VALUES (%s, %s, %s)",
                                           [education_place_area_id + 1, area_name, education_place_region_id])
                            education_place_area_id = education_place_area_id + 1
                    else:
                        education_place_area_id = education_place_area_id[0]
                else:
                    cursor.execute(
                        "SELECT area_id FROM area WHERE area_name = 'None';")
                    education_place_area_id = cursor.fetchone()
                    if not education_place_area_id:
                        cursor.execute(
                            "INSERT INTO area(area_id, area_name, region_id) VALUES (0, 'None', %s)",[education_place_region_id] )
                        education_place_area_id = 0


                # education place teretory
                #print(14)
                if new_list[14]:
                    a = str(new_list[14])
                    teretory = a.replace("'", "`")
                    cursor.execute("SELECT teretory_id FROM teretory WHERE teretory_name = '" + str(teretory) + "';")

                    education_place_teretory_id = cursor.fetchone()[0]
                    if not education_place_teretory_id:
                        cursor.execute("SELECT MAX(teretory_id) FROM teretory;")
                        education_place_teretory_id = cursor.fetchone()
                        education_place_teretory_id = education_place_teretory_id[0]
                        if not education_place_teretory_id:
                            cursor.execute("INSERT INTO teretory(teretory_id, teretory_name, area_id, teretory_type_id) VALUES (%s, %s, %s, %s)",
                                [1, teretory, education_place_area_id, teretory_type_id])
                            education_place_teretory_id = 1
                        else:
                            cursor.execute("INSERT INTO teretory(teretory_id, teretory_name, area_id, teretory_type_id) VALUES (%s, %s, %s, %s)",
                                [education_place_teretory_id + 1, teretory, education_place_area_id, teretory_type_id])
                            education_place_teretory_id = education_place_teretory_id + 1
                    else:
                        education_place_teretory_id = education_place_teretory_id
                else:
                    cursor.execute(
                        "SELECT teretory_id FROM teretory WHERE teretory_name = 'None';")
                    education_place_teretory_id = cursor.fetchone()
                    if not education_place_teretory_id:
                        cursor.execute(
                            "INSERT INTO teretory(teretory_id, teretory_name, area_id, teretory_type_id) VALUES (0, 'None', %s, %s)", [area_id, teretory_type_id])
                        education_place_teretory_id = 0

                #student
                cursor.execute("SELECT out_id FROM student WHERE out_id = '" + new_list[0] + "';")
                o = cursor.fetchone()
                if not o:
                    cursor.execute(
                    """INSERT INTO student(out_id, birth, exam_year, education_place_id, language_id, class_profile_id, 
                    teretory_id, sex_type_id, reg_type_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    [new_list[0], new_list[1], new_list[-1], education_place_id, class_language_id, class_profile_id,
                     teretory_id, sex_type_id,reg_type_id])


                #physics
                if new_list[46] is not None:
                    subject_id = 4
                    if new_list[47]:
                        cursor.execute("SELECT language_id FROM all_languages WHERE language_name = '"+new_list[47]+"';")
                        language_id = cursor.fetchone()[0]
                        if not language_id:
                            cursor.execute("SELECT MAX(language_id) FROM all_languages;")
                            language_id = cursor.fetchone()[0]
                            if not language_id:
                                language_id = 1
                                cursor.execute("INSERT INTO all_languages(language_id, language_name) VALUES(1, %s);", [new_list[47]])
                            else:
                                cursor.execute("INSERT INTO all_languages(language_id, language_name) VALUES(%s, %s);", [language_id+1, new_list[47]])
                    else:
                        cursor.execute("SELECT language_id FROM all_languages WHERE language_name = 'None';")
                        language_id = cursor.fetchone()[0]
                        if language_id:
                            language_id = 0
                        else:
                            cursor.execute(
                                "INSERT INTO all_languages(language_id, language_name) VALUES(0, 'None');")

                    # region name
                    # print(2)

                    if new_list[53]:
                        cursor.execute(
                            "SELECT region_id FROM region WHERE region_name = '" + str(new_list[53]) + "';")
                        region_id = cursor.fetchone()
                        if not region_id:
                            cursor.execute("SELECT MAX(region_id) FROM region;")
                            region_id = cursor.fetchone()
                            region_id = region_id[0]
                            if not region_id:
                                cursor.execute("INSERT INTO region(region_id, region_name) VALUES (%s, %s)",
                                   [1, new_list[53]])
                                region_id = 1
                            else:
                                cursor.execute("INSERT INTO region(region_id, region_name) VALUES (%s, %s)",
                                               [region_id + 1, new_list[53]])
                                region_id = region_id + 1
                        else:
                            region_id = region_id
                    else:
                        cursor.execute(
                                    "SELECT region_id FROM region WHERE region_name = 'None';")
                        region_id = cursor.fetchone()[0]
                        if not region_id:
                            cursor.execute(
                                    "INSERT INTO region(region_id, region_name) VALUES (0, 'None')", )
                            region_id = 0

                    #area name
                    #print(3)
                    if new_list[54]:
                        a = str(new_list[54])
                        area_name = a.replace("'","`")
                        cursor.execute("SELECT area_id FROM area WHERE area_name = '" + str(area_name) + "';")

                        area_id = cursor.fetchone()
                        if not area_id:
                            cursor.execute("SELECT MAX(area_id) FROM area;")
                            area_id = cursor.fetchone()
                            area_id = area_id[0]
                            if not area_id:
                                cursor.execute("INSERT INTO area(area_id, area_name, region_id) VALUES (%s, %s, %s)", [1, area_name, region_id])
                                area_id = 1
                            else:
                                cursor.execute("INSERT INTO area(area_id, area_name, region_id) VALUES (%s, %s, %s)",
                                           [area_id + 1, area_name, region_id])
                                area_id = area_id + 1
                        else:
                            area_id = area_id
                    else:
                        cursor.execute(
                        "SELECT area_id FROM area WHERE area_name = 'None';")
                        area_id = cursor.fetchone()[0]
                        if not area_id:
                            cursor.execute(
                                "INSERT INTO area(area_id, area_name, region_id) VALUES (0, 'None', %s)", [region_id])
                            area_id = 0

                    # teretory type
                    #print(4)

                    if new_list[55]:
                        a = str(new_list[55])
                        teretory = a.replace("'", "`")
                        cursor.execute("SELECT teretory_id FROM teretory WHERE teretory_name = '" + str(teretory) + "';")

                        teretory_id = cursor.fetchone()
                        if not teretory_type_id:
                            cursor.execute("SELECT MAX(teretory_id) FROM teretory;")
                            teretory_id = cursor.fetchone()
                            teretory_id = teretory_id[0]

                            if not teretory_id:
                                cursor.execute("INSERT INTO teretory(teretory_id, teretory_name, area_id, teretory_type_id) VALUES (%s, %s, %s, %s)",
                                           [1, teretory, area_id, teretory_type_id])
                                teretory_type_id = 1
                            else:
                                cursor.execute(
                                    "INSERT INTO teretory(teretory_id, teretory_name, area_id, teretory_type_id) VALUES (%s, %s, %s, %s)",
                                    [teretory_id + 1, teretory, area_id, teretory_type_id])
                                teretory_id = teretory_id + 1
                        else:
                            teretory_id = teretory_id
                    else:
                        cursor.execute(
                            "SELECT teretory_id FROM teretory WHERE teretory_name = 'None';")
                        teretory_id = cursor.fetchone()
                        if not teretory_id:
                            cursor.execute(
                                "INSERT INTO teretory(teretory_id, teretory_name, area_id, teretory_type_id) VALUES (0, 'None', %s, %s)",
                            [area_id, teretory_type_id])
                            teretory_id = 0

                    # education place
                    # print(11)

                    education_place = new_list[52]
                    if education_place:
                        a = str(new_list[52])
                        education_place = a.replace("'", "`")

                        cursor.execute(
                            "SELECT education_place_id FROM education_place WHERE education_place_name = '" + str(
                                education_place) + "';")

                        education_place_id = cursor.fetchone()
                        if education_place_id:
                            education_place_id = education_place_id[0]
                        if not education_place_id:
                            cursor.execute("SELECT MAX(education_place_id) FROM education_place;")
                            education_place_id = cursor.fetchone()
                            education_place_id = education_place_id[0]
                            if not education_place_id:
                                cursor.execute(
                                    "INSERT INTO education_place(education_place_id, education_place_name, teretory_id, education_place_type_id, education_place_parent_id) VALUES (%s, %s, %s, %s, %s)",
                                    [1, education_place_type, teretory_id, education_place_type_id,
                                        education_place_parent_id])
                                education_place_id = 1
                            else:
                                cursor.execute(
                                    "INSERT INTO education_place(education_place_id, education_place_name, teretory_id, education_place_type_id, education_place_parent_id) VALUES (%s, %s, %s, %s, %s)",
                                    [education_place_id + 1, education_place_type, teretory_id,
                                        education_place_type_id, education_place_parent_id])

                                education_place_id = education_place_id + 1
                        else:
                            education_place_id = education_place_id
                    else:
                        cursor.execute(
                            "SELECT education_place_id FROM education_place WHERE education_place_name = 'None';")
                        education_place_id = cursor.fetchone()
                        if not education_place_id:
                            cursor.execute(
                                "INSERT INTO education_place(education_place_id, education_place_name, teretory_id, education_place_type_id, education_place_parent_id) VALUES (0, 'None', %s, %s, %s)",
                                [teretory_id, education_place_type_id, education_place_parent_id])
                            education_place_id = 0

                    cursor.execute("SELECT MAX(test_id) FROM test;")
                    test_id = cursor.fetchone()[0]

                    if test_id:
                        test_id = test_id + 1
                        cursor.execute("""INSERT INTO test(test_id, subject_id, language_id, ball, ball100, ball12, status, dpa_level, adapt_scale, education_place_id)
                                                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                                       [test_id, subject_id, language_id, new_list[51], new_list[49], new_list[50],
                                        new_list[48], 'None', 0, education_place_id])
                    else:
                        test_id = 1
                        cursor.execute("""INSERT INTO test(test_id, subject_id, language_id, ball, ball100, ball12, status, dpa_level, adapt_scale, education_place_id)
                                                                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                                       [test_id, subject_id, language_id, new_list[51], new_list[49], new_list[50],
                                        new_list[48], 'None', 0, education_place_id])

                    cursor.execute("INSERT INTO students_tests(out_id, test_id) VALUES (%s, %s)", [new_list[0], test_id])






                a = file.readline()
                if len(a)<10:
                    a = False

        cursor.close()
        conn.commit()
        conn.close()
    except Exception as error:
        print(error)

for el in os.listdir(path="dir"):
    print("Uploading:", el)
    download_address("dir/" + el)
    os.remove("dir/" + el)
try:
    conn = psycopg2.connect(
    host=host,
    port=port,
    dbname=dbname,
    user=username,
    password=password
    )
    cursor = conn.cursor()
    cursor.execute(select_query)
    records = cursor.fetchall()
    print(type(records))
    cursor.close()
    conn.commit()
    conn.close()
    with open("result.csv", 'w') as file:
        file.write("Область, Середній бал ЗНО, Рік\n")
        for el in records:
            print(el)
            new_string = ""
            new_string = new_string + '"' + el[0] + '",' + str(round(el[1], 2)) + ',' + str(el[2]) + '\n'
            file.write(new_string)
except Exception as error:
    print(error)

