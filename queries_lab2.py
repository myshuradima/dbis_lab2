# -*- coding: utf8 -*-
select_out_id = """SELECT OUTID FROM odata;"""
count_rows = """SELECT COUNT(*) FROM odata;"""
select_from_odata = """SELECT * FROM odata WHERE OUTID = """
insert_subjects = """INSERT INTO subject(subject_id, subject_name) VALUES(1,'Українська мова і література');
                     INSERT INTO subject(subject_id, subject_name) VALUES(2,'Історія');
                     INSERT INTO subject(subject_id, subject_name) VALUES(3,'Математика');
                     INSERT INTO subject(subject_id, subject_name) VALUES(4,'Фізика');
                     INSERT INTO subject(subject_id, subject_name) VALUES(5,'Хімія');
                     INSERT INTO subject(subject_id, subject_name) VALUES(6,'Біологія');
                     INSERT INTO subject(subject_id, subject_name) VALUES(7,'Географія');
                     INSERT INTO subject(subject_id, subject_name) VALUES(8,'Англійська мова');
                     INSERT INTO subject(subject_id, subject_name) VALUES(9,'Французька мова');
                     INSERT INTO subject(subject_id, subject_name) VALUES(10,'Німецька мова');
                     INSERT INTO subject(subject_id, subject_name) VALUES(11,'Іспанська мова');
                    """
select_query = """select region_name, avg(test.ball100), exam_year from region join area on region.region_id = area.region_id 
                    join teretory on area.area_id = teretory.area_id
                    join student on teretory.teretory_id = student.teretory_id
                    join students_tests on student.out_id = students_tests.out_id
                    join test on students_tests.test_id = test.test_id
                    join subject on test.subject_id = subject.subject_id
                    group by region_name, exam_year;"""