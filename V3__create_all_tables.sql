create table sex_type(sex_type_id integer, sex_type_name varchar(255));
create table teretory(teretory_id integer, teretory_name varchar(255), area_id integer, teretory_type_id integer);
create table area(area_id integer, area_name varchar(255), region_id integer);
create table region(region_id integer, region_name varchar(255));
create table teretory_type(teretory_type_id integer, teretory_type_name varchar(255));
create table reg_type(reg_type_id integer, reg_type_name varchar(255));
create table class_profile(class_profile_id integer, class_profile_name varchar(255));
create table subject(subject_id integer, subject_name varchar(255));
create table all_languages(language_id integer, language_name varchar(255));
create table education_place(education_place_id integer, education_place_name varchar(255), teretory_id integer, education_place_type_id integer, education_place_parent_id integer);
create table education_place_types(education_place_type_id integer, education_place_type_name varchar(255));
create table education_place_parent(education_place_parent_id integer, education_place_parent_name varchar(255));
create table students_tests(out_id varchar(255), test_id integer);
create table student(out_id varchar(255), birth integer, exam_year integer, education_place_id integer, language_id integer, class_profile_id integer, teretory_id integer, sex_type_id integer, reg_type_id integer);
create table test(test_id integer, subject_id integer, language_id integer, ball integer, ball100 integer, ball12 integer, status varchar(255), DPA_level VARCHAR(255), adapt_scale integer, education_place_id integer);