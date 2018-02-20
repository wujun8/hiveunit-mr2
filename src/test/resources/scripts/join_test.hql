CREATE TABLE employees (
        emp_no INT,
        first_name STRING,
        last_name STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '${EMP_DATA}' OVERWRITE INTO TABLE employees;
CREATE TABLE dept_emp (
        emp_no INT,
        dept_no STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '${DEPT_EMP_DATA}' OVERWRITE INTO TABLE dept_emp;
set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask=false;
create table test_results as
    SELECT a.emp_no,a.last_name,b.dept_no
    FROM employees a JOIN dept_emp b ON a.emp_no = b.emp_no;
SELECT COUNT(*) FROM test_results;

