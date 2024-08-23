import os
import psycopg2
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import time


# class DocumentDetails(Base):
#     __tablename__='DocumentDetails'
    
#     id=Column(Integer, primary_key=True)
#     lable=Column(String(100))
#     coll_code=Column(Integer)
#     date=Column(String(25))
#     src_location=Column(String(100))

admitcard_list=[]
payment_list=[]

def process(srcpath):
    Session=sessionmaker(bind=pg_engine)
    session=Session()
    dirList=os.listdir(srcpath)
    for dir in dirList:
        if dir.split(' ')[0]=='admission_approval':
            print("hii")
            admitcard_list.append(dir)
            obj=DocumentDetails(lable=dir.split(' ')[0], coll_code=dir.split(' ')[1], date= dir.split(' ')[2], src_location=srcpath)
            session.add(obj)

        # if dir.split(' - ')[0]=='payments':
        #     payment_list.append(dir)
        #     obj1=DocumentDetails(lable=dir.split(' _ ')[0], coll_code=dir.split(' _ ')[1], date= dir.split(' _ ')[2], src_location=srcpath)
        #     session.add(obj1)

    session.commit()

def importPaymentData(engine):

    path="D://my_workspace//"
    try:
        dirlist=os.listdir('D://my_workspace//')
        files = [os.path.join(path, file) for file in dirlist if file.startswith("payments")]
        files_sorted_by_date = sorted(files, key=os.path.getmtime)

        print("payment excel report file:- ", files_sorted_by_date[-1])

        df = pd.read_excel(files_sorted_by_date[-1])
        df.to_sql('payment', engine, if_exists='replace', index=False)
        print("Payment excel report file imported successfully..")
    except:
        print("somthing went wromg..")


def importResultTables(engine):
    path="D://PRSU_DUMP//result_annual.sql"
    
    os.system("psql -U postgres -h localhost -p 5432 -d prsu_erp -f "+path)
    print("success")



def getStatus(engine):
    with engine.connect() as connection:
        result=connection.execute("select subcode, course,semyr, pub_result,(to_date(pub_result,'dd/mm/yyyy')+15)::text as end_date, count(*) from result_annual GROUP BY subcode,course,semyr, pub_result ORDER BY to_date(pub_result,'dd/mm/yyyy')+15")
        rows=result.fetchall()
        for row in rows:
            print(row)


def getSubcodeAndYear(engine, result_publish_date):
    with engine.connect() as connection:
        query="select course, subcode, semyr_code from(select distinct course, subcode, semyr_code from result_annual where pub_result = {} GROUP BY course,subcode,semyr_code) as abc".format(result_publish_date,)
        result=connection.execute(query)
        rows=result.fetchall()
        for row in rows:
            print(row)
        return rows


def annual_Payment_applied_form_count(engine, table_name, flag, course, subcode, semyr):
    paylist = []
    appliedlist = []
    
    with engine.connect() as connection:
        qry_paylist = f"""
            SELECT DISTINCT description 
            FROM payment 
            WHERE notes->>'sem' = '{semyr}y' 
            AND notes->>'course' = '{course}' 
            AND status = 'captured' 
            AND notes->>'pay_for' = '{flag}'
        """
        
        result = connection.execute(qry_paylist, {'semyr': semyr, 'course': course})
        paylist = [row[0] for row in result.fetchall()]
    
        qry_appliedlist = f"""
            SELECT scanno 
            FROM {table_name} 
            WHERE subcode = '{subcode}' 
            AND semyr_code = '{semyr}' 
            AND payment_status = '0300' 
            AND retotaling_status = 'Y'
            AND scanno IN (
                SELECT description::int 
                FROM payment 
                WHERE status = 'captured' 
                AND notes->>'pay_for' = '{flag}'
            )
        """
        result = connection.execute(qry_appliedlist, {'subcode': subcode, 'semyr': semyr})
        appliedlist = [row[0] for row in result.fetchall()]
    
    return paylist, appliedlist

    
def mismatchList(l1, l2):
    s1=set(l1)
    s2=set(l2)

    if(s1==s2):
        print("true")

    # return [ele for ele in l1 if ele not in l2]

def empty_temp_table(engine, temp_table):
    with engine.connect() as connection:
        qry="TRUNCATE TABLE "+temp_table
        connection.execute(qry)

def generateScrutinyReport(engine, subcode, semyr, report_type):
    annual_retot_reportqry=f"""
        insert into annual_scrutiny_report_temp(
        select * from(
        --sub1p1
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub1p1 as paper, r.sub1p1code as papercode,rr.sub1p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub1p1 is not null and rr.sub1p1code is not null

        --sub1p2
        union
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub1p2 as paper, r.sub1p2code as papercode,rr.sub1p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub1p2 is not null and rr.sub1p2code is not null

        --sub1p3
        union
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub1p3 as paper, r.sub1p3code as papercode,rr.sub1p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub1p3 is not null and rr.sub1p3code is not null

        --sub2p1
        union
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub2p1 as paper, r.sub2p1code as papercode,rr.sub2p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub2p1 is not null and rr.sub2p1code is not null

        --sub2p2
        union
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub2p2 as paper, r.sub2p2code as papercode,rr.sub2p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub2p2 is not null and rr.sub2p2code is not null

        --sub2p3
        union
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub2p3 as paper, r.sub2p3code as papercode,rr.sub2p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub2p3 is not null and rr.sub2p3code is not null

        --sub3p1
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub3p1 as paper, r.sub3p1code as papercode,rr.sub3p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub3p1 is not null and rr.sub3p1code is not null

        --sub3p2
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub3p2 as paper, r.sub3p2code as papercode,rr.sub3p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub3p2 is not null and rr.sub3p2code is not null

        --sub3p3
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub3p3 as paper, r.sub3p3code as papercode,rr.sub3p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub3p3 is not null and rr.sub3p3code is not null

        --sub3p4
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub3p4 as paper, r.sub3p4code as papercode,rr.sub3p4_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub3p4 is not null and rr.sub3p4code is not null

        --sub4p1
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub4p1 as paper, r.sub4p1code as papercode,rr.sub4p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub4p1 is not null and rr.sub4p1code is not null

        --sub4p2
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub4p2 as paper, r.sub4p2code as papercode,rr.sub4p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub4p2 is not null and rr.sub4p2code is not null

        --sub4p3
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub4p3 as paper, r.sub4p3code as papercode,rr.sub4p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub4p3 is not null and rr.sub4p3code is not null

        --sub4p4
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub4p4 as paper, r.sub4p4code as papercode,rr.sub4p4_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub4p4 is not null and rr.sub4p4code is not null

        --sub5p1
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub5p1 as paper, r.sub5p1code as papercode,rr.sub5p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub5p1 is not null and rr.sub5p1code is not null

        --sub5p2
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub5p2 as paper, r.sub5p2code as papercode,rr.sub5p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub5p2 is not null and rr.sub5p2code is not null

        --sub5p3
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub5p3 as paper, r.sub5p3code as papercode,rr.sub5p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub5p3 is not null and rr.sub5p3code is not null

        --sub5p4
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub5p4 as paper, r.sub5p4code as papercode,rr.sub5p4_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub5p4 is not null and rr.sub5p4code is not null

        --sub6p1
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub6p1 as paper, r.sub6p1code as papercode,rr.sub6p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub6p1 is not null and rr.sub6p1code is not null

        --sub6p2
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub6p2 as paper, r.sub6p2code as papercode,rr.sub6p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub6p2 is not null and rr.sub6p2code is not null

        --sub6p3
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub6p3 as paper, r.sub6p3code as papercode,rr.sub6p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub6p3 is not null and rr.sub6p3code is not null

        --sub6p4
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub6p4 as paper, r.sub6p4code as papercode,rr.sub6p4_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub6p4 is not null and rr.sub6p4code is not null

        --sub7p1
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub7p1 as paper, r.sub7p1code as papercode,rr.sub7p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub7p1 is not null and rr.sub7p1code is not null

        --sub7p2
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub7p2 as paper, r.sub7p2code as papercode,rr.sub7p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub7p2 is not null and rr.sub7p2code is not null

        --sub7p3
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub7p3 as paper, r.sub7p3code as papercode,rr.sub7p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub7p3 is not null and rr.sub7p3code is not null

        --sub8p1
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub8p1 as paper, r.sub8p1code as papercode,rr.sub8p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub8p1 is not null and rr.sub8p1code is not null

        --sub8p2
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub8p2 as paper, r.sub8p2code as papercode,rr.sub8p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub8p2 is not null and rr.sub8p2code is not null

        --sub8p3
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub8p3 as paper, r.sub8p3code as papercode,rr.sub8p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub8p3 is not null and rr.sub8p3code is not null

        --sub9p1
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub9p1 as paper, r.sub9p1code as papercode,rr.sub9p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub9p1 is not null and rr.sub9p1code is not null

        --sub9p2
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub9p2 as paper, r.sub9p2code as papercode,rr.sub9p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub9p2 is not null and rr.sub9p2code is not null

        --sub9p3
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub9p3 as paper, r.sub9p3code as papercode,rr.sub9p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub9p3 is not null and rr.sub9p3code is not null

        --sub10p1
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub10p1 as paper, r.sub10p1code as papercode,rr.sub10p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub10p1 is not null and rr.sub10p1code is not null

        --sub10p2
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub10p2 as paper, r.sub10p2code as papercode,rr.sub10p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub10p2 is not null and rr.sub10p2code is not null

        --sub10p3
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub10p3 as paper, r.sub10p3code as papercode,rr.sub10p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub10p3 is not null and rr.sub10p3code is not null

        --sub11p1
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub11p1 as paper, r.sub11p1code as papercode,rr.sub11p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub11p1 is not null and rr.sub11p1code is not null

        --sub11p2
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub11p2 as paper, r.sub11p2code as papercode,rr.sub11p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub11p2 is not null and rr.sub11p2code is not null

        --sub11p3
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub11p3 as paper, r.sub11p3code as papercode,rr.sub11p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub11p3 is not null and rr.sub11p3code is not null

        --sub12p1
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub12p1 as paper, r.sub12p1code as papercode,rr.sub12p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub12p1 is not null and rr.sub12p1code is not null

        --sub12p2
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub12p2 as paper, r.sub12p2code as papercode,rr.sub12p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub12p2 is not null and rr.sub12p2code is not null

        --sub12p3
        union 
        select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub12p3 as paper, r.sub12p3code as papercode,rr.sub12p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rt' as scrutiny_type from result_annual_retotaling rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub12p3 is not null and rr.sub12p3code is not null

        ) as abc where abc.payment_status='0300' and abc.subcode in ('{subcode}') and abc.semyr_code in ('{semyr}') and scanno not in (select scanno :: int from annual_scrutiny_report where scrutiny_type='rt')
        )
        
    """

    annual_reval_reportqry=f"""
            insert into annual_scrutiny_report_temp(
            select * from(
            --sub1p1
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub1p1 as paper, r.sub1p1code as papercode,rr.sub1p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and (rr.sub1p1 is not null or rr.sub1p1code is not null)


            --sub1p2
            union
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub1p2 as paper, r.sub1p2code as papercode,rr.sub1p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub1p2 is not null and rr.sub1p2code is not null

            --sub1p3
            union
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub1p3 as paper, r.sub1p3code as papercode,rr.sub1p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub1p3 is not null and rr.sub1p3code is not null

            --sub2p1
            union
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub2p1 as paper, r.sub2p1code as papercode,rr.sub2p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub2p1 is not null and rr.sub2p1code is not null

            --sub2p2
            union
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub2p2 as paper, r.sub2p2code as papercode,rr.sub2p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub2p2 is not null and rr.sub2p2code is not null

            --sub2p3
            union
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub2p3 as paper, r.sub2p3code as papercode,rr.sub2p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub2p3 is not null and rr.sub2p3code is not null

            --sub3p1
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub3p1 as paper, r.sub3p1code as papercode,rr.sub3p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub3p1 is not null and rr.sub3p1code is not null

            --sub3p2
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub3p2 as paper, r.sub3p2code as papercode,rr.sub3p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub3p2 is not null and rr.sub3p2code is not null

            --sub3p3
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub3p3 as paper, r.sub3p3code as papercode,rr.sub3p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub3p3 is not null and rr.sub3p3code is not null

            --sub3p4
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub3p4 as paper, r.sub3p4code as papercode,rr.sub3p4_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub3p4 is not null and rr.sub3p4code is not null

            --sub4p1
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub4p1 as paper, r.sub4p1code as papercode,rr.sub4p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub4p1 is not null and rr.sub4p1code is not null

            --sub4p2
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub4p2 as paper, r.sub4p2code as papercode,rr.sub4p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub4p2 is not null and rr.sub4p2code is not null

            --sub4p3
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub4p3 as paper, r.sub4p3code as papercode,rr.sub4p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub4p3 is not null and rr.sub4p3code is not null

            --sub4p4
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub4p4 as paper, r.sub4p4code as papercode,rr.sub4p4_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub4p4 is not null and rr.sub4p4code is not null

            --sub5p1
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub5p1 as paper, r.sub5p1code as papercode,rr.sub5p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub5p1 is not null and rr.sub5p1code is not null

            --sub5p2
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub5p2 as paper, r.sub5p2code as papercode,rr.sub5p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub5p2 is not null and rr.sub5p2code is not null

            --sub5p3
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub5p3 as paper, r.sub5p3code as papercode,rr.sub5p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub5p3 is not null and rr.sub5p3code is not null

            --sub5p4
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub5p4 as paper, r.sub5p4code as papercode,rr.sub5p4_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub5p4 is not null and rr.sub5p4code is not null

            --sub6p1
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub6p1 as paper, r.sub6p1code as papercode,rr.sub6p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub6p1 is not null and rr.sub6p1code is not null

            --sub6p2
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub6p2 as paper, r.sub6p2code as papercode,rr.sub6p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub6p2 is not null and rr.sub6p2code is not null

            --sub6p3
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub6p3 as paper, r.sub6p3code as papercode,rr.sub6p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub6p3 is not null and rr.sub6p3code is not null

            --sub6p4
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub6p4 as paper, r.sub6p4code as papercode,rr.sub6p4_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub6p4 is not null and rr.sub6p4code is not null

            --sub7p1
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub7p1 as paper, r.sub7p1code as papercode,rr.sub7p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub7p1 is not null and rr.sub7p1code is not null

            --sub7p2
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub7p2 as paper, r.sub7p2code as papercode,rr.sub7p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub7p2 is not null and rr.sub7p2code is not null

            --sub7p3
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub7p3 as paper, r.sub7p3code as papercode,rr.sub7p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub7p3 is not null and rr.sub7p3code is not null

            --sub8p1
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub8p1 as paper, r.sub8p1code as papercode,rr.sub8p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub8p1 is not null and rr.sub8p1code is not null

            --sub8p2
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub8p2 as paper, r.sub8p2code as papercode,rr.sub8p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub8p2 is not null and rr.sub8p2code is not null

            --sub8p3
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub8p3 as paper, r.sub8p3code as papercode,rr.sub8p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub8p3 is not null and rr.sub8p3code is not null

            --sub9p1
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub9p1 as paper, r.sub9p1code as papercode,rr.sub9p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub9p1 is not null and rr.sub9p1code is not null

            --sub9p2
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub9p2 as paper, r.sub9p2code as papercode,rr.sub9p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub9p2 is not null and rr.sub9p2code is not null

            --sub9p3
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub9p3 as paper, r.sub9p3code as papercode,rr.sub9p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub9p3 is not null and rr.sub9p3code is not null

            --sub10p1
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub10p1 as paper, r.sub10p1code as papercode,rr.sub10p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub10p1 is not null and rr.sub10p1code is not null

            --sub10p2
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub10p2 as paper, r.sub10p2code as papercode,rr.sub10p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub10p2 is not null and rr.sub10p2code is not null

            --sub10p3
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub10p3 as paper, r.sub10p3code as papercode,rr.sub10p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub10p3 is not null and rr.sub10p3code is not null

            --sub11p1
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub11p1 as paper, r.sub11p1code as papercode,rr.sub11p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub11p1 is not null and rr.sub11p1code is not null

            --sub11p2
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub11p2 as paper, r.sub11p2code as papercode,rr.sub11p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub11p2 is not null and rr.sub11p2code is not null

            --sub11p3
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub11p3 as paper, r.sub11p3code as papercode,rr.sub11p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub11p3 is not null and rr.sub11p3code is not null

            --sub12p1
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub12p1 as paper, r.sub12p1code as papercode,rr.sub12p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub12p1 is not null and rr.sub12p1code is not null

            --sub12p2
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub12p2 as paper, r.sub12p2code as papercode,rr.sub12p2_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub12p2 is not null and rr.sub12p2code is not null

            --sub12p3
            union 
            select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name, rr.fname, payment_status, rr.centrename, rr.centrecode,rr.subcode, r.sub12p3 as paper, r.sub12p3code as papercode,rr.sub12p3_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date,'rv' as scrutiny_type from result_annual_reval rr, result_annual r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub12p3 is not null and rr.sub12p3code is not null
            ) as abc where abc.payment_status='0300' and abc.subcode in ('{subcode}') and abc.semyr_code in ('{semyr}')
            and scanno not in (select scanno :: int from annual_scrutiny_report where scrutiny_type='rv')
            )
            
        """
    
    with engine.begin() as connection:
        try:
            if(report_type=='retot'):
                connection.execute(annual_retot_reportqry)
                print(f"Retotaling report generated for {subcode} and semyr {semyr}")

            elif(report_type=='reval'):
                connection.execute(annual_reval_reportqry)
                print(f"Revaluation report generated for {subcode} and semyr {semyr}")

        except:
            print("something went wrong ..")
        
def dump_report_in_excel(engine, filename):
    path='C://Users//madhu//Desktop//'
    df = pd.read_sql("SELECT * FROM annual_scrutiny_report_temp", engine)
    df.to_excel(path+filename+'.xlsx', index=False)

    print("Report successfully dumped in location:- "+path)

def dump_excel_report():
    pass



if __name__=='__main__':
    pg_engine=create_engine("postgresql://postgres:postgres@localhost:5432/prsu_erp")
    Base=declarative_base()

    # importPaymentData(pg_engine)
    # importResultTables(pg_engine)
 
    # getStatus(pg_engine) pub_date
    rowscount=getSubcodeAndYear(pg_engine,"'04/07/2024'")
    l=len(rowscount)-1
    for i,row in enumerate(rowscount):
        course=row[0]
        subcode=row[1]
        semyr=row[2]
        retotpayCount, retotappliedCount=annual_Payment_applied_form_count(pg_engine,'result_annual_retotaling', 'RETOT_ANNUAL', course, subcode, semyr)
        revalpayCount, revalappliedCount=annual_Payment_applied_form_count(pg_engine,'result_annual_reval', 'REVAL_ANNUAL', course, subcode, semyr)
        print("Retotaling payment done and applied form count-->", len(retotpayCount), len(retotappliedCount))
        print("Revaluation payment done and applied form count-->",len(revalpayCount), len(revalappliedCount))
        if(len(retotpayCount) != len(retotappliedCount)):
            # mismatchList(retotpayCount, retotappliedCount)
            print(" ############### Retotaling count mismatch ...##############")
        elif(len(retotpayCount) == len(retotappliedCount) and len(retotpayCount)!=0 and len(retotappliedCount)!=0):
            if i == 0:
                empty_temp_table(pg_engine, 'annual_scrutiny_report_temp')
            generateScrutinyReport(pg_engine, subcode, semyr, 'retot')
            time.sleep(0.5)
            if i == l:
                dump_report_in_excel(pg_engine,'annual_scrutiny_report_retotaling')

        if(len(revalpayCount) != len(revalappliedCount)):
            print(" ############### Reval count mismatch ...##############")

        if(len(revalpayCount) != len(revalappliedCount) and len(revalpayCount)!=0 and len(revalappliedCount)!=0):
            # //////////annual revaluation ////////////
            if i == 0:
                empty_temp_table(pg_engine, 'annual_scrutiny_report_temp')
            generateScrutinyReport(pg_engine, subcode, semyr, 'reval')
            time.sleep(0.5)
            if i == l:
                dump_report_in_excel(pg_engine,'annual_scrutiny_report_reval')


    
    

 