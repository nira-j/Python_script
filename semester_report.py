import os
import psycopg2
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import time


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
        result=connection.execute("select subcode, course,semyr, pub_result, to_date(pub_result,'dd/mm/yyyy')+15 as end_date, count(*) from result  GROUP BY subcode,course,semyr, pub_result ORDER BY to_date(pub_result,'dd/mm/yyyy')+15")
        rows=result.fetchall()
        for row in rows:
            print(row)


def getSubcodeAndYear(engine, result_publish_date):
    with engine.connect() as connection:
        query="select course, subcode, semyr_code from(select distinct course, subcode, semyr_code from result where pub_result = {} GROUP BY course, subcode, semyr_code) as abc".format(result_publish_date,)
        result=connection.execute(query)
        rows=result.fetchall()
        for row in rows:
            print(row)
        return rows


def semester_payment_applied_form_count(engine, table_name, flag, course, subcode, semyr):
    paylist = []
    appliedlist = []
    
    with engine.connect() as connection:
        qry_paylist = f"""
            SELECT DISTINCT description 
            FROM payment 
            WHERE notes->>'sem' = '{semyr}s' 
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

    

def empty_temp_table(engine, temp_table):
    with engine.connect() as connection:
        qry="TRUNCATE TABLE "+temp_table
        connection.execute(qry)

def generateScrutinyReport(engine, subcode, semyr, report_type):
    retot_reportqry=f"""
        insert into scrutiny_report_temp(
select * from (
select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub1p1 as paper, r.sub1p1code as papercode,rr.sub1p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rt' as scrutiny_type from result_retotaling rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub1 is not null and sub1_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub2p1 as paper, r.sub2p1code as papercode,rr.sub2p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rt' as scrutiny_type from result_retotaling rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub2 is not null and sub2_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub3p1 as paper, r.sub3p1code as papercode,rr.sub3p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rt' as scrutiny_type from result_retotaling rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub3 is not null and sub3_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub4p1 as paper, r.sub4p1code as papercode,rr.sub4p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rt' as scrutiny_type from result_retotaling rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub4 is not null and sub4_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub5p1 as paper, r.sub5p1code as papercode,rr.sub5p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rt' as scrutiny_type from result_retotaling rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub5 is not null and sub5_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub6p1 as paper, r.sub6p1code as papercode,rr.sub6p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rt' as scrutiny_type from result_retotaling rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub6 is not null and sub6_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub7p1 as paper, r.sub7p1code as papercode,rr.sub7p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rt' as scrutiny_type from result_retotaling rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub7 is not null and sub7_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub8p1 as paper, r.sub8p1code as papercode,rr.sub8p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rt' as scrutiny_type from result_retotaling rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub8 is not null and sub8_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub9p1 as paper, r.sub9p1code as papercode,rr.sub9p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rt' as scrutiny_type from result_retotaling rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub9 is not null and sub9_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub10p1 as paper, r.sub10p1code as papercode,rr.sub10p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rt' as scrutiny_type from result_retotaling rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub10 is not null and sub10_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub11p1 as paper, r.sub11p1code as papercode,rr.sub11p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rt' as scrutiny_type from result_retotaling rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub11 is not null and sub11_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub12p1 as paper, r.sub12p1code as papercode,rr.sub12p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rt' as scrutiny_type from result_retotaling rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub12 is not null and sub12_retotaling='true'

) as abc where abc.subcode in ('{subcode}') and abc.payment_status='0300' and abc.semyr_code='{semyr}' and abc.scanno not in (select scanno::int from scrutiny_report) and abc.scanno in (select description::int from payment where status='captured' and notes->>'pay_for'='RETOT'))
        
    """

    reval_reportqry=f"""
            insert into scrutiny_report_temp(
select * from (
select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub1p1 as paper, r.sub1p1code as papercode,rr.sub1p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rv' as scrutiny_type from result_reval rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub1 is not null and sub1_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub2p1 as paper, r.sub2p1code as papercode,rr.sub2p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rv' as scrutiny_type from result_reval rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub2 is not null and sub2_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub3p1 as paper, r.sub3p1code as papercode,rr.sub3p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rv' as scrutiny_type from result_reval rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub3 is not null and sub3_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub4p1 as paper, r.sub4p1code as papercode,rr.sub4p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rv' as scrutiny_type from result_reval rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub4 is not null and sub4_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub5p1 as paper, r.sub5p1code as papercode,rr.sub5p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rv' as scrutiny_type from result_reval rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub5 is not null and sub5_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub6p1 as paper, r.sub6p1code as papercode,rr.sub6p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rv' as scrutiny_type from result_reval rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub6 is not null and sub6_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub7p1 as paper, r.sub7p1code as papercode,rr.sub7p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rv' as scrutiny_type from result_reval rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub7 is not null and sub7_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub8p1 as paper, r.sub8p1code as papercode,rr.sub8p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rv' as scrutiny_type from result_reval rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub8 is not null and sub8_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub9p1 as paper, r.sub9p1code as papercode,rr.sub9p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rv' as scrutiny_type from result_reval rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub9 is not null and sub9_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub10p1 as paper, r.sub10p1code as papercode,rr.sub10p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rv' as scrutiny_type from result_reval rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub10 is not null and sub10_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub11p1 as paper, r.sub11p1code as papercode,rr.sub11p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rv' as scrutiny_type from result_reval rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub11 is not null and sub11_retotaling='true' union

select rr.scanno,rr.ansidrno, rr.enrollment_no, rr.course, rr.semyr_code, rr.name,rr.fname,payment_status,rr.centrename,rr.centrecode,rr.subcode, r.sub12p1 as paper, r.sub12p1code as papercode,rr.sub12p1_th as marks,to_char(app_date, 'dd/mm/yyyy') as app_date, 'rv' as scrutiny_type from result_reval rr,result r where r.id=rr.id and rr.retotaling_status='Y' and rr.sub12 is not null and sub12_retotaling='true'

) as abc where abc.subcode in ('{subcode}') and abc.payment_status='0300' and abc.semyr_code='{semyr}' and abc.scanno not in (select scanno::int from scrutiny_report where scrutiny_type='rv') and abc.scanno in (select description::int from payment where status='captured' and notes->>'pay_for'='REVAL'))
            
    """
    
    with engine.begin() as connection:
        try:
            if(report_type=='retot'):
                connection.execute(retot_reportqry)
                print(f"Retotaling report generated for {subcode} and semyr {semyr}")

            elif(report_type=='reval'):
                connection.execute(reval_reportqry)
                print(f"Revaluation report generated for {subcode} and semyr {semyr}")

        except:
            print("something went wrong ..")
        
def dump_report_in_excel(engine, filename):
    path='C://Users//madhu//Desktop//tables//'
    df = pd.read_sql("SELECT * FROM scrutiny_report_temp", engine)
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
    rowscount=getSubcodeAndYear(pg_engine,"'06/08/2024'")
    l=len(rowscount)-1
    for i,row in enumerate(rowscount):
        course=row[0]
        subcode=row[1]
        semyr=row[2]
        retotpayCount, retotappliedCount=semester_payment_applied_form_count(pg_engine,'result_retotaling', 'RETOT', course, subcode, semyr)
        if i == 0:
            empty_temp_table(pg_engine, 'scrutiny_report_temp')
        if(len(retotpayCount)!=0):
            print("{}, {} Retotaling payment done and applied form count-->".format(subcode, semyr), len(retotpayCount), len(retotappliedCount))
        if(len(retotpayCount) != len(retotappliedCount)):
            print(" ############### Retotaling count mismatch ...##############")
        elif(len(retotpayCount) == len(retotappliedCount) and len(retotpayCount)!=0 and len(retotappliedCount)!=0):
            generateScrutinyReport(pg_engine, subcode, semyr, 'retot')
            time.sleep(0.5)
        if i == l:
            dump_report_in_excel(pg_engine,'semester_scrutiny_report_retotaling')

    for i,row in enumerate(rowscount):
        course=row[0]
        subcode=row[1]
        semyr=row[2]

        revalpayCount, revalappliedCount=semester_payment_applied_form_count(pg_engine,'result_reval', 'REVAL', course, subcode, semyr)
        if i == 0:
            empty_temp_table(pg_engine, 'scrutiny_report_temp')
        if(len(revalpayCount)!=0):
            print("{}, {} Revaluation payment done and applied form count-->".format(subcode, semyr),len(revalpayCount), len(revalappliedCount))

        if(len(revalpayCount) != len(revalappliedCount)):
            print(" ############### Reval count mismatch ...##############")

        elif(len(revalpayCount) == len(revalappliedCount) and len(revalpayCount)!=0 and len(revalappliedCount)!=0):
            generateScrutinyReport(pg_engine, subcode, semyr, 'reval')
            time.sleep(0.5)
        if i == l:
            dump_report_in_excel(pg_engine,'semester_scrutiny_report_reval')
 