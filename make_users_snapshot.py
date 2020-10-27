import psycopg2
from datetime import datetime
from pipeline import Pipeline

def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE cps (
            SERIAL PRIMARY KEY,
            HHID VARCHAR(255) NOT NULL,
            Person_type VARCHAR(255) NOT NULL,
            Interview_Status VARCHAR(255) NOT NULL,
            Age INTEGER,
            Sex VARCHAR(255) NOT NULL,
            Race VARCHAR(255) NOT NULL,                    
            Hispanic VARCHAR(255) NOT NULL,                     
            LF_recode VARCHAR(255) NOT NULL,                    
            LF_recode2 VARCHAR(255) NOT NULL,            
            Civilian_LF VARCHAR(255) NOT NULL,                  
            Employed_nonfarm VARCHAR(255) NOT NULL,             
            Have_job VARCHAR(255) NOT NULL,                     
            Unpaid_family_work VARCHAR(255) NOT NULL,           
            Recall_return VARCHAR(255) NOT NULL,                
            Recall_look VARCHAR(255) NOT NULL,                  
            Job_offered VARCHAR(255) NOT NULL,                  
            Job_offered_week VARCHAR(255) NOT NULL,              
            Available_ft VARCHAR(255) NOT NULL,                 
            Job_search VARCHAR(255) NOT NULL,                   
            Look_last_month VARCHAR(255) NOT NULL,              
            Look_last_year VARCHAR(255) NOT NULL,               
            Last_work VARCHAR(255) NOT NULL,                    
            Discouraged VARCHAR(255) NOT NULL,                  
            Retired VARCHAR(255) NOT NULL,                      
            Disabled VARCHAR(255) NOT NULL, VARCHAR(255) NOT NULL,                    
            Situation VARCHAR(255) NOT NULL,                    
            FT_PT VARCHAR(255) NOT NULL,                        
            FT_PT_status VARCHAR(255) NOT NULL,                 
            Detailed_reason_part_time VARCHAR(255) NOT NULL,    
            Main_reason_part_time VARCHAR(255) NOT NULL,        
            Main_reason_not_full_time VARCHAR(255) NOT NULL,    
            Want_job VARCHAR(255) NOT NULL,                     
            Want_job_ft VARCHAR(255) NOT NULL,                  
            Want_job_ft_pt VARCHAR(255) NOT NULL,               
            Want_job_nilf VARCHAR(255) NOT NULL,                
            Reason_unemployment VARCHAR(255) NOT NULL,          
            Reason_not_looking VARCHAR(255) NOT NULL,           
            Hours_per_week INTEGER,
            Hours_per_week_last INTEGER,
            In_school VARCHAR(255) NOT NULL,                    
            In_school_ft_pt VARCHAR(255) NOT NULL,              
            School_type VARCHAR(255) NOT NULL,                  
            In_school_nilf VARCHAR(255) NOT NULL,               
            State_FIPS VARCHAR(255) NOT NULL,                   
            County_FIPS VARCHAR(255) NOT NULL,                  
            Metro_Code VARCHAR(255) NOT NULL,                   
            Metro_Size VARCHAR(255) NOT NULL,                  
            Metro_Status VARCHAR(255) NOT NULL,                 
            Region VARCHAR(255) NOT NULL,                       
            Division VARCHAR(255) NOT NULL,                    
            Year INTEGER,
            Month INTEGER,
            FIPS VARCHAR(255) NOT NULL,                         
            Age_group INTEGER,
            Retired_want_work VARCHAR(255) NOT NULL,          
            Want_work VARCHAR(255) NOT NULL                   
        )
        """
    )

    for command in commands:
        cur.execute(command)
    # close communication with the PostgreSQL database server
    cur.close()
    # commit the changes
    conn.commit()
    

if __name__ == '__main__':
    conn = psycopg2.connect(database="PublicData", user="psql", password="psql", host="127.0.0.1")
    create_tables()


    # today = '2014-08-14'
    # date = datetime.strptime(today, '%Y-%m-%d').strftime("%Y%m%d")

    #pipeline = Pipeline(conn)

    # pipeline.add_step('''CREATE TEMPORARY TABLE tmp_friends AS
    # SELECT userid1 AS userid, COUNT(*) AS num_friends
    # FROM
    # (SELECT userid1, userid2 FROM friends
    # UNION
    # SELECT userid2, userid1 FROM friends) a
    # GROUP BY userid1;
    # ''')

    # pipeline.add_step('''CREATE TEMPORARY TABLE tmp_logins AS
    # SELECT a.userid, a.cnt AS logins_7d_mobile, b.cnt AS logins_7d_web
    # FROM
    # (SELECT userid, COUNT(*) AS cnt
    # FROM logins
    # WHERE logins.tmstmp > timestamp %(date)s - interval '7 days'
    # AND type='mobile'
    # GROUP BY userid) a
    # JOIN
    # (SELECT userid, COUNT(*) AS cnt
    # FROM logins
    # WHERE logins.tmstmp > timestamp %(date)s - interval '7 days' AND type='web'
    # GROUP BY userid) b
    # ON a.userid=b.userid;
    # ''', {'date': date})

    # pipeline.add_step('''CREATE TABLE users_snapshot AS
    # SELECT a.userid, a.reg_date, a.last_login, f.num_friends,
    #     COALESCE(l.logins_7d_mobile + l.logins_7d_web, 0) AS logins_7d,
    #     COALESCE(l.logins_7d_mobile, 0) AS logins_7d_mobile,
    #     COALESCE(l.logins_7d_web, 0) AS logins_7d_web,
    #     CASE WHEN optout.userid IS NULL THEN 0 ELSE 1 END AS optout,
    #     timestamp %(date)s AS date_7d
    # FROM
    # (SELECT r.userid, r.tmstmp::date AS reg_date, MAX(l.tmstmp::date)
    # AS last_login
    # FROM registrations r
    # LEFT OUTER JOIN logins l
    # ON r.userid=l.userid
    # GROUP BY r.userid, r.tmstmp) a
    # LEFT OUTER JOIN tmp_friends f
    # ON f.userid=a.userid
    # LEFT OUTER JOIN tmp_logins l
    # ON l.userid=a.userid
    # LEFT OUTER JOIN optout
    # ON a.userid=optout.userid;
    # ''', {'date': date})

    # pipeline.execute()
    # pipeline.close()
