# load-rds-mysql-dummy-data
import mysql
import mysql.connector
from mysql.connector import errorcode
import random
import random
from datetime import datetime

import logging
logging.basicConfig(filename="load-database.log",format='%(levelname)s:%(asctime)s:%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

now=datetime.now()
DT_STR = now.strftime("%Y%m%d_%H%M%S")
DB_INITIAL = random.choice(['demo_', 'test_'])
DB_NAME = DB_INITIAL+DT_STR
TABLE_NAME = DB_NAME+"_table"
USER_NAME = "xxx"
DB_PASSWORD = "xxxxxx"
DB_HOST_AURORA_MYSQL = "small-mysql.c1szidrzra9a.ap-southeast-1.rds.amazonaws.com"

def create_table_innodb():
    config = {
        'user': USER_NAME,
        'password': DB_PASSWORD,
        'host': DB_HOST_AURORA_MYSQL,
        'database': DB_NAME,
        'raise_on_warnings': True
    }
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        create_table_sql = f""" CREATE TABLE {TABLE_NAME} (
                          `commit_index` bigint NOT NULL,
                          `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                          `user_id` bigint NOT NULL,
                          `body1` char(32) DEFAULT (MD5(RAND())),
                          `body2` char(32) DEFAULT (MD5(RAND())),
                          `body3` char(32) DEFAULT (MD5(RAND())),
                          `body4` char(32) DEFAULT (MD5(RAND())),
                          `body5` char(32) DEFAULT (MD5(RAND())),
                          `body6` char(32) DEFAULT (MD5(RAND())),
                          `body7` char(32) DEFAULT (MD5(RAND())),
                          `body8` char(32) DEFAULT (MD5(RAND())),
                          `body9` char(32) DEFAULT (MD5(RAND())),
                          `body10` char(32) DEFAULT (MD5(RAND())),
                          `body11` char(32) DEFAULT (MD5(RAND())),
                          `body12` char(32) DEFAULT (MD5(RAND())),
                          `body13` char(32) DEFAULT (MD5(RAND())),
                          `body15` char(32) DEFAULT (MD5(RAND())),
                          `body16` char(32) DEFAULT (MD5(RAND())),
                          `body17` char(32) DEFAULT (MD5(RAND())),
                          `body18` char(32) DEFAULT (MD5(RAND())),
                          `body14` char(32) DEFAULT (MD5(RAND())),
                          `body19` char(32) DEFAULT (MD5(RAND())),
                          `body20` char(32) DEFAULT (MD5(RAND())),
                          `body21` char(32) DEFAULT (MD5(RAND())),
                          `body22` char(32) DEFAULT (MD5(RAND())),
                          `body23` char(32) DEFAULT (MD5(RAND())),
                          `body24` char(32) DEFAULT (MD5(RAND())),
                          `body25` char(32) DEFAULT (MD5(RAND())),
                          `body26` char(32) DEFAULT (MD5(RAND())),
                          `body27` char(32) DEFAULT (MD5(RAND())),
                          `body28` char(32) DEFAULT (MD5(RAND())),
                          `body29` char(32) DEFAULT (MD5(RAND())),
                          `body30` char(32) DEFAULT (MD5(RAND())),
                          PRIMARY KEY (`commit_index`),
                          KEY `idx_uid` (`user_id`)
                        ) ENGINE=InnoDB;
                        """
        cursor.execute(create_table_sql)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.info("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.info("Database does not exist")
        else:
            logger.info(err)
    else:
        conn.close()


def create_database():
    root_config = {
        'user': USER_NAME,
        'password': DB_PASSWORD,
        'host': DB_HOST_AURORA_MYSQL,
        'raise_on_warnings': True
    }
    conn = mysql.connector.connect(**root_config)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'UTF8MB4'".format(DB_NAME))
    except mysql.connector.Error as err:
        logger.info("Failed creating database: {}".format(err))
        exit(1)
    try:
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        logger.info("Database {} does not exists.".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            logger.info("Database {} created successfully.".format(DB_NAME))
        else:
            logger.info(err)
            exit(1)

# single thread batch insert
def batch_insert():
    try:
        config = {
            'user': USER_NAME,
            'password': DB_PASSWORD,
            'host': DB_HOST_AURORA_MYSQL,
            'database': DB_NAME,
            'raise_on_warnings': True
        }
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        batch_insert_sql = f"""INSERT INTO {TABLE_NAME} (commit_index, user_id) 
                          VALUES (%s, %s);
                          """
        r_start = 0  
        r_end = 10000
        batch_size = 200

        seg = int((r_end-r_start) / batch_size)
        logger.info(seg)
        full_list = range(r_start, r_end)
        i = 0
        for count in range(seg):
            try:
                data = []
                for e in full_list[i:i+batch_size]:
                    row = (e, e*2)
                    data.append(row)
                i = i + batch_size
                logger.info(i)
                cursor.executemany(batch_insert_sql, data)
                conn.commit()
            except Exception as e:
                logger.info(f'error is {e}')

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.info("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.info("Database does not exist")
        else:
            logger.info(err)
    else:
        cursor.close()
        conn.close()
        logger.info('done')

create_database()
create_table_innodb()
batch_insert()
