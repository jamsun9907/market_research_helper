# MongoDB에 적재된 데이터를 RDB에 정제하여 적재하는 파일

import psycopg2
import config

def get_mongo_data():
    """
    MongoDB에서 데이터를 불러오는 함수
    :return: MongoDB에서 얻은 rows
    """
    pass


def transpose_data(rows):
    """
    row를 판다스 데이터프레임으로 만든 뒤
    row를 1시간 단위로 집계한다.
    그 외 필요한 정제작업이 있으면 그것도 함
    - 중복제거
    :param row:
    :return: Dataframe
    """
    pass


# PostgreSQL
def get_connection():
    """
    PostgreSQL의 connection을 get.

    :return:
    connection
    """
    connection = psycopg2.connect(
        host=config.host,
        user=config.user,
        password=config.password,
        database=config.db
    )
    return connection


def load_on_sql(row, connection):
    """
    raw 데이터를 PostgreSQL에 적재한다.
    Dataframe 형태를
    :return:
    None
    """
    cur = connection.cursor()
    columns = 'MODEL_NM, SERIAL_NO, SENSING_TIME, REGION, AUTONOMOUS_DISTRICT, ADMINISTRATIVE_DISTRICT, VISITOR_COUNT, REG_DTTM'
    query = f'INSERT INTO population({columns}) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'

    # INSERT DATA
    try:
        # df를 insert
        cur.execute(query, tuple(row.values()))
    except (Exception, psycopg2.Error) as e:
        raise

    finally:
        print("done!!")

    return None



# test

rows = get_data(page=3)

connection = get_connection()
cur = connection.cursor()

# INSERT DATA
for row in rows:
    try:
        row['SENSING_TIME'] = row['SENSING_TIME'].replace('_', ' ')
        load_data(row, connection)
        print(f'{len(rows)} records loaded ')
    except:
        print("pass inserting a data that has incorrect datatype")

    finally:
        connection.commit()
