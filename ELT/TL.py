# MongoDB에 적재된 데이터를 RDB에 정제하여 적재하는 파일

import pandas as pd
import psycopg2
from psycopg2 import extras
import config
from pipeline import get_mongo_collection


def get_mongo_data():
    """
    MongoDB에서 데이터를 불러오는 함수
    :return: MongoDB에서 얻은 rows의 dataframe
    """
    collection = get_mongo_collection()
    print('Fetching data from MongoDB...')

    # CMongoDB에서 데이터를 불러와 데이터 프레임으로 변환
    cursor = collection.find()
    list_cur = list(cursor)
    df = pd.DataFrame(list_cur)

    # 중복 및 필요 없는 컬럼 제거
    df.drop(['_id', 'REG_DTTM'], axis=1, inplace=True)
    df.drop_duplicates(inplace=True)

    return df

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


def load_on_sql(df):
    """
    raw 데이터를 PostgreSQL에 적재한다.
    Dataframe 형태를
    :return:
    None
    """
    connection = get_connection()
    cur = connection.cursor()

    columns = ','.join(df.columns)

    # table creation
    cur.execute("""CREATE TABLE IF NOT EXISTS population_Seoul(
                    Id SERIAL PRIMARY key,
                    MODEL_NM VARCHAR(255) NOT null,
                    SERIAL_NO VARCHAR(255) NOT null,
                    SENSING_TIME TIMESTAMP NOT null,
                    REGION VARCHAR(255) NOT null,
                    AUTONOMOUS_DISTRICT VARCHAR(255),
                    VISITOR_COUNT INTEGER);""")

    # insert query
    query = f'INSERT INTO population({columns}) VALUES(%s,%s,%s,%s,%s,%s,%s)'

    # INSERT DATA
    try:
        # df를 insert
        print(f'trying to load rows...')
        extras.execute_batch(cur=cur, sql=query, argslist=tuple(df.values))

        cur.close()
        connection.commit()
        print('completed', end=' ')

    except (Exception, psycopg2.Error) as e:
        raise

    finally:
        connection.close()
        print("done!!")


# test

if __name__ == '__main__':
    df = get_mongo_data()
    load_on_sql(df)


