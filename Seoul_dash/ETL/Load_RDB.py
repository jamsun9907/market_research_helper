# MongoDB에 적재된 데이터를 RDB에 정제하여 적재하는 파일

import pandas as pd
import psycopg2
from psycopg2 import extras
import config
from Pipeline_numeric import get_mongo_collection


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

    return df


def data_transform(df):
    # 중복 및 필요 없는 컬럼 제거
    """
    - ID 컬럼 : MongoDB에서 임의로 생겨난 데이터. PostgreSQL에서 id값 생성하므로 Drop
    - MODEL_NM : 모든 데이터가 같은 모델명. 의미 없으므로 drop
    - REG_DITM : 데이터가 서울시 DB에 등록된 시각. 그쪽 로그이므로 drop
    """
    print('Transforming data...')
    df.drop(['_id', 'MODEL_NM', 'REG_DTTM'], axis=1, inplace=True)
    df.drop_duplicates(inplace=True)

    # 이상한 측정값(서울대공원) 제거
    df.drop(df[df['AUTONOMOUS_DISTRICT'] == 'Seoul_Grand_Park'].index, inplace=True)

    # 영어 동 이름 정제
    df['ADMINISTRATIVE_DISTRICT'] = df['ADMINISTRATIVE_DISTRICT'].str.replace(r'\d-', '-').str.replace(r'dong\d', 'dong')

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
    cur.execute("""CREATE TABLE IF NOT EXISTS population(
                    SENSING_TIME TIMESTAMP NOT null,
                    SERIAL_NO VARCHAR(255) NOT null,
                    AUTONOMOUS_DISTRICT VARCHAR(255),
                    ADMINISTRATIVE_DISTRICT VARCHAR(255),
                    REGION VARCHAR(255),
                    VISITOR_COUNT INTEGER NOT NULL,
                    PRIMARY KEY (SERIAL_NO, SENSING_TIME)
                    );""")

    # insert query
    query = f'INSERT INTO population({columns}) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING'  # 중복 데이터가 충돌할 경우 아무것도 안함

    # INSERT DATA
    try:
        # df를 insert
        print(f'Trying to load rows...')
        extras.execute_batch(cur=cur, sql=query, argslist=tuple(df.values))

        cur.close()
        connection.commit()
        print('Completed')

    except (Exception, psycopg2.Error) as e:
        raise

    finally:
        connection.close()
        print("Done!!")


def update_sql():  # 추후 개발
    """
    MongoDB에서 축적된 데이터를 1시간에 한 번 씩 업데이트 한다.
    파이썬 파일로 로그를 추가하여 그 이후 데이터에 대해서만 SQL에 적재한다.
    :return:
    """
    pass

# test

if __name__ == '__main__':
    # Extract
    df = get_mongo_data()

    # Transform
    df = data_transform(df)

    # Load
    load_on_sql(df)


