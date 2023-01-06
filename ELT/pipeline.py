# 설계 : 하루에 한 번 이 api에서 전일 유동인구 정보를 자동으로 가져와 DB에 적재하도록 한다.
import requests
import json
import psycopg2
import config


# Config
KEY = '474f527a726a616d363373734e6375'  # 추후 가리기

# Extract
def get_data(page):
    """
    API로부터 서울시 유동인구 데이터 row를 불러온다.
    1회 최대 요청건수는 1000회이므로 page 파라미터 1 당 1000개 row를 호출한다.

    :param page:
    호출하는 api row 인덱스 범위 (page ~ page+999)

    :return:
    field가 딕셔너리로 구성된 row를 리스트 형식으로 반환
    """
    START_INDEX = (page+1) * 1000  # 데이터 행 시작 번호
    END_INDEX = START_INDEX + 999  # 데이터 행 끝 번호

    API = f'http://openapi.seoul.go.kr:8088/{config.KEY}/json/IotVdata018/{START_INDEX}/{END_INDEX}/'


    # Parsing
    response = requests.get(API)
    status = response.status_code  # 400
    text = json.loads(response.text)

    if status == 200:
        try:
            rows = text['IotVdata018']['row']
            print('Records : ', len(rows))
            return rows  # 구하고자 하는 rows
        except:
            print(response.text)
            print('디버깅을 시작해야겠군')


    else:
        print(f'Request Error : {status}')  # 예외처리 추가


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



def load_data(row, connection):
    """
    raw 데이터를 PostgreSQL에 적재한다.

    :return:
    None
    """
    cur = connection.cursor()
    columns = 'MODEL_NM, SERIAL_NO, SENSING_TIME, REGION, AUTONOMOUS_DISTRICT, ADMINISTRATIVE_DISTRICT, VISITOR_COUNT, REG_DTTM'
    query = f'INSERT INTO population({columns}) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'

    # INSERT DATA
    cur.execute(query, tuple(row.values()))

    return None

# test
rows = get_data(page=3)

connection = get_connection()
cur = connection.cursor()

# INSERT DATA
for row in rows:
    try:
        load_data(row, connection)
    except:
        print("pass inserting a data that has incorrect datatype")

print(f'{len(rows)} records loaded ')
connection.commit()
