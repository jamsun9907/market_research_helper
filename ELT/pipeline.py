# 설계 : 하루에 한 번 이 api에서 전일 유동인구 정보를 자동으로 가져와 DB에 적재하도록 한다.
import requests
import json
from pymongo import MongoClient
import time
import config


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
            return rows  # 구하고자 하는 rows

        except:
            print(response.text)
            print('디버깅을 시작해야겠군')


    else:
        print(f'Request Error : {status}')  # 예외처리 추가

# MongoDB
def get_mongo_collection():
    HOST = config.HOST
    USER = config.USER
    PASSWORD = config.PASSWORD
    DATABASE_NAME = 'CP1_DB'
    COLLECTION_NAME = 'population'
    MONGO_URI = f"mongodb+srv://{USER}:{PASSWORD}@{HOST}/{DATABASE_NAME}?retryWrites=true&w=majority"

    # 커넥션 접속 작업
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]  # Connection
    collection = db[COLLECTION_NAME]  # Creating table

    return collection


def load_on_mongoDB(rows):
    """
    rows를 mongoDB에 일단 저장
    1000개를 배치 단위로 저장
    """
    collection = get_mongo_collection()
    collection.insert_many(documents=rows)

    return None

# 실행
if __name__ == '__main__':
    start = time.time()

    for page in range(94,200):
        # Api에서 데이터를 호출
        print(f'--------------------\nProcessing {page} page...')

        try:
            rows = get_data(page=page)
            load_on_mongoDB(rows)
            print(f'        Log : {len(rows)} records loaded')
        except Exception as e:
            print('Error occured :\n', e)

        finally:
            print(f'        Executed time : {time.time()-start:.2f} sec')
    print('Total running time : ', f'{time.time()-start:.2f} sec')
