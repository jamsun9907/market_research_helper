# 데이터 베이스를 안쓰고 결과를 반환, flask에 보여줄 것이므로 구조적 프로그래밍을 진행

import requests
import json
import re
import Seoul_dash.ETL.config as c
import time
from konlpy.tag import Okt
from collections import Counter
import plotly
import plotly.express as px
import pandas as pd


class KeywordSearcher:
    """
    Attribute
    - self.keyword : 검색 키워드
    - self.raw_json : API 호출 원 데이터
    - self.data : 파싱된 json 데이터
    - self.tokens : 토큰화된 텍스트 데이터들 {token : cnt} 형태
    """

    def __init__(self, keyword):
        """
        API에서 사용자가 검색한 데이터를 호출하여 json형태로 반환한다.
        100건의 검색결과를 반환한다.
        :return:
        """
        # config
        self.keyword = keyword  # 필요할 경우 예외처리 추가
        display = 100  # 한 번에 표시할 검색 결과 개수 (네이버 기본값: 10, 최댓값: 100)

        # API 데이터 추출
        API = f'https://openapi.naver.com/v1/search/blog.json?query={self.keyword}&display={display}'

        self.raw_json = requests.get(API,
                                params={'query': self.keyword, 'display': display},
                                headers={'X-Naver-Client-Id': c.Client_ID, 'X-Naver-Client-Secret': c.Client_Secret}
                                )

    def parsing_json(self):  # 위의 함수와 나누는게 의미가 있을까?
        status = self.raw_json.status_code
        if status == 200:
            contents = json.loads(self.raw_json.text)

        # Parsing
        self.data = contents['items']

        # Contents to tokenize
        titles = [idx['title'] for idx in self.data]
        descriptions = [idx['description'] for idx in self.data]

        return titles, descriptions

    def _preprocessing(self, sentence):
        # <b> 태그 제거 : 그런데 이런식으로 제거하는게 좀 비효율적으로 보인다. 좋은 방법이 없을까?
        sentence = sentence.replace('<b>', '').replace('</b>', '').replace('&apos;', '')

        # 문자만 추출
        pat = re.compile('[^\w ]')
        sent = re.sub(pat, '', sentence)
        return sent

    def get_tags(self, sequences):
        """
        제목들과 글 요약을 각각 토큰화하여 count 객체로 반환한다.

        KoNLP 라이브러리 : pip install konlpy
        :return:
        """
        # timer
        time_start = time.time()

        # 전처리
        sequences_preprocessed = ''.join([self._preprocessing(sent) for sent in sequences])
        # print(sequences_preprocessed)

        # 토큰화 : 시간이 남으면 pos stemming한 것 시간 비교해보기
        okt = Okt()
        keywords = okt.nouns(sequences_preprocessed)

        # 카운트 : 각각의 토큰의 개수를 센다.
        token_count = Counter(keywords)
        print(f'{len(sequences_preprocessed)}개의 시퀀스 토큰화 소요시간 : ', (time.time() - time_start))

        return token_count

    def get_graph(self, token_count):
        """
        keyword 그래프를 반환한다.
        :return: json graph
        """
        # UPDATE : 토큰 중 가장 언급이 많이 된 키워드 30개만 가져온다. : 이거 plotly에서 조정 가능하게 변경한다.
        # 카운터로 데이터 프래임 만들기
        df = pd.DataFrame.from_dict(token_count, orient='index').reset_index()
        df.rename(columns={'index': '키워드', 0: '횟수'}, inplace=True)

        # 상위 20개 데이터만 가져오기
        num_lookup = 20
        df = df.sort_values(by='횟수' ,ascending=False).head(num_lookup)

        # 그래프를 그린다
        fig = px.bar(df, x='키워드', y='횟수', text='횟수')  # text : 상단에 표시할 때 어떤 축 할지
        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        return graphJSON


# Test
if __name__ == '__main__':
    keyword = '응암동 맛집'
    key = KeywordSearcher(keyword)
    title, description = key.parsing_json()

    token_count_desciption = key.get_tags(description)

    token_count_title = key.get_tags(title)

    graph = key.get_graph(token_count_desciption)

