from flask import Flask, render_template, request, g
from Seoul_dash.ETL.Keyword_data import KeywordSearcher
import time

app = Flask(__name__)  # __main__

# 메인 화면 1
# 기본 메인 대시보드 페이지 : 태블로로 구현한 맵이 보여야 함
@app.route('/', methods=['GET'])
def index():
    return render_template('main.html')

# 메인 화면 2
# Keyword를 검색하는 페이지 : 검색 결과는 Plotly 그래프로 시각화
@app.route('/search', methods=['GET', 'POST'])
def search():
    # 기본 검색 화면
    if request.method == 'GET':
        return render_template('search.html')

    # 유저가 검색을 한 경우
    elif request.method == 'POST':
        start_time = time.time()  # 시간 체크

        # 0. 유저가 search에 입력한 데이터를 받아옴
        keyword = request.form['user_input']
        print('User search input :', keyword)

        # 1. 이 키워드를 함수에 넣어 토큰화
        key = KeywordSearcher(keyword)
        title, description = key.parsing_json()

        # 2. count한다.
        token_t = key.get_tags(title)  # 제목
        token_d = key.get_tags(description)  # 본문 요약 토큰 - count dict {keyword : cnt} 형태

        # 3. 그 결과를 Plot
        graph_t = key.get_graph(token_d)
        graph_d = key.get_graph(token_t)

        print(f'Execution time : {time.time() - start_time:.2f}s')  # 시간 체크

        return render_template('search_results.html', Keyword=keyword, graph_title=graph_t, graph_description=graph_d)

    # 예외처리 : GET, POST 외의 요청일 경우
    else:
        return '<h1>Oops! 뭔가가 잘못되었습니다!</h1>'
