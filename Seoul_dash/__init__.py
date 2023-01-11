from flask import Flask, render_template, request
from Seoul_dash.ETL.Keyword_data import KeywordSearcher

app = Flask(__name__)  # __main__


# 기본 메인 대시보드 페이지 : 태블로로 구현한 맵이 보여야 함
@app.route('/', methods=['GET'])
def index():
    return render_template('main.html')

# Keyword를 검색하는 페이지
# @app.route('/search', methods=['GET','POST'] )
# def search():
#     # user의 키워드 값 인풋을 받는다.
#     return render_template('search.html')

# @app.route('/dash', methods=['GET','POST'] )
# def dash():
#     # user의 키워드 값 인풋을 받는다.
#     if request.method == 'POST':
#         keyword = request.form
#         print(keyword)
#
#         return test()
#
#     elif request.method == 'GET':
#         return render_template('sub_back.html')

# 결과를 받는 페이지
@app.route('/search', methods=['GET','POST'])
def search():
    # 유저가 인풋 값을 주는 경우
    if request.method == 'POST':
        # 0. 유저가 search에 입력한 데이터를 받아옴
        keyword = request.form['user_input']
        print('User search input :', keyword)

        # 1. 이 키워드를 함수에 넣어 토큰화
        key = KeywordSearcher(keyword)
        title, description = key.parsing_json()

        # 데이터를 토큰화하여 count한다.
        token_t = key.get_tags(title)  # 제목
        token_d = key.get_tags(description)  # 본문 요약 토큰 - count dict {keyword : cnt} 형태

        # 2. 그 결과를 플로함
        graph_description = key.get_graph(token_t)
        graph_title = key.get_graph(token_d)

        return render_template('search_results.html',Keyword=keyword, graph_title=graph_title, graph_description=graph_description)

    elif request.method == 'GET':
        return render_template('search.html')

    else:
        return '<h1>Oops! 뭔가가 잘못되었습니다!</h1>'
