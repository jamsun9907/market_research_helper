from flask import Flask, render_template

app = Flask(__name__)  # __main__

@app.route('/')
def index():
    # 기본 메인 대시보드 페이지

    return render_template('main.html')


