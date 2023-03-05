from flask import Blueprint

bp = Blueprint("main", __name__, url_prefix='/')

# Flask 애플리케이션 루트
@bp.route('/hello')
def hello_pybo():
    return 'Hello, Pybo!'


@bp.route('/')
def index():
    return 'Pybo, index!'
