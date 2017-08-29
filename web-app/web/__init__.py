"""TODO: module doc..."""

from flask import Blueprint
from flask_restful import Api

# :pylint: disable=invalid-name

#app = Flask(__name__)

API_VERSION = '1'

api_blueprint = Blueprint(
    'api',
    __name__,
    template_folder='templates',
    static_folder='static',
    url_prefix='/api/{0}'.format(API_VERSION)
)

api = Api(api_blueprint)

import views  # noqa
