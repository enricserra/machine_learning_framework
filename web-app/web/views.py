from . import api

from flask.ext.restful import (
    Resource,
    abort,
    reqparse,
    inputs,
)

class SampleListResource(Resource):
    """
    Create a new sample
    """

    # :pylint: disable=no-init, too-few-public-methods, no-self-use

    def get(self):
        """
        TODO function doc
        :return:
        """
        parser = reqparse.RequestParser()
        parser.add_argument('component', location='args', choices=Component)
        parser.parse_args()
        return "ASTRINGSampleSchema(many=True).dump(samples).data"


api.add_resource(SampleListResource, '/sample/')
