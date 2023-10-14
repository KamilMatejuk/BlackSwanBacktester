import connexion
from flask import jsonify
from decouple import Config, RepositoryEnv
from flask_socketio import SocketIO

from schemas_request import StartRequest #, validate


def post_greeting(name: str):
    return f'Hello {name}', 200


def validate(cls):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                data = cls(**connexion.request.json)
                data.validate()
                return func(data)
            except Exception as ex:
                return jsonify({"error": "Invalid request schema", "details": str(ex)}), 401
        return wrapper
    return decorator


'''
{
  "asset": "BTCUSDT",
  "end_time": 15,
  "model_url": {
    "host": "localhost",
    "port": 4001,
    "slug": "action"
  },
  "signals": [
    {
      "name": "price",
      "url": {
        "host": "localhost",
        "port": 4002,
        "slug": "price/latest/BTCUSDT"
      }
    }
  ],
  "start_time": 10,
  "timeframe": "1d"
}
'''
@validate(StartRequest)
def start_backtest(data):
    # get all signals for all timeframes
    # combine signals
    # for each:
    #   pass to model and get action
    #   simulate result
    #   save progress
    # save results
    return {}, 200

# create demo model (just random action)
# once all this is working, add RL to model


config = Config(RepositoryEnv('.env.local'))
app = connexion.FlaskApp(__name__,
        server='tornado',
        specification_dir='',
        options={'swagger_url': '/swagger-ui'})
app.add_api('openapi.yaml')
socketio = SocketIO(app.app)
socketio.run(app.app, port=config.get('PORT'))
