from flask import Flask
from routes import bp_clips

app = Flask(__name__)

app.register_blueprint(bp_clips)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=False)