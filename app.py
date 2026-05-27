#!/usr/bin/python
# -*- coding: utf-8 -*-

from flask import Flask
from flask_cors import CORS
from logging import Formatter
from logging.handlers import TimedRotatingFileHandler
import os

# import blueprints
from routes.sys_routes import bp as sys_bp
#!/usr/bin/python
# -*- coding: utf-8 -*-

from flask import Flask
from flask_cors import CORS
from logging import Formatter
from logging.handlers import TimedRotatingFileHandler
import os

# import blueprints
from routes.sys_routes import bp as sys_bp
from routes.task_routes import bp as task_bp
from routes.person_routes import bp as person_bp
from routes.project_routes import bp as project_bp
from routes.transaction_routes import bp as transaction_bp
from routes.schedule_routes import bp as schedule_bp
from routes.strava_routes import bp as strava_bp
from routes.activity_routes import bp as activity_bp

app = Flask(__name__)
CORS(app, resources=r'/*', supports_credentials=True)

# ensure logs directory exists
os.makedirs('logs', exist_ok=True)

# 日志配置
LOG_FORMAT = '%(asctime)s %(levelname)s: %(message)s'
LOG_FILE = 'logs/app.log'
LOG_LEVEL = 'INFO'

rolling_handler = TimedRotatingFileHandler(
    LOG_FILE, when='midnight', interval=1)
rolling_handler.setLevel(LOG_LEVEL)
rolling_handler.setFormatter(Formatter(LOG_FORMAT))

# register blueprints
app.register_blueprint(sys_bp)
app.register_blueprint(task_bp)
app.register_blueprint(person_bp)
app.register_blueprint(project_bp)
app.register_blueprint(transaction_bp)
app.register_blueprint(schedule_bp)
app.register_blueprint(strava_bp)
app.register_blueprint(activity_bp)


if __name__ == '__main__':
    app.logger.addHandler(rolling_handler)
    app.run(host='0.0.0.0', port=5000, debug=True)
