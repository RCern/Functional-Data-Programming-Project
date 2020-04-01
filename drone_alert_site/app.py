from flask import Flask, render_template, request, session, redirect, url_for
import requests
from threading import Event
import signal
from flask_kafka import FlaskKafka

import os 


app = Flask(__name__)
app.secret_key = os.urandom(24)

alerts = list()
active_alerts = 0
solved_alerts = 0
total_alerts = active_alerts + solved_alerts
INTERRUPT_EVENT = Event()

bus = FlaskKafka(INTERRUPT_EVENT,
                 bootstrap_servers=",".join(["192.168.1.18:9092"]),
                 group_id="consumer-grp-id"
                 )


def listen_kill_server():
    signal.signal(signal.SIGTERM, bus.interrupted_process)
    signal.signal(signal.SIGINT, bus.interrupted_process)
   


@bus.handle('sendToFlask')
def test_topic_handler(msg):
    print(msg)
    alert = str(msg.value)
    alert = alert[2:-1].split(",")
    alerts.append(alert)
    global active_alerts 
    active_alerts += 1


@app.route('/')
def index():
    return render_template('login.html')
@app.route('/signup', methods=['GET', 'POST'])
def login_page():
    if request.method == 'POST':
        email = request.values.get('InputEmail')
        password = request.values.get('InputPassword')
        
        if(email == 'admin' and password == 'admin'):
                return redirect(url_for('dashboard'))
        else:
            return render_template('login.html')

    return render_template('login.html')



@app.route('/dashboard')
def dashboard():
    global active_alerts
    print(alerts)
    return render_template('index.html',alerts=alerts,active_alerts=active_alerts,solved_alerts=solved_alerts,total_alerts=total_alerts)


if __name__ == '__main__':
    bus.run()
    listen_kill_server()
    app.run()
