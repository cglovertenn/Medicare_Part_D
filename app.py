# Import Dependencies
from flask import Flask, jsonify, render_template, send_from_directory
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

engine = create_engine("sqlite:///part_d_db.sqlite")
Base = automap_base()
Base.prepare(engine, reflect=True)

Drug = Base.classes.drug_db
State = Base.classes.state_db
session = Session(engine)

app = Flask(__name__, static_folder='static')

@app.route('/vendor/<path:path>')
def load_vendor(path):
    return send_from_directory('static/vendor', path)

@app.route('/js/<path:path>')
def load_js(path):
    return send_from_directory('static/js', path)

@app.route('/css/<path:path>')
def load_css(path):
    return send_from_directory('static/css', path)

@app.route('/img/<path:path>')
def load_img(path):
    return send_from_directory('static/img', path)

@app.route('/')
def home():
    return app.send_static_file('index.html')

@app.route('/drug')
def drug():

    return jsonify('/Drug.html')

@app.route('/state')
def state():

    return jsonify(State.html)

@app.route('/api/v1.0/statedatabase')
def state_api():
    state_all = session.query(State.nppes_provider_state,\
                                State.drug_name,\
                                State.generic_name,\
                                State.bene_count,\
                                State.total_claim_count,\
                                State.total_day_supply,\
                                State.total_drug_cost,\
                                State.total_30_day_fill_count).all()
    state_list = list(state_all)
    return jsonify(state_list)

@app.route('/api/v1.0/drugdatabase')
def drug_api():
    drug_all = session.query(Drug.nppes_provider_state,\
                                Drug.drug_name,\
                                Drug.generic_name,\
                                Drug.bene_count,\
                                Drug.total_claim_count,\
                                Drug.total_day_supply,\
                                Drug.total_drug_cost,\
                                Drug.total_30_day_fill_count).all()
    drug_list = list(drug_all)
    return jsonify(drug_list)

app.run()
