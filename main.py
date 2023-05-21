from flask import Flask, jsonify, request, render_template
import os
from sqlalchemy import create_engine, text
import pandas as pd
import requests
from datetime import datetime
import base64

import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import io

app = Flask(__name__)


@app.route('/')
def index():
    return jsonify({"Choo Choo": "Welcome to your Flask app 🚅"})

@app.route('/api/v0/GET/get_demand', methods=['GET'])
def get_demand():
    parameters = ["start_date", "end_date", "time_resolution", "plot_type", "orientation"]
    conditions = [x in request.args for x in parameters]
    if all(conditions):
        style = request.args["plot_type"]
        t_resolution = request.args["time_resolution"]
        orientation = request.args["orientation"]
        #convert string to pandas datetime
        try:
            start_date = pd.to_datetime(request.args["start_date"])
            end_date = pd.to_datetime(request.args["end_date"])
        except:
            return "Bad API request: date format not correct"
        if end_date<start_date:
            return "Bad API request: start_date must be before end_date"
        #I go to the electric grid api and take the data
        #I cannot collect them all together because there is a limit imposed by the api developers.
        #I will send my get query for blocks of 600 hours, to be sure it works fine.
        max_seconds = 600 * 60 * 60 # 25 days
        tot_seconds = (end_date-start_date).total_seconds()
        iterations = int((tot_seconds//max_seconds)+1)
        df = pd.DataFrame()
        tmp_start_date = start_date
        for _ in range(iterations):
            # (start_date + 25 days) or end_date if end_date happens before (start_date + 25 days)
            tmp_end_date = min(tmp_start_date + pd.Timedelta(seconds=max_seconds), end_date)
            #return [tmp_start_date, tmp_end_date, str(tmp_end_date-tmp_start_date)]

            response = requests.get(f'https://apidatos.ree.es/en/datos/demanda/evolucion?start_date={tmp_start_date}&end_date={tmp_end_date}&time_trunc=hour&geo_trunc=electric_system&geo_limit=peninsular&geo_ids=8741')
            tmp_start_date = tmp_end_date + pd.Timedelta(hours=1)
            data = response.json()["included"][0]["attributes"]["values"]
            df = pd.concat([df, pd.json_normalize(data)])
        df = df.reset_index(drop=True)
        #drop extra columns that I don't need
        df = df.drop(["percentage"], axis=1)
        #I end up with a dataframe with "datetime" and "value" columns
        #I connect to my database in railway
        engine = create_engine("postgresql://postgres:dH2GNTdVNNqv5iwDOfoA@containers-us-west-90.railway.app:5626/railway")
        #engine = create_engine("postgresql://postgres:KtT9gHOw6nVx6QZyRZP0@containers-us-west-189.railway.app:5627/railway")
        #I collect what I already saved in the database
        existing_data_df = pd.read_sql_query(text("""SELECT * FROM electric_grid"""), con = engine.connect())
        #I delete from new_entries_df all lines which are already saved in my database 
        new_entries_df = df[~df["datetime"].isin(existing_data_df["datetime"])]
        #I append the new entries in the remote database
        #con = engine is the connection with the railway database
        new_entries_df.to_sql("electric_grid", con=engine, if_exists='append', index=False)

        #PLOT
        styles = ["bar", "line"]
        orientations = ["horizontal", "vertical", "h", "v"]
        t_resolutions = ["hour", "day", "month", "year"]

        if style not in styles:
            return "Bad API request: plot_type must be either 'bar' or 'line'"
        if orientation not in orientations:
            return "Bad API request: orientation must be either 'horizontal' or 'h' or 'vertical' or 'v'"
        if t_resolution not in t_resolutions:
            return "Bad API request: time_resolution must be 'hour', 'day', 'month', or 'year'"
        
        #This part of the code is used to aggregate correctly the dataframe depending on t_resolution
        #it looks a bit crazy, probably there was a better way to do it!
        df["datetime"] = pd.to_datetime(df['datetime'], errors='coerce')
        df["month"] = [x.month for x in df["datetime"]]
        df["year"] = [x.year for x in df["datetime"]]
        df["day"] = [x.day for x in df["datetime"]]
        years = sorted(df["year"].unique())
        months = sorted(df["month"].unique())
        days = sorted(df["day"].unique())
        if t_resolution!="hour":
            dictio = {"year" : [], "month" : [], "day" : [], "value": []}
            for year in years:
                df_tmp1 = df[df["year"]==year]
                if t_resolution!="year":
                    for month in months:
                        df_tmp2 = df_tmp1[df_tmp1["month"]==month]
                        if t_resolution =="day":
                            for day in days:
                                df_tmp3 = df_tmp2[df_tmp2["day"]==day]
                                if len(df_tmp3)>0:
                                    value = df_tmp3.groupby("day")["value"].mean().iloc[0]
                                    dictio["year"].append(year)
                                    dictio["month"].append(month)
                                    dictio["day"].append(day)
                                    dictio["value"].append(value)
                        else:
                            if len(df_tmp2)>0:
                                value = df_tmp2.groupby("month")["value"].mean().iloc[0]
                                dictio["year"].append(year)
                                dictio["month"].append(month)
                                dictio["value"].append(value)
                else:
                    if len(df_tmp1)>0:
                        value = df_tmp1.groupby("year")["value"].mean().iloc[0]
                        dictio["year"].append(year)
                        dictio["value"].append(value)
            if t_resolution=="month":
                dictio.pop("day")
            elif t_resolution=="year":
                dictio.pop("month")
                dictio.pop("day")
            plot_df = pd.DataFrame(dictio)
            if t_resolution=="year":
                plot_df["datetime"] = plot_df["year"].astype(str)
            elif t_resolution=="month":
                plot_df["datetime"] =plot_df["year"].astype(str) + "-" + plot_df["month"].astype(str)
            elif t_resolution=="day":
                plot_df["datetime"] = (plot_df["year"].astype(str) + "-" + 
                                    plot_df["month"].astype(str) + "-" + plot_df["day"].astype(str))
        elif t_resolution=="hour":
            plot_df = df
            
        # Create the plot
        fig = Figure(figsize=(9,7))
        plt.figure(figsize=(9, 7))
        axis = fig.add_subplot(1,1,1)
        if style=="line":
            axis.plot(plot_df["datetime"], plot_df["value"])
        else:
            axis.bar(plot_df["datetime"], plot_df["value"])

        axis.plot(plot_df["datetime"], plot_df["value"])
        axis.set_xlabel(f"{t_resolution}s")
        axis.set_ylabel("Electric demand")
        axis.set_title(f"Electric demand from {start_date} to {end_date} in {t_resolution}")
        x = plot_df["datetime"]
        xlabels = plot_df["datetime"]
        axis.set_xticks(x)
        axis.set_xticklabels(xlabels, rotation=60)

        # Convert the plot to a PNG image
        canvas = FigureCanvas(fig)
        png_output = io.BytesIO()
        canvas.print_png(png_output)
        png_output.seek(0)  # Move the cursor to the start of the stream
        plot_data = base64.b64encode(png_output.getvalue()).decode('utf-8')
    else:
        return "Bad API request, please use parameters start_date, end_date, time_resolution, plot_type, orientation"
    
    return render_template('plot.html', plot_url=plot_data)

@app.route('/api/v0/GET/get_db_data', methods=['GET'])
def get_db_data():
    #connect to database (change it if you want to use other database)
    engine = create_engine("postgresql://postgres:dH2GNTdVNNqv5iwDOfoA@containers-us-west-90.railway.app:5626/railway")
    if "start_date" in request.args:
        start_date = pd.to_datetime(request.args["start_date"]).tz_localize("Europe/Madrid")
    else:
        start_date = pd.to_datetime("1700-01-01").tz_localize("Europe/Madrid")
    if "end_date" in request.args:
        end_date = pd.to_datetime(request.args["end_date"]).tz_localize("Europe/Madrid")
    else:
        end_date = pd.to_datetime("2100-01-01").tz_localize("Europe/Madrid")
    #I would like to do it in sql directly, but it was not possible to save the column in railway 
    #as datetime, so I had to save them as strings and now I cannot make comparisons with dates 
    df = pd.read_sql_query(text("""SELECT * FROM electric_grid"""), con = engine.connect())
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df[(df["datetime"]>start_date) & (df["datetime"]<end_date)]
    df = df.sort_values(by="datetime").reset_index()
    return jsonify(df.to_dict())

@app.route('/api/v0/DELETE/wipe_data', methods=['POST'])
def wipe_data():
    if "secret" in request.args:
        if request.args["secret"] == "boludez":
            engine = create_engine("postgresql://postgres:dH2GNTdVNNqv5iwDOfoA@containers-us-west-90.railway.app:5626/railway")
            #engine = create_engine("postgresql://postgres:KtT9gHOw6nVx6QZyRZP0@containers-us-west-189.railway.app:5627/railway")
            with engine.begin() as connection:
                connection.execute(text("""TRUNCATE TABLE electric_grid RESTART IDENTITY"""))
                #connection.execute(f"REINDEX TABLE electric_grid")
        else:
            return "not correct secret"
    else:
        return "You must pass a secret parameter"
    connection.close()
    return "All data wiped"
if __name__ == '__main__':
    app.run(debug=True, port=os.getenv("PORT", default=5000))



