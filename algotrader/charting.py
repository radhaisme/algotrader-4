from databases.influx_manager import influx_client
from math import pi
from bokeh.plotting import figure, show, output_file
import pandas as pd
import plotly as py
import plotly.graph_objs as go


def get_data():

    cql = 'SELECT * FROM fx_1min WHERE '
    db_client = influx_client(client_type='dataframe')
    data = db_client.query(cql)['fx_1min']

    return data


def plot_bokeh(data):
    data["date"] = pd.to_datetime(data.index)
    inc = data.close > data.open
    dec = data.open > data.close
    w = 24 * 60 * 60 * 1000  # one day in ms

    TOOLS = "pan,wheel_zoom,box_zoom,reset,save"

    p = figure(x_axis_type="datetime", tools=TOOLS, plot_width=1200, title="fx 1min")
    p.xaxis.major_label_orientation = pi / 4
    p.grid.grid_line_alpha = 0.3

    p.segment(data.date, data.high, data.date, data.low, color="black")
    p.vbar(data.date[inc], w, data.open[inc], data.close[inc], fill_color="#D5E1DD", line_color="black")
    p.vbar(data.date[dec], w, data.open[dec], data.close[dec], fill_color="#F2583E", line_color="black")

    output_file("fx 1min.html", title="fx 1min")

    show(p)  # open a browser


def plot_plotly(data):
    py.offline.init_notebook_mode(connected=True)
    trace = go.Candlestick(x=data.index,
                           open=data.open,
                           high=data.high,
                           low=data.low,
                           close=data.close)

    data = [trace]
    py.offline.iplot(data, filename='simple_candlestick')

if __name__ == '__main__':

    d = get_data()
    # plot_bokeh(d)
    plot_plotly(d)
