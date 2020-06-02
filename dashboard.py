import dash
import dash_table
import pandas as pd
import dash_html_components as html
import plotly.graph_objects as go
from datetime import datetime as dt
import dash_core_components as dcc
import re
import psycopg2
import json
with open("redshift.json") as f:
    redshift = json.load(f)

conn=psycopg2.connect(f"dbname = {redshift['dbname']} host={redshift['host']}, port= {redshift['port']}, user={redshift['user']} password={redshift['password']}") 
conn.autocommit = True
cur = conn.cursor()
cur.execute("select min(time_key) as min_date, max(time_key) as max_date from fact_loans")

dates_df = pd.DataFrame.from_records(cur.fetchall())
date_min, date_max = dates_df.loc[0,0], dates_df.loc[0,1]

cur.execute("""
with time_range as (
  select * 
  from fact_loans 
  where time_key <= '{}'::date 
      and time_key>='{}'::date)
  select time_key as date, count(*) as count 
  from time_range
  group by time_key
""".format(date_max, date_min))
df = pd.DataFrame.from_records(cur.fetchall())
df.columns=['date','count']
df['date']=pd.to_datetime(df['date'])
df.set_index('date', inplace=True)
idx = pd.date_range(date_min.strftime('%m-%d-%Y'), date_max.strftime('%m-%d-%Y'))
df = df.reindex(idx, fill_value=0).reset_index()
df.columns = ['date','count']
top_columns=['Title', 'loan_numbers']
app = dash.Dash(__name__)
app.layout = html.Div([html.H1(children='Loans Overview'),\
                        html.Div(id='output_date'),
                        dcc.DatePickerRange(
                            id='date-picker',
                            min_date_allowed=date_min,
                            max_date_allowed=date_max,
                            initial_visible_month=date_min,
                            start_date =date_min,
                            end_date=date_max
                        ),\
                        dcc.Graph(id='loans_day'),
                        dcc.Graph(id='subjects'),
                        dash_table.DataTable(
                            style_cell={
                                'whiteSpace': 'normal',
                                'height': 'auto',
                            },
                            id='data_table', 
                            columns=[{"name": i, "id": i} for i in top_columns])
                        ])


@app.callback(
    [dash.dependencies.Output('output_date', 'children'),
    dash.dependencies.Output('loans_day', 'figure'),
    dash.dependencies.Output('subjects', 'figure'),
    dash.dependencies.Output('data_table', 'data')],
    [dash.dependencies.Input('date-picker', 'start_date'),
     dash.dependencies.Input('date-picker', 'end_date')])
def update_output(start_date, end_date):
    string_prefix = 'You have selected: '
    if start_date is not None:
        start_date2 = dt.strptime(re.split('T| ', start_date)[0], '%Y-%m-%d')
        start_date_string = start_date2.strftime('%B %d, %Y')
        string_prefix = string_prefix + 'Start Date: ' + start_date_string + ' | '
    if end_date is not None:
        end_date2 = dt.strptime(re.split('T| ', end_date)[0], '%Y-%m-%d')
        end_date_string = end_date2.strftime('%B %d, %Y')
        string_prefix = string_prefix + 'End Date: ' + end_date_string
    if len(string_prefix) == len('You have selected: '):
        string_prefix = 'Select a date to see it displayed here'
    df_temp = df[(df['date']>=start_date)&(df['date']<=end_date)]
    fig_temp = go.Figure(data=go.Scatter(x=df_temp['date'].tolist(), y=df_temp['count'].tolist(), mode='lines'))
    fig_temp.update_layout(
        title="Loans Per Day",
        xaxis_title="Date",
        yaxis_title="# of Loans")
    global cur

    cur.execute("""
        with filtered_subject as (
        select fl.bibnumber, ds.subject as subject 
        from fact_loans fl 
            left outer join bridge_subject bs
                on fl.subject_group_id=bs.subject_group_id 
            left outer join dim_subject ds 
                on bs.subject_id = ds.subject_id
        where fl.time_key>='{}'::date 
            and fl.time_key <='{}'::date
        )
        select subject, count(*) as count
        from filtered_subject
        group by subject
        order by count desc 
        limit 30
    """.format(start_date, end_date))

    subcount = pd.DataFrame.from_records(cur.fetchall())
    subcount.columns=['subject','count']    
    
    fig_subject = go.Figure()
    fig_subject.add_trace(go.Bar(
            x=subcount['subject'],
            y=subcount['count'],
            name='Popular Subjects',    
        ))
    fig_subject.update_layout(
        title="Popular Subjects",
        xaxis_title="Subject",
        yaxis_title="# of Loans with Subject",
    )

    cur.execute("""
        select db.title as Title, count(*) as loan_numbers
        from fact_loans fl left outer join dim_books db
            on fl.bibnumber = db.bibnum
        where fl.time_key>='{}'::date and fl.time_key<='{}'::date
        group by db.title
        order by loan_numbers desc
        limit 20
    """.format(start_date, end_date))
    top = pd.DataFrame.from_records(cur.fetchall())
    top.columns = ['Title','loan_numbers']
    data_records = top.to_dict('records')
    return string_prefix, fig_temp, fig_subject, data_records


if __name__ == '__main__':
    app.run_server(debug=True)