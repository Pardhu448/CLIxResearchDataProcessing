import pandas
path = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/'
mz_lessons_data = pandas.read_csv(path + '/mz_lessons_boxplot_data1.csv')

import plotly.graph_objects as go

import numpy as np
x_data = ['July-2018', 'Aug-2018',
          'Sep-2018', 'Oct-2018',
          'Nov-2018']

for each in ['e', 'm', 's']:
    y_data_domain = mz_lessons_data[mz_lessons_data['domain'] == each]

    y_jul = y_data_domain[y_data_domain['month_slot'] == 'Jul-2018']['lessons_visited']
    y_aug = y_data_domain[y_data_domain['month_slot'] == 'Aug-2018']['lessons_visited']
    y_sep = y_data_domain[y_data_domain['month_slot'] == 'Sep-2018']['lessons_visited']
    y_oct = y_data_domain[y_data_domain['month_slot'] == 'Oct-2018']['lessons_visited']
    y_nov = y_data_domain[y_data_domain['month_slot'] == 'Nov-2018']['lessons_visited']

    y_data = [y_jul, y_aug, y_sep, y_oct, y_nov]
    #y_data = mz_lessons_data['lessons_visited']

    colors = ['rgba(93, 164, 214, 0.5)', 'rgba(255, 144, 14, 0.5)', 'rgba(44, 160, 101, 0.5)',
          'rgba(255, 65, 54, 0.5)', 'rgba(207, 114, 255, 0.5)', 'rgba(127, 96, 0, 0.5)']

    fig = go.Figure()

    for xd, yd, cls in zip(x_data, y_data, colors):
        fig.add_trace(go.Box(
            y=yd,
            name=xd,
            boxpoints='all',
            jitter=0.5,
            whiskerwidth=0.2,
            fillcolor=cls,
            marker_size=2,
            line_width=1)
        )
    if each == 'e':
        title = 'Number of Lessons in English Completed by Students across Mizoram'
    elif each == 'm':
        title = 'Number of Lessons in Maths Completed by Students across Mizoram'
    else:
        title = 'Number of Lessons in Science Completed by Students across Mizoram'

    fig.update_layout(
        title= title,
        yaxis=dict(
        autorange=True,
        showgrid=True,
        zeroline=True,
        dtick=2,
        gridcolor='rgb(255, 255, 255)',
        gridwidth=1,
        zerolinecolor='rgb(255, 255, 255)',
        zerolinewidth=2,
     ),
     margin=dict(
        l=40,
        r=30,
        b=80,
        t=100,
     ),
     paper_bgcolor='rgb(243, 243, 243)',
     plot_bgcolor='rgb(243, 243, 243)',
     showlegend=False
    )
    import plotly.offline
    plotly.offline.plot(fig, filename='file_' + each + '.html')


