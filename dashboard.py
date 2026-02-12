import dash
from dash import html, dcc
import pandas as pd
from flask_login import current_user
from models import db
import plotly.express as px

def init_dashboard(server):
    app = dash.Dash(
        server=server,
        url_base_pathname="/dash/",
        suppress_callback_exceptions=True
    )

    # Load data once at startup
    query = """SELECT department, COUNT(*) as employees FROM employees GROUP BY department"""
    try:
        result = db.session.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    except:
        df = pd.DataFrame({'department': ['A', 'B', 'C', 'D'], 'employees': [0, 0, 0, 0]})

    fig = px.bar(df, x="department", y="employees", title="Employees per Department")

    # Define layout as a function so current_user is accessed during a request
    def serve_layout():
        role = current_user.role if current_user.is_authenticated else "viewer"
        username = current_user.username if current_user.is_authenticated else "Guest"

        return html.Div([
            html.H1("ZimStats Dashboard"),
            dcc.Graph(figure=fig),
            html.H3(f"Logged in as: {username} ({role})"),

            html.Div([
                html.H2("Admin/Editor Tools"),
                html.Button("Upload Data"),
                html.Button("Modify Department"),
            ], style={"display": "none" if role == "viewer" else "block"})
        ])

    app.layout = serve_layout

    return app
