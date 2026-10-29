"""Presentation helpers; no networking or mutations in plotting code."""
import numpy as np
import pandas as pd
import plotly.graph_objects as go


def timeseries(ds, *, days=90):
    start = pd.Timestamp(ds.time.values[-1]) - pd.Timedelta(days=days)
    part = ds.sel(time=slice(start, None))
    fig = go.Figure(go.Scattergl(x=part.time.values, y=part.kp.values, name="NOAA Kp", mode="lines"))
    fig.add_hline(y=5, line_dash="dash", annotation_text="Local storm threshold Kp >= 5")
    fig.update_layout(title="NOAA estimated planetary Kp", xaxis_title="UTC interval start",
                      yaxis_title="Kp", yaxis_range=[0, 9.3], height=350)
    return fig


def heatmap(ds, *, days=90):
    frame = ds.kp.to_series().tail(days*8).rename("kp").reset_index()
    frame["day"] = frame.time.dt.date.astype(str)
    frame["hour"] = frame.time.dt.hour
    grid = frame.pivot(index="day", columns="hour", values="kp").reindex(columns=range(0, 24, 3))
    fig = go.Figure(go.Heatmap(z=grid.values, x=grid.columns, y=grid.index, zmin=0, zmax=9,
                              colorbar={"title": "Kp"}))
    fig.update_layout(title="Three-hour activity by day (blank = missing)", height=400,
                      xaxis_title="UTC interval start hour", yaxis_title="Date")
    return fig


def conceptual_field(kp):
    """Illustration only: Kp does NOT determine magnetopause size or field lines."""
    amplitude = float(kp) / 9
    theta = np.linspace(0.12, np.pi-0.12, 220)
    fig = go.Figure()
    for radius in (1.8, 2.7, 3.6, 4.5):
        r = radius * np.sin(theta)**2
        for side in (-1, 1):
            x = side * r * np.sin(theta)
            z = r * np.cos(theta)
            # An explicitly artistic deformation, not a physical inference from Kp.
            x = x * (1 + amplitude * (0.25 if side < 0 else -0.12))
            z += amplitude * 0.1 * np.sin(7 * theta)
            keep = x*x + z*z >= 1
            fig.add_trace(go.Scatter(x=np.where(keep, x, np.nan), y=np.where(keep, z, np.nan),
                                     mode="lines", showlegend=False, hoverinfo="skip"))
    earth = np.linspace(0, 2*np.pi, 150)
    fig.add_trace(go.Scatter(x=np.cos(earth), y=np.sin(earth), mode="lines", fill="toself", name="Earth"))
    fig.update_layout(title=f"Conceptual disturbance illustration · Kp {kp:.2f} · NOT a field simulation",
                      height=440, xaxis={"range": [-6, 6]}, yaxis={"range": [-4, 4], "scaleanchor": "x"})
    return fig
