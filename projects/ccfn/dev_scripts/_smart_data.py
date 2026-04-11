"""
CCFN custom tasks for processing SMART Connect data.
"""
from typing import Annotated, cast

import pandas as pd
from pydantic import Field
from wt_registry import register

from ecoscope.platform.annotations import AnyGeoDataFrame


@register(tags=["ccfn"])
def add_count_column(
    geodataframe: Annotated[
        AnyGeoDataFrame,
        Field(description="Input GeoDataFrame to add a count column to.", exclude=True),
    ],
    column_name: Annotated[
        str,
        Field(description="Name for the count column.", default="count"),
    ] = "count",
) -> AnyGeoDataFrame:
    """
    Adds a numeric column with value 1 for every row.
    Use this before passing a GeoDataFrame to draw_time_series_bar_chart
    so that rows can be counted via a 'sum' aggregation.
    """
    gdf = geodataframe.copy()
    gdf[column_name] = 1
    return cast(AnyGeoDataFrame, gdf)


@register(tags=["ccfn"])
def normalize_time_column(
    geodataframe: Annotated[
        AnyGeoDataFrame,
        Field(description="Input GeoDataFrame containing a timezone-aware datetime column.", exclude=True),
    ],
    time_col: Annotated[
        str,
        Field(description="Name of the datetime column to strip timezone info from."),
    ],
) -> AnyGeoDataFrame:
    """
    Removes timezone info from a datetime column so it is compatible with
    draw_time_series_bar_chart, which builds truncated datetime objects
    using datetime.datetime(x.year, x.month, ...) without timezone context.
    """
    gdf = geodataframe.copy()
    if time_col in gdf.columns and pd.api.types.is_datetime64_any_dtype(gdf[time_col]):
        gdf[time_col] = gdf[time_col].dt.tz_localize(None)
    return cast(AnyGeoDataFrame, gdf)
