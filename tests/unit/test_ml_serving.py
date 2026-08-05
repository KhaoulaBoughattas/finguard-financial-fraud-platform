import pandas as pd

from src.ml.features import FEATURE_COLUMNS
from src.ml.serving import _feature_row_to_model_input


def test_feature_row_to_model_input_produces_numeric_dtypes_not_object():
    """The core regression test for the dtype-collapse bug found during
    Phase 7: extracting one row from a mixed-type DataFrame as a Series
    (which can only hold one dtype) turns every value into `object`,
    which XGBoost's dtype validation then rejects."""
    row_dict = {col: 1.0 for col in FEATURE_COLUMNS}
    row_dict["is_unusual_hour"] = False
    row_dict["is_cross_border"] = True
    row_dict["customer_id"] = "C001"  # extra non-feature column, like a real feature row has
    series = pd.Series(row_dict)  # collapses everything to object dtype, as the bug did

    result = _feature_row_to_model_input(series)

    assert result.dtypes["is_unusual_hour"] == bool
    assert result.dtypes["is_cross_border"] == bool
    for col in FEATURE_COLUMNS:
        if col not in ("is_unusual_hour", "is_cross_border"):
            assert pd.api.types.is_float_dtype(result.dtypes[col]), (
                f"{col} has dtype {result.dtypes[col]}, expected float"
            )


def test_feature_row_to_model_input_has_exactly_the_expected_columns():
    row_dict = {col: 1.0 for col in FEATURE_COLUMNS}
    row_dict["is_unusual_hour"] = False
    row_dict["is_cross_border"] = True
    row_dict["extra_column_not_a_feature"] = "should be dropped"
    series = pd.Series(row_dict)

    result = _feature_row_to_model_input(series)

    assert list(result.columns) == FEATURE_COLUMNS
    assert len(result) == 1
