import sys
import unittest

import pandas as pd
from pandas._testing import assert_frame_equal

from plugins.main import transform_analytics_arrete, transform_analytics_niveau


class TestTransform(unittest.TestCase):
    def test_transform(self):
        df_arretes = pd.read_csv('data_input/arrete.csv', dtype=object)
        df_zone_alerte = pd.read_csv('data_input/zone_alerte.csv', dtype=object)

        df_analytics_arrete = transform_analytics_arrete(df_arretes)
        df_analytics_niveau = transform_analytics_niveau(df_arretes, df_zone_alerte)

        df_expected_analytics_arrete = pd.read_csv('data_output_expected/analytics_arrete.csv', dtype=object)
        df_expected_analytics_niveau = pd.read_csv('data_output_expected/analytics_niveau.csv', dtype=object)

        assert_frame_equal(df_analytics_arrete.astype(str), df_expected_analytics_arrete.astype(str), check_dtype=False)
        assert_frame_equal(df_analytics_niveau.astype(str), df_expected_analytics_niveau.astype(str), check_dtype=False)
