import unittest

import pandas as pd
from cache.make_minute_cache import correct_preclose, resample_minute_data


class CorrectPrecloseTest(unittest.TestCase):

    def test_a_normal_case(self):
        m_data = pd.read_csv('./make_minute_cache_data/correct_preclose/minute_data.csv')
        correct_preclose(m_data)
        ans = pd.read_csv('./make_minute_cache_data/correct_preclose/ans.csv')
        self.assertFalse(abs(ans - m_data).sum().any())


class ResampleMinuteDataTest(unittest.TestCase):

    def test_a_normal_case(self):
        m_data = pd.read_csv('./make_minute_cache_data/resample_minute_data/minute_data.csv', parse_dates=['datetime'])
        resampled = resample_minute_data(m_data, '10min')
        ans = pd.read_csv('./make_minute_cache_data/resample_minute_data/ans.csv', parse_dates=['datetime'])
        self.assertFalse(abs(ans - resampled).sum().any())


if __name__ == '__main__':
    unittest.main()
