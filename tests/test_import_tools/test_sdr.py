import pandas as pd
import pytest
from bblocks.import_tools import sdr
from bblocks.config import PATHS


def test_create_tsv_link():
    """Test create_tsv_link"""

    link = sdr.create_tsv_link(2022, 10,  31)

    assert link == 'https://www.imf.org/external/np/fin/tad/extsdr2.aspx?date1key=2022-10-31&tsvflag=Y'


def test_parse_sdr_links():
    """Test parse_sdr_links"""

    response = open(f'{PATHS.test_files}/sdr_main_page.html', 'rb').read()
    link = sdr.parse_sdr_links(response)

    expected = {'2022': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2022', '2021': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2021', '2020': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2020', '2019': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2019', '2018': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2018', '2017': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2017', '2016': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2016', '2015': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2015', '2014': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2014', '2013': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2013', '2012': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2012', '2011': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2011', '2010': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2010', '2009': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2009', '2008': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2008', '2007': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2007', '2006': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2006', '2005': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2005', '2004': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2004', '2003': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2003', '2002': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2002', '2001': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2001', '2000': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=2000', '1999': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1999', '1998': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1998', '1997': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1997', '1996': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1996', '1995': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1995', '1994': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1994', '1993': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1993', '1992': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1992', '1991': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1991', '1990': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1990', '1989': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1989', '1988': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1988', '1987': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1987', '1986': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1986', '1985': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1985', '1984': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1984', '1983': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1983', '1982': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1982', '1981': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1981', '1980': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1980', '1979': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1979', '1978': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1978', '1977': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1977', '1976': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1976', '1975': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1975', '1974': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1974', '1973': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1973', '1972': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1972', '1971': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1971', '1970': 'https://www.imf.org/external/np/fin/tad/extsdr3.aspx?dateyear=1970'}

    assert link == expected


def test_clean_df():
    """test clean_df"""

    df_to_clean = pd.DataFrame({'SDR Allocations and Holdings': {0: 'for all members as of October 31, 2022', 1: '(in SDRs)', 2: 'Members\tSDR Holdings\tSDR Allocations', 3: 'Afghanistan, Islamic Republic of\t342,615,561\t465,662,004', 4: 'Albania\t203,310,003\t179,963,045', 5: 'Algeria\t3,129,799,553\t3,076,660,983', 6: 'Andorra\t93,813,046\t114,444,729', 7: 'Angola\t792,096,079\t982,361,190', 8: 'Antigua and Barbuda\t240,860\t31,668,625', 9: 'Argentina\t2,272,530,198\t5,074,923,669', 10: 'Armenia, Republic of\t14,763,044\t211,437,666'}}
    )

    df = sdr.clean_df(df_to_clean, '2022-10-31')
    expected_df = pd.DataFrame({'entity': {0: 'Afghanistan, Islamic Republic of', 1: 'Albania', 2: 'Algeria', 3: 'Andorra', 4: 'Angola', 5: 'Antigua and Barbuda', 6: 'Argentina', 7: 'Armenia, Republic of', 8: 'Afghanistan, Islamic Republic of', 9: 'Albania', 10: 'Algeria', 11: 'Andorra', 12: 'Angola', 13: 'Antigua and Barbuda', 14: 'Argentina', 15: 'Armenia, Republic of'}, 'indicator': {0: 'holdings', 1: 'holdings', 2: 'holdings', 3: 'holdings', 4: 'holdings', 5: 'holdings', 6: 'holdings', 7: 'holdings', 8: 'allocations', 9: 'allocations', 10: 'allocations', 11: 'allocations', 12: 'allocations', 13: 'allocations', 14: 'allocations', 15: 'allocations'}, 'value': {0: 342615561.0, 1: 203310003.0, 2: 3129799553.0, 3: 93813046.0, 4: 792096079.0, 5: 240860.0, 6: 2272530198.0, 7: 14763044.0, 8: 465662004.0, 9: 179963045.0, 10: 3076660983.0, 11: 114444729.0, 12: 982361190.0, 13: 31668625.0, 14: 5074923669.0, 15: 211437666.0}, 'date': {0: '2022-10-31', 1: '2022-10-31', 2: '2022-10-31', 3: '2022-10-31', 4: '2022-10-31', 5: '2022-10-31', 6: '2022-10-31', 7: '2022-10-31', 8: '2022-10-31', 9: '2022-10-31', 10: '2022-10-31', 11: '2022-10-31', 12: '2022-10-31', 13: '2022-10-31', 14: '2022-10-31', 15: '2022-10-31'}}
                               ).assign(date=lambda x: pd.to_datetime(x['date']))

    pd.testing.assert_frame_equal(df, expected_df)


def test_format_date():
    """Test format_date"""

    expected = '2022-10-31'
    assert sdr.format_date([2022, 10]) == expected
    assert sdr.format_date((2022, 10)) == expected

    with pytest.raises(ValueError) as error:
        sdr.format_date([2022, 10, 31])

    assert str(error.value) == 'Date must be a list or tuple containing year and month'


def test_parse_exchange():
    """test parse_exchange"""

    response = open(f'{PATHS.test_files}/sdr_exchange_rate.html', 'rb').read()
    parsed_usd = sdr.parse_exchange(response, 'USD')
    parsed_sdr = sdr.parse_exchange(response, 'SDR')

    assert parsed_usd == ('2022-11-11', 0.763884)
    assert parsed_sdr == ('2022-11-11', 1.309100)


def test_get_latest_exchange_rate():
    """Test get_latest_exchange_rate"""

    with pytest.raises(ValueError) as error:
        sdr.get_latest_exchange_rate('invalid currency')

    assert 'Invalid currency' in str(error.value)










