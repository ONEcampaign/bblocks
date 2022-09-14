""" """
import pandas as pd
import requests
from dataclasses import dataclass
import json
from bblocks.import_tools.common import ImportData
import os
from bblocks.config import PATHS

URL = 'https://aidsinfo.unaids.org/datasheetdatarequest'


indicators = {'People living with HIV - All ages': 'People living with HIV',
 'People living with HIV - Children (0-14)': 'People living with HIV',
 'People living with HIV - Adolescents (10-19)': 'People living with HIV',
 'People living with HIV - Young people (15-24)': 'People living with HIV',
 'People living with HIV - Adults (15+)': 'People living with HIV',
 'People living with HIV - Adults (15-49)': 'People living with HIV',
 'People living with HIV - People aged 50 and over': 'People living with HIV',
 'HIV Prevalence - Adults (15-49)': 'People living with HIV',
 'HIV Prevalence - Young people (15-24)': 'People living with HIV',
 'HIV Prevalence - Adults (15+)': 'People living with HIV',
 'New HIV infections - All ages': 'New HIV infections',
 'New HIV infections - Children (0-14)': 'New HIV infections',
 'New HIV infections - Adolescents (10-19)': 'New HIV infections',
 'New HIV infections - Young people (15-24)': 'New HIV infections',
 'New HIV infections - Adults (15+)': 'New HIV infections',
 'New HIV infections - Adults (15-49)': 'New HIV infections',
 'New HIV infections - People aged 50 and over': 'New HIV infections',
 'HIV incidence per 1000 population - All ages': 'New HIV infections',
 'HIV incidence per 1000 population - Young people (15-24)': 'New HIV infections',
 'HIV incidence per 1000 population - Adults (15+)': 'New HIV infections',
 'HIV incidence per 1000 population - Adults (15-49)': 'New HIV infections',
 'HIV incidence per 1000 population - People aged 50 and over': 'New HIV infections',
 'AIDS-related deaths - All ages': 'AIDS-related deaths',
 'AIDS-related deaths - Children (0-14)': 'AIDS-related deaths',
 'AIDS-related deaths - Adolescents (10-19)': 'AIDS-related deaths',
 'AIDS-related deaths - Young people (15-24)': 'AIDS-related deaths',
 'AIDS-related deaths - Adults (15+)': 'AIDS-related deaths',
 'AIDS-related deaths - Adults (15-49)': 'AIDS-related deaths',
 'AIDS-related deaths - People aged 50 and over': 'AIDS-related deaths',
 'AIDS mortality per 1000 population': 'AIDS-related deaths',
 'AIDS orphans (0-17)': 'AIDS-related deaths',
 'Progress towards 95-95-95 targets - All ages': 'Treatment cascade',
 'Progress towards 95-95-95 target - Male adults (15+)': 'Treatment cascade',
 'Progress towards 95-95-95 target - Female adults (15+)': 'Treatment cascade',
 'Testing and treatment cascade - All ages': 'Treatment cascade',
 'Testing and treatment cascade - Male adults (15+)': 'Treatment cascade',
 'Testing and treatment cascade - Female adults (15+)': 'Treatment cascade',
 'People living with HIV who know their status (%)': 'Treatment cascade',
 'People living with HIV receiving ART (%)': 'Treatment cascade',
 'People living with HIV receiving ART (#)': 'Treatment cascade',
 'People living with HIV who have suppressed viral loads (%)': 'Treatment cascade',
 'Late HIV diagnosis': 'Treatment cascade',
 'Coverage of pregnant women who receive ARV for PMTCT': 'Elimination of vertical transmission',
 'Pregnant women who received ARV for PMTCT': 'Elimination of vertical transmission',
 'Pregnant women needing ARV for PMTCT': 'Elimination of vertical transmission',
 'Vertical transmission rate': 'Elimination of vertical transmission',
 'New HIV Infections averted due to PMTCT': 'Elimination of vertical transmission',
 'Early infant diagnosis': 'Elimination of vertical transmission',
 'HIV testing among pregnant women': 'Elimination of vertical transmission',
 'HIV-exposed children who are uninfected': 'Elimination of vertical transmission',
 'Sex workers: Size estimate': 'Sex workers',
 'HIV prevalence among sex workers': 'Sex workers',
 'HIV testing and status awareness among sex workers': 'Sex workers',
 'Condom use among sex workers': 'Sex workers',
 'Coverage of HIV prevention programmes among sex workers': 'Sex workers',
 'Antiretroviral therapy coverage among sex workers living with HIV': 'Sex workers',
 'Experience of sexual and/or physical violence among sex workers': 'Sex workers',
 'Experience of stigma and discrimination among sex workers': 'Sex workers',
 'Avoidance of health care because of stigma and discrimination': 'Transgender people',
 'Viral hepatitis among sex workers': 'Sex workers',
 'Syphilis prevalence among sex workers': 'Sex workers',
 'Men who have sex with men: Size estimate': 'Men who have sex with men',
 'HIV prevalence among men who have sex with men': 'Men who have sex with men',
 'HIV testing and status awareness among men who have sex with men': 'Men who have sex with men',
 'Condom use among men who have sex with men': 'Men who have sex with men',
 'Coverage of HIV prevention programmes among men who have sex with men': 'Men who have sex with men',
 'Antiretroviral therapy coverage among men who have sex with men living with HIV': 'Men who have sex with men',
 'Experience of sexual and/or physical violence among men who have sex with men': 'Men who have sex with men',
 'Experience of stigma and discrimination among men who have sex with men': 'Men who have sex with men',
 'Viral hepatitis among men who have sex with men': 'Men who have sex with men',
 'Syphilis prevalence among men who have sex with men': 'Men who have sex with men',
 'People who inject drugs: Size estimate': 'People who inject drugs',
 'HIV prevalence among people who inject drugs': 'People who inject drugs',
 'Experience of stigma and discrimination among people who inject drugs': 'People who inject drugs',
 'HIV testing and status awareness among people who inject drugs': 'People who inject drugs',
 'Condom use among people who inject drugs': 'People who inject drugs',
 'Coverage of HIV prevention programmes among people who inject drugs': 'People who inject drugs',
 'Antiretroviral therapy coverage among people who inject drugs living with HIV': 'People who inject drugs',
 'Safe injecting practices': 'People who inject drugs',
 'Number of needles per person who injects drugs per year': 'People who inject drugs',
 'Coverage of opioid substitution therapy': 'People who inject drugs',
 'Experience of sexual and/or physical violence among people who inject drugs': 'People who inject drugs',
 'People who inject drugs: Avoidance of health care because of stigma and discrimination': 'Stigma and Discrimination',
 'Viral hepatitis among people who inject drugs': 'People who inject drugs',
 'Prisoners: Size estimate': 'Prisoners',
 'HIV prevalence among prisoners': 'Prisoners',
 'HIV prevention programmes in prisons': 'Prisoners',
 'Antiretroviral therapy coverage among prisoners living with HIV': 'Prisoners',
 'Viral hepatitis among prisoners': 'Prisoners',
 'Transgender people: size estimate': 'Transgender people',
 'HIV prevalence among transgender people': 'Transgender people',
 'HIV testing and status awareness among transgender people': 'Transgender people',
 'Condom use among transgender people': 'Transgender people',
 'Coverage of HIV prevention programmes among transgender people': 'Transgender people',
 'Antiretroviral therapy coverage among transgender people living with HIV': 'Transgender people',
 'Experience of sexual and/or physical violence among transgender people': 'Transgender people',
 'Experience of stigma and discrimination among transgender people': 'Transgender people',
 'Viral hepatitis among transgender people': 'Transgender people',
 'Syphilis prevalence among transgender people': 'Transgender people',
 'People receiving pre-exposure prophylaxis (PrEP)': 'Combination prevention',
 'Condom use at last high-risk sex': 'Combination prevention',
 'Prevalence of male circumcision': 'Combination prevention',
 'Number of male circumcisions performed': 'Combination prevention',
 'Annual number of condoms distributed': 'Combination prevention',
 'HIV tests (volume)': 'Combination prevention',
 'HIV positivity rate (%)': 'Combination prevention',
 'Self-tests': 'Combination prevention',
 'Experience of HIV-related discrimination in healthcare settings': 'Stigma and Discrimination',
 'Discriminatory attitudes towards people living with HIV': 'Stigma and Discrimination',
 'Internalized stigma reported by people living with HIV': 'Stigma and Discrimination',
 'People living with HIV seeking redress for violations of their rights': 'Stigma and Discrimination',
 'Stigma and discrimination experienced by people living with HIV in community settings': 'Stigma and Discrimination',
 'Sex workers: Avoidance of health care because of stigma and discrimination': 'Stigma and Discrimination',
 'Men who have sex with men: Avoidance of health care because of stigma and discrimination': 'Stigma and Discrimination',
 'Transgender people: Avoidance of health care because of stigma and discrimination': 'Stigma and Discrimination',
 'Attitudes towards violence against women': 'Gender',
 'Prevalence of recent intimate partner violence': 'Gender',
 'Demand for family planning satisfied by modern methods': 'Gender',
 'Knowledge about HIV prevention among young people (15-24)': 'Young people',
 'Country-reported HIV expenditure': 'HIV expenditure',
 'Co-management of TB and HIV treatment': 'TB nd HIV',
 'TB-related deaths among people living with HIV': 'TB nd HIV',
 'Incident TB cases in people living with HIV': 'TB nd HIV',
 'Proportion of people living with HIV newly enrolled in HIV care with active TB disease': 'TB nd HIV',
 'Proportion of people living with HIV currently enrolled in HIV care receiving TB preventative therapy': 'TB nd HIV',
 'Hepatitis C testing': 'Hepatitis C and HIV',
 'People coinfected with HIV and HCV starting HCV treatment': 'Hepatitis C and HIV',
 'Cervical cancer screening of women living with HIV (survey data)': 'Cervical canver aand HIV',
 'Cervical cancer and HIV screening (programme data)': 'Cervical canver aand HIV'}


geographies = {'country': {'name': 'world', 'code': 2}, 'region': {'name': 'world-continents', 'code': 1}}


@dataclass
class AidsData:
    """ """

    indicator: str
    region: str
    indicator_group: str = None
    region_name = None
    region_code=None

    def __post_init__(self):

        if self.indicator not in indicators:
            raise ValueError('Invalid indicator')
        else:
            self.indicator_group = indicators[self.indicator]

        if self.region not in geographies:
            raise ValueError('Invalid geography')
        else:
            self.region_name = geographies[self.region]['name']
            self.region_code = geographies[self.region]['code']

    @property
    def data(self):
        return {
            'reqObj[Group_Name]': self.indicator_group,
            'reqObj[Display_Name]': self.indicator,
            'reqObj[TabStatus]': self.region_name,
            'reqObj[Area_Level]': self.region_code
        }

    @property
    def headers(self):
        return {'Accept': 'application/json, text/javascript, */*; q=0.01',
                          'Accept-Language': 'en-US,en;q=0.9',
                          'Connection': 'keep-alive',
                          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                          'Origin': 'https://aidsinfo.unaids.org',
                          'Referer': 'https://aidsinfo.unaids.org/',
                          'Sec-Fetch-Dest': 'empty',
                          'Sec-Fetch-Mode': 'cors',
                          'Sec-Fetch-Site': 'same-origin',
                          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
                          'X-Requested-With': 'XMLHttpRequest',
                          'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
                          'sec-ch-ua-mobile': '?0',
                          'sec-ch-ua-platform': '"Windows"'}

    def get_response(self) -> dict:
        """returns a json response from UNAIDS"""
        try:
            response = requests.post(URL, headers=self.headers, data=self.data)
            return json.loads(response.content)

        except:
            raise ConnectionError('Could not extract data')


@dataclass
class Aids(ImportData):
    """ """

    grouping: str = 'country'

    def __post_init__(self):

        if self.grouping not in ['country', 'region']:
            raise ValueError('Invalid grouping. Choose from ["country", "region"]')
        self.indicators = dict()

    def load_indicator(self, indicator: str) -> ImportData:
        """Load an indicator"""

        if not os.path.exists(f"{PATHS.imported_data}/aids_{self.grouping}_{indicator}") or \
                (indicator not in self.indicators and self.update_data):

            df = self._extract_data(indicator)
            self.indicators[indicator] = df
            df.to_csv(f"{PATHS.imported_data}/aids_{self.grouping}_{indicator}.csv", index=False)

        df = pd.read_csv(f"{PATHS.imported_data}/aids_{self.grouping}_{indicator}.csv")

        self.indicators[indicator]: df

        return self

    def _extract_data(self, indicator):

        records = []

        aids = AidsData(indicator, self.grouping)
        data = aids.get_response()

        categories = data['MultiSubgroups'][0]
        years = data['TimePeriod']

        for row in data['tableData']:
            country = row['Area_ID']
            for i in range(len(row['Data_Val'])):

                values = row['Data_Val'][i]
                values_dict = {categories[j]: values[0][j] for j in range(len(categories))}
                values_dict.update({'country': country, 'year': years[i]})

                records.append(values_dict)

        df = pd.DataFrame.from_records(records)

        return df

    def update(self, reload_data: bool = True):

        if len(self.indicators) == 0:
            raise RuntimeError("No indicators loaded")

        for indicator in self.indicators:
            df = self._extract_data(indicator)
            df.to_csv(f"{PATHS.imported_data}/aids_{self.grouping}_{indicator}", index=False)

            if reload_data is True:
                self.indicators[indicator] = df

    def get_data(self, indicator):

        if indicator not in self. indicators:
            raise ValueError(f'Indicator not loaded: {indicator}')

        return self.indicators[indicator]
















