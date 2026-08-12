# Debt Sustainability Analysis (DSA) Importer

The `get_dsa` helper fetches and tidies the IMF's Low-Income Country (LIC) debt sustainability assessments. It
downloads the latest PDF released by the IMF and returns a pandas DataFrame with cleaned, structured columns ready for
analysis.

## About the LIC DSA list

The IMF publishes a regularly updated list of debt sustainability assessments for low-income countries in PDF format.
Each release summarises the publication date of the latest assessment, the assessed risk of debt distress, and whether
the work was carried out jointly with the World Bank.

The source document can be accessed on the IMF website: [Latest LIC DSA list](https://www.imf.org/external/Pubs/ft/dsa/DSAlist.pdf)

## Basic usage

Call `get_dsa()` to receive the cleaned pandas data frame. No additional parameters are required.

```python
from bblocks.data_importers import get_dsa

df = get_dsa()
```


**Column reference**:

- `country_name` – Country name with IMF footnote markers removed.
- `latest_publication` – The date of the latest publication
- `risk_of_debt_distress` – The risk of debt distress ('High', 'Moderate', 'Low', 'In debt distress').
- `debt_sustainability_assessment` – The debt sustainability assessment category ('Sustainable', 'Unsustainable')
- `joint_with_world_bank` – Boolean flag indicating whether the assessment was prepared jointly with the World Bank.
- `latest_dsa_discussed` - Date of the latest DSA discussed by the Executive Board but not yet published 


If you encounter format changes in the source PDF, please [open an issue](https://github.com/ONEcampaign/bblocks_data_importers/issues)
so we can update the parser accordingly.


## Caching

The data is cached using LRU cache to avoid unnecessary repeated downloads. To
clear the cache restart the Python session.