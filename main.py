# %%
from ETL.extract_olx import OlxScraper
from ETL.extract_otodom import OtodomScraper
from ETL.transform import OlxTransform, OtoDomTransform, JoinEstateData
from ETL.load import FactLoad

# %%
OtoDomExtractionObject = OtodomScraper(key='fnDCgzv5DVue77FXWkHp_')
OtoDomExtractionObject.get_all_urls(print_page_numbers=False)
OtoDomData = OtoDomExtractionObject.scrap_data(print_page_numbers=True)

# %%
OlxExtractionObject = OlxScraper()
OlxExtractionObject.get_all_urls(print_page_numbers=False)
OlxData = OlxExtractionObject.scrap_data()

# %%
Fact_Data = JoinEstateData(OlxTransform(OlxData), OtoDomTransform(OtoDomData))

# %%
FactLoad(Fact_Data)


