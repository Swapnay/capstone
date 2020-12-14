import pandas as pd
from  toazureblob.sabutils import sablobutils
import logging
from toazureblob.sourcetoblob.parent_source import ParentSource

class Housing(ParentSource):
    price_config = {
        "housing_mid_tier": "http://files.zillowstatic.com/research/public_v2/zhvi/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_mon.csv",
        "housing_top_tier": "http://files.zillowstatic.com/research/public_v2/zhvi/City_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_mon.csv",
        "housing_bottom_tier": "http://files.zillowstatic.com/research/public_v2/zhvi/City_zhvi_uc_sfrcondo_tier_0.0_0.33_sm_sa_mon.csv",
        "housing_single_family": "http://files.zillowstatic.com/research/public_v2/zhvi/City_zhvi_uc_sfr_tier_0.33_0.67_sm_sa_mon.csv",
        "housing_condo": "http://files.zillowstatic.com/research/public_v2/zhvi/City_zhvi_uc_condo_tier_0.33_0.67_sm_sa_mon.csv",
        "housing_1_bd": "http://files.zillowstatic.com/research/public_v2/zhvi/City_zhvi_bdrmcnt_1_uc_sfrcondo_tier_0.33_0.67_sm_sa_mon.csv",
        "housing_2_bd": "http://files.zillowstatic.com/research/public_v2/zhvi/City_zhvi_bdrmcnt_2_uc_sfrcondo_tier_0.33_0.67_sm_sa_mon.csv",
        "housing_3_bd": "http://files.zillowstatic.com/research/public_v2/zhvi/City_zhvi_bdrmcnt_3_uc_sfrcondo_tier_0.33_0.67_sm_sa_mon.csv",
        "housing_4_bd": "http://files.zillowstatic.com/research/public_v2/zhvi/City_zhvi_bdrmcnt_4_uc_sfrcondo_tier_0.33_0.67_sm_sa_mon.csv",
        "housing_5_bd": "http://files.zillowstatic.com/research/public_v2/zhvi/City_zhvi_bdrmcnt_5_uc_sfrcondo_tier_0.33_0.67_sm_sa_mon.csv"
    }

    inventory_metro_config = {
        "housing_for_sale": "http://files.zillowstatic.com/research/public_v2/invt_fs/Metro_invt_fs_uc_sfrcondo_smoothed_month.csv",
        "housing_days_to_pending": "http://files.zillowstatic.com/research/public_v2/med_doz_pending/Metro_med_doz_pending_uc_sfrcondo_smoothed_monthly.csv",
        "housing_median_sale_price": "http://files.zillowstatic.com/research/public_v2/median_sale_price/Metro_median_sale_price_uc_SFRCondo_sm_sa_month.csv",
        "housing_median_list_price":"http://files.zillowstatic.com/research/public_v2/mlp/Metro_mlp_uc_sfrcondo_smoothed_month.csv",
        "housing_mean_price_cut": "http://files.zillowstatic.com/research/public_v2/mean_listings_price_cut_amt/Metro_mean_listings_price_cut_amt_uc_sfrcondo_raw_month.csv",
        "housing_median_price_cut" :"http://files.zillowstatic.com/research/public_v2/med_listings_price_cut_amt/Metro_med_listings_price_cut_amt_uc_sfrcondo_raw_month.csv"
        }
    rental_metro_config = {
        "housing_rental_all_homes":"http://files.zillowstatic.com/research/public_v2/zori/Metro_ZORI_AllHomesPlusMultifamily_SSA.csv"
    }
    logger = logging.getLogger('pyspark')

    def upload_to_azure_blob(self):
        try:
            for key in self.inventory_metro_config:
                spark = sablobutils.get_spark_session()
                housing_data = pd.read_csv(self.inventory_metro_config[key])
                sablobutils.write_to_blob(housing_data,key)
        except Exception as ex:
            self.logger.error(ex)

        try:
            for key in self.price_config:
                housing_data = pd.read_csv(self.price_config[key])
                sablobutils.write_to_blob(housing_data,key)
        except Exception as ex:
            self.logger.error(ex)

        try:
            for key in self.rental_metro_config:
                housing_data = pd.read_csv(self.rental_metro_config[key])
                sablobutils.write_to_blob(housing_data,key)
        except Exception as ex:
            self.logger.error(ex)


