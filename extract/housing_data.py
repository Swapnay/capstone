import pandas as pd

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
}

for key in inventory_metro_config:
    housing_data = pd.read_csv(inventory_metro_config[key])
    housing_data.to_csv("../datasets/" + key + ".csv")
    print(housing_data.dtypes)
