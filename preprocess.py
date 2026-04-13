from typing import Optional
from pyspark.sql import SparkSession
import os
from datetime import date, datetime
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.types import *
import re

grade_de_d_carm =["rmad_d_cfc_hbde_undrcust"]
grade_fr_d_carm = ["rmad_d_cfc_hbfr_undrcust"]
grade_hsen_d_carm =["rmad_d_cfc_hscn_undrcust", "rmad_d_cfc_hshk_undrcust", "rmad_d_cfc_hsmo_undrcust",
"rmad_d_cfc_hssg_undrcust"]
grade_hbap_d_carm =["rmad_d_cfc_hbau_undrcust", "rmad_d_cfc_hbbd_undrcust", "rmad_d_cfc_hbbn_undrcust",
"rmad_d_cfc_hbcn_undrcust", "rmad_d_cfc_hbhk_undrcust", "rmad_d_cfc_hbid_undrcust", "rmad_d_cfc_hbin_undrcust",
"rmad_d_cfc_hbjp_undrcust", "rmad_d_cfc_hbkr_undrcust", "rmad_d_cfc_hblk_undrcust", "rmad_d_cfc_hbmo_undrcust",
"rmad_d_cfc_hbmu_undrcust", "rmad_d_cfc_hbmy_undrcust", "rmad_d_cfc_hbnz_undrcust", "rmad_d_cfc_hbph_undrcust",
"rmad_d_cfc_hbsg_undrcust", "rmad_d_cfc_hbth_undrcust", "rmad_d_cfc_hbvn_undrcust"]
grade_latam_d_carm =["rmad_d_cfc_hbar_undrcust", "rmad_d_cfc_hbbm_undrcust", "rmad_d_cfc_hbms_undrcust",
"rmad_d_cfc_hbbz_undrcust", "rmad_d_cfc_hbcl_undrcust", "rmad_d_cfc_hbmx_undrcust"]
grade_rstof_hbeu_d_carm =["rmad_d_cfc_hbam_undrcust", "rmad_d_cfc_hbbe_undrcust", "rmad_d_cfc_hbch_undrcust",
"rmad_d_cfc_hbcz_undrcust", "rmad_d_cfc_hbes_undrcust", "rmad_d_cfc_hbgr_undrcust", "rmad_d_cfc_hbie_undrcust",
"rmad_d_cfc_hbil_undrcust", "rmad_d_cfc_hbit_undrcust", "rmad_d_cfc_hbje_undrcust", "rmad_d_cfc_hblu_undrcust",
"rmad_d_cfc_hbmt_undrcust", "rmad_d_cfc_hbnl_undrcust", "rmad_d_cfc_hbpl_undrcust", "rmad_d_cfc_hbru_undrcust",
"rmad_d_cfc_hbza_undrcust"]
grade_tw_d_carm =["rmad_d_cfc_hbtw_undrcust", "rmad_d_cfc_hbuy_undrcust"]
grade_uk_d_carm =["rmad_d_cfc_hbuk_undrcust"]
grade_us_d_carm = ["rmad_d_cfc_hbus_undrcust"]

start_date = "200709"  ## was None, unter_
end_date = "202512"

def history_selector():
    global start_date, end_date
    if part == 1:
        start_date = "201606"
        end_date = "201612"
    elif part == 2:
        start_date = "201701"
        end_date = "201706"
    elif part == 3:
        start_date = "201707"
        end_date = "201712"
    elif part == 4:
        start_date = "201801"
        end_date = "201806"
    elif part == 5:
        start_date = "201807"
        end_date = "201812"
    elif part == 6:
        start_date = "201901"
        end_date = "201906"
    elif part == 7:
        start_date = "201907"
        end_date = "201912"
    elif part == 8:
        start_date = "202001"
        end_date = "202006"
    elif part == 9:
        start_date = "202007"
        end_date = "202012"
    elif part == 10:
        start_date = "202101"
        end_date = "202106"
    elif part == 11:
        start_date = "202107"
        end_date = "202112"
    elif part == 12:
        start_date = "202201"
        end_date = "202206"
    elif part == 13:
        start_date = "202207"
        end_date = "202212"
    elif part == 14:
        start_date = "202301"
        end_date = "202306"
    elif part == 15:
        start_date = "202307"
        end_date = "202312"
    elif part == 16:
        start_date = "202401"
        end_date = "202406"
    elif part == 17:
        start_date = "202407"
        end_date = "202412"
    elif part == 18:
        start_date = "202501"
        end_date = "202506"
    elif part == 19:
        start_date = "202507"
        end_date = "202512"

def loop_instance():
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    # ############################## 1. Map your schemas to the lists you defined earlier in the script, CHANGE BECAUSE THE NEW NAMES IN HIVE
    db_region_tables_map = {
        "grade_de_d_carm": grade_de_d_carm,
        "grade_fr_d_carm": grade_fr_d_carm,
        "grade_hsen_d_carm": grade_hsen_d_carm,
        "grade_hbap_d_carm": grade_hbap_d_carm,
        "grade_latam_d_carm": grade_latam_d_carm,
        "grade_rstof_hbeu_d_carm": grade_rstof_hbeu_d_carm,
        "grade_tw_d_carm": grade_tw_d_carm,
        "grade_uk_d_carm": grade_uk_d_carm,
        "grade_us_d_carm": grade_us_d_carm
    }

    # 2. Create a list to hold all the dataframes we are about to read
    dfs_to_union =[]

    # 3. Double loop through schemas and their respective tables
    for schema_name, tables_list in db_region_tables_map.items():
        for table_name in tables_list:

            # --- THE NEW DEFINITION FOR src_tbl_name ---
            src_tbl_name = f"{schema_name}.{table_name}"
            print(f"Reading table: {src_tbl_name}")

            try:
                # Read the table
                src_df = spark.table(src_tbl_name)

                # KEEP YOUR EXISTING SELECTION LOGIC HERE (lines 150-157 from Image 3)
                reg_undrcust_df = src_df.select(
                    F.col("CARM_INSTANCE_STANDALONE"),
                    F.col("UNDERLYING_CUST_ID_STANDALONE").cast("string"),
                    F.col("RELATIONSHIP_ID_STANDALONE").cast("string"),
                    F.col("CUSTOMER_ID_STANDALONE").cast("string"),
                    F.col("REL_MANAGER_ID_STANDALONE").cast("string"),
                    F.col("UPDATE_DATE_STANDALONE"),
                    F.col("UPDATE_TIME_STANDALONE")
                )

                # Append the processed dataframe to our list
                dfs_to_union.append(reg_undrcust_df)

            except Exception as e:
                print(f"Warning - Could not read {src_tbl_name}: {e}")

    if not dfs_to_union:
        print("Warning - No DataFrames successfully read.")
        return None

    all_undercust = dfs_to_union[0]
    for df_part in dfs_to_union[1:]:
        all_undercust = all_undercust.unionByName(df_part, allowMissingColumns=True)

    # ###################################################################################### change _
    all_undercust = all_undercust.withColumn(
        "CARM_INSTANCE_STANDALONE",
        F.when(
            F.trim(F.col("CARM_INSTANCE_STANDALONE")) == F.lit("CARM_BMS"),
            F.lit("CARM_BM"),
        ).otherwise(F.col("CARM_INSTANCE_STANDALONE"))
    )

    return all_undercust

# TODO: Ensure Spark is configured to connect to Hive metastore and Kerberos if required.
spark: SparkSession = (
    SparkSession.builder.appName("cdcdsc_local_id_matching_history_pre_processing_css_v4_13")
    .enableHiveSupport()
    .getOrCreate()
)

GRADE_HIVE_CFG: Optional[str] = os.environ.get("SAS_HADOOP_CONFIG_PATH")
GRADE_HIVE_PARAMETERS: str = "tez.queue.name=GRADE"
GRADE_HIVE_URI: str = (
    "jdbc:hive2://gbl20167157.systems.uk.hsbc:2181,"
    "gbl20167158.systems.uk.hsbc:2181,"
    "gbl20167159.systems.uk.hsbc:2181/;serviceDiscoveryMode=zooKeeper;"
    "zooKeeperNamespace=hiveserver2;ssl=true;hive.load.dynamic.partitions.thread=1;"
    "hive.mv.files.thread=1;hive.acid.direct.insert.enabled=false;"
    "EMAIL;3e9d3d7d58b9da8653cf8a439f08d26dd5edd8c54be8ad3a7c1a447b25fd461c18cb633eaa9f0dcf79c645a4812ac4349bf8ea516164cdaf8adfa7ddb4|"
    f"{GRADE_HIVE_PARAMETERS}"
)

GRADE_HIVE_SERVER: str = (
    "gbl20167157.systems.uk.hsbc,"
    "gbl20167158.systems.uk.hsbc,"
    "gbl20167159.systems.uk.hsbc"
)
GRADE_HIVE_PORT: str = "2181"
ver: int = 413
current_run: str = "N"
part: int = 1

# Macro call to set start and end dates for the period.
history_selector()
# TODO: Retrieve start_date and end_date values from the history_selector macro context.
start_date: Optional[str] = None
end_date: Optional[str] = None

print(f"NOTE: Running for period {start_date} --> {end_date}.")

# TODO: Configure and validate access to the Hive schema grade_global_d_gcdu in Spark.

# PROC SQL: Create gcdu_audit as distinct grade_detail_table filtered by substring
df_hist_audit_log: DataFrame = spark.table("grade_global_d_gcdu.rmad_d_gcdu_hist_audit_log")
gcdu_audit: DataFrame = (
    df_hist_audit_log.where(F.col("grade_detail_table").contains("RMAD_D_GCDU_HIST_"))
    .select(F.col("grade_detail_table"))
    .distinct()
    .orderBy(F.col("grade_detail_table"))
)
gcdu_audit.createOrReplaceTempView("gcdu_audit")

# DATA step: derive yyyymm, yyyy, mm from grade_detail_table
gcdu_audit_enriched: DataFrame = (
    gcdu_audit.withColumn(
        "yyyymm",
        F.reverse(
            F.substring(F.reverse(F.trim(F.col("grade_detail_table"))), 1, 6)
        )
    )
    .withColumn("yyyy", F.substring(F.col("yyyymm"), 1, 4))
    .withColumn("mm", F.substring(F.col("yyyymm"), 5, 2))
)
gcdu_audit_enriched.createOrReplaceTempView("gcdu_audit")

# PROC SQL: years as distinct yyyy
years: DataFrame = gcdu_audit_enriched.select("yyyy").distinct().orderBy(F.col("yyyy"))
years.createOrReplaceTempView("years")

# DATA step: years_and_months with cross join years and months 01..12
months_df: DataFrame = spark.createDataFrame([(i,) for i in range(1, 13)],["i"]).withColumn(
    "mm", F.lpad(F.col("i").cast("int"), 2, "0")
).select("mm")
years_and_months: DataFrame = years.crossJoin(months_df)
years_and_months.createOrReplaceTempView("years_and_months")

# PROC SQL: gcdu_audit_full with right join and end-of-month date
a = gcdu_audit_enriched.alias("a")
b = years_and_months.alias("b")
gcdu_audit_full: DataFrame = (
    b.join(
        a,
        on=[
            F.trim(F.col("a.yyyy")) == F.trim(F.col("b.yyyy")),
            F.trim(F.col("a.mm")) == F.trim(F.col("b.mm")),
        ],
        how="left",
    )
    .select(
        F.col("a.grade_detail_table"),
        F.coalesce(F.col("a.yyyy"), F.col("b.yyyy")).alias("yyyy"),
        F.coalesce(F.col("a.mm"), F.col("b.mm")).alias("mm"),
    )
    .withColumn(
        "yyyymm_dt",
        F.last_day(F.to_date(F.concat_ws("-", F.col("yyyy"), F.col("mm"), F.lit("01")))),
    )
    .orderBy(F.col("b.yyyy").desc_nulls_last(), F.col("b.mm").desc_nulls_last(), F.col("a.grade_detail_table").asc_nulls_last())
)
gcdu_audit_full.createOrReplaceTempView("gcdu_audit_full")

# DATA step: gcdu_month_lookup with lag and fallback logic
w_order = Window.orderBy(F.col("yyyy").desc(), F.col("mm").desc(), F.col("grade_detail_table").asc())

def is_missing_expr(c: F.Column) -> F.Column:
    return c.isNull() | (F.length(F.trim(c)) == 0)

gcdu_month_lookup: DataFrame = (
    gcdu_audit_full.withColumn("grade_detail_table_lag1", F.lag(F.col("grade_detail_table"), 1).over(w_order))
    .withColumn("grade_detail_table_lag2", F.lag(F.col("grade_detail_table"), 2).over(w_order))
    .withColumn("grade_detail_table_lag3", F.lag(F.col("grade_detail_table"), 3).over(w_order))
)

gcdu_month_lookup = gcdu_month_lookup.withColumn(
    "grade_detail_table_lag1",
    F.when(
        is_missing_expr(F.col("grade_detail_table_lag1")) & ~is_missing_expr(F.col("grade_detail_table_lag2")),
        F.col("grade_detail_table_lag2"),
    )
    .when(
        is_missing_expr(F.col("grade_detail_table_lag1")) & is_missing_expr(F.col("grade_detail_table_lag2")),
        F.col("grade_detail_table_lag3"),
    )
    .otherwise(F.col("grade_detail_table_lag1")),
)

gcdu_month_lookup = gcdu_month_lookup.withColumn(
    "grade_detail_table",
    F.when(
        is_missing_expr(F.col("grade_detail_table")) & ~is_missing_expr(F.col("grade_detail_table_lag1")),
        F.col("grade_detail_table_lag1"),
    )
    .otherwise(F.col("grade_detail_table")),
)

gcdu_month_lookup = gcdu_month_lookup.drop("grade_detail_table_lag2", "grade_detail_table_lag3").withColumnRenamed(
    "grade_detail_table_lag1", "grade_detail_table_to_use"
)

gcdu_month_lookup.createOrReplaceTempView("gcdu_month_lookup")

# TODO: SPOE LIBNAME definitions map to HDFS file paths in SAS. Configure Spark/Hive catalog locations as needed.

print(
    f"PREPARATION TASKS STARTED: {datetime.now().date().strftime('%d%b%y').upper()} "
    f"{datetime.now().time().strftime('%H:%M:%S')}"
)

# DATA _NULL_: Compute smonth, emonth, num_months
# TODO: Ensure start_date and end_date are provided as 'YYYYMM' strings by history_selector.
smonth_py: Optional[date] = None
emonth_py: Optional[date] = None
num_months: Optional[int] = None

if start_date and end_date and len(start_date) == 6 and len(end_date) == 6:
    s_year: int = int(start_date[0:4])
    s_month: int = int(start_date[4:6])
    e_year: int = int(end_date[0:4])
    e_month: int = int(end_date[4:6])
    smonth_py = date(s_year, s_month, 1)
    emonth_py = date(e_year, e_month, 1)
    num_months = (e_year - s_year) * 12 + (e_month - s_month) + 1

# TODO: Masked SAS symput targets are not directly applicable.

_INPUT1: str = "grade_uk_api.grade_involved_party" # "PAPI_UK.GRADE_INVOLVED_PARTY"
_INPUT2: str = "grade_ca_api.grade_involved_party" # "PAPI_CA.GRADE_INVOLVED_PARTY"
_INPUT3: str = "grade_hbap_api.grade_involved_party" # "PAPI_AP.GRADE_INVOLVED_PARTY"
_INPUT4: str = "grade_hsen_api.grade_involved_party" # "PAPI_HS.GRADE_INVOLVED_PARTY"
_INPUT5: str = "grade_fr_api.grade_involved_party" # "PAPI_FR.GRADE_INVOLVED_PARTY"
_INPUT6: str = "grade_tw_api.grade_involved_party" # "PAPI_TW.GRADE_INVOLVED_PARTY"
_INPUT7: str = "grade_us_api.grade_involved_party" # "PAPI_US.GRADE_INVOLVED_PARTY"
_INPUT8: str = "grade_global_bank_api.grade_involved_party" # "API_BANK.GRADE_INVOLVED_PARTY"
_INPUT9: str = "grade_mena_api.grade_involved_party" # "PAPI_ME.GRADE_INVOLVED_PARTY"
_INPUT10: str = "grade_rstof_hbeu_api.grade_involved_party" # "PAPI_EU.GRADE_INVOLVED_PARTY"
_INPUT11: str = "grade_latam_api.grade_involved_party" # "PAPI_LA.GRADE_INVOLVED_PARTY"
_INPUT12: str = "grade_de_api.grade_involved_party" # "PAPI_DE.GRADE_INVOLVED_PARTY"
_INPUT13: str = "API_MON.GRADE_INVOLVED_PARTY"

# Build SELECT fragments for union queries.
# TODO: Confirm the default ELSE mapping logic for region classification.
SELECT_STR: str = """
CASE
    WHEN SITE_NAME IN ('Belgium', 'Czech Republic', 'France', 'Greece', 'Ireland', 'Italy', 'Luxembourg',
    'Malta', 'Netherlands', 'Poland', 'Spain') THEN 'HBCE'
    WHEN SITE_NAME = 'Taiwan' THEN 'HBAP'
    WHEN SITE_NAME IN ('Bermuda (HUB)', 'Bermuda (Stand Alone)') THEN 'HBEU'
    ELSE CARM_INSTANCE
END AS CARM_REGION,
CARM_INSTANCE,
SITE_NAME,
GCDU_GLOBAL_ID,
RELATIONSHIP_ID,
CUSTOMER_ID,
CUSTOMER_NAME,
APPLICATION_STATUS,
APPROVAL_DATE,
CREDIT_SERIAL_NUMBER
"""

SELECT_STR_2: str = SELECT_STR

# PROC SQL: Create view IP_VIEW with union all across multiple inputs.
# TODO: Ensure all source tables are available in the Spark catalog.
spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW IP_VIEW AS
    SELECT {SELECT_STR} FROM {_INPUT1}
    UNION ALL
    SELECT {SELECT_STR} FROM {_INPUT2}
    UNION ALL
    SELECT {SELECT_STR} FROM {_INPUT3}
    UNION ALL
    SELECT {SELECT_STR} FROM {_INPUT4}
    UNION ALL
    SELECT {SELECT_STR} FROM {_INPUT5}
    UNION ALL
    SELECT {SELECT_STR} FROM {_INPUT7}
    UNION ALL
    SELECT {SELECT_STR} FROM {_INPUT6}
    UNION ALL
    SELECT {SELECT_STR_2} FROM {_INPUT8}
    UNION ALL
    SELECT {SELECT_STR} FROM {_INPUT9}
    UNION ALL
    SELECT {SELECT_STR} FROM {_INPUT10}
    UNION ALL
    SELECT {SELECT_STR} FROM {_INPUT11}
    UNION ALL
    SELECT {SELECT_STR} FROM {_INPUT12}
    """
)

# PROC SQL: Create INVOLVED_PARTY with distinct rows and additional columns
involved_party_sql: str = """
SELECT DISTINCT
    CARM_REGION,
    CARM_INSTANCE,
    SITE_NAME,
    GCDU_GLOBAL_ID,
    RELATIONSHIP_ID,
    CUSTOMER_ID,
    CUSTOMER_NAME,
    APPROVAL_DATE AS APPROVAL_DATE_ORIG,
    CREDIT_SERIAL_NUMBER AS CREDIT_SERIAL_NUMBER_ORIG,
    CAST(NULL AS DATE) AS GRADE_APP_EFFECTIVE_FROM,
    CAST(NULL AS DATE) AS GRADE_APP_EFFECTIVE_TO
FROM IP_VIEW
"""

INVOLVED_PARTY: DataFrame = spark.sql(involved_party_sql).orderBy(
    F.col("CARM_INSTANCE"), F.col("CUSTOMER_ID"), F.col("APPROVAL_DATE_ORIG").desc_nulls_last()
)

INVOLVED_PARTY.createOrReplaceTempView("INVOLVED_PARTY")

# DATA step: INVOLVED_PARTY_TS selecting first row per group after ordering and applying transformations
w_ip = Window.partitionBy("CARM_INSTANCE", "CUSTOMER_ID").orderBy(F.col("APPROVAL_DATE_ORIG").desc_nulls_last())
INVOLVED_PARTY_TS: DataFrame = (
    INVOLVED_PARTY.withColumn("rn", F.row_number().over(w_ip))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn(
        "CARM_INSTANCE",
        F.when(F.col("CARM_INSTANCE") == F.lit("CARM_BMS"), F.lit("CARM_BM")).otherwise(F.col("CARM_INSTANCE")),
    )
    .withColumn("APPROVAL_DATE", F.lit(None).cast("date"))
    .withColumn("CREDIT_SERIAL_NUMBER", F.lit(None).cast("string"))
)
INVOLVED_PARTY_TS.createOrReplaceTempView("INVOLVED_PARTY_TS")

# PROC SQL: Create carms table with filters
# TODO: Ensure RMADDREF.RMAD_D_REF_CARM_SITE_CODE is available in Spark catalog.
df_carm_site_code: DataFrame = spark.table("grade_global_d_ref.RMAD_D_REF_CARM_SITE_CODE") #RMADDREF is ok exists in hive
carms: DataFrame = (
    df_carm_site_code.where(F.substring(F.col("CARM_INSTANCE"), 1, 4) == F.lit("CARM"))
    .where((F.col("HIVE_ROOT_NAME").isNotNull()) & (F.length(F.trim(F.col("HIVE_ROOT_NAME"))) > 0))
    .where(F.col("CARM_INSTANCE") != F.lit("CARM_JA"))
    .select(
        "CARM_INSTANCE",
        "REGION",
        "REGIONAL_MART",
        "SITE_NAME",
        "SITE_ACRONYM",
        "ESR_SITE_CODE",
        "CARM_CURRENCY",
        "PATH",
        "REG",
        "RA",
        "MEP_PILOT_FLAG",
        "FINANCIALS_MODEL",
        "FINANCIALS_MODEL_DESCRIPTION",
        "MRA_FLAG",
        "DEV_ECON",
        "HIVE_ROOT_NAME",
    )
)
carms.createOrReplaceTempView("carms")

# Macro call to loop_instance
loop_instance()
print("Running loop_instance")

undercust_df = loop_instance()

print(" running")
# Save INVOLVED_PARTY_TS to the Hive database
print("Saving INVOLVED_PARTY_TS to Hive...")
spark.table("INVOLVED_PARTY_TS").write.mode("overwrite").saveAsTable("grade_pf_javier.local_id_matching_history_pre_processing_cas_v4_13_involved_party")

# Save undercust to the Hive database
if undercust_df is not None:
    print("Saving Undercust to Hive...")
    undercust_df.write.mode("overwrite").saveAsTable("grade_pf_javier.local_id_matching_history_pre_processing_cas_v4_13_undercust_results")

print(" Successfully saved both tables to Hive database 'grade_pf_javier'!")
