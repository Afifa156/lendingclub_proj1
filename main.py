import sys
from lib import DataManipulation, DataReader, Utils, logger,CreateSubDF, DataWrite
from pyspark.sql.functions import *
from lib.logger import Log4j

if __name__ == '__main__':

    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)

    job_run_env = sys.argv[1]

    # print("Creating Spark Session")

    spark = Utils.get_spark_session(job_run_env)

    logger = Log4j(spark)

    logger.info("Created Spark Session")

    # raw_df = DataReader.read_original(spark,job_run_env)
    # preprocessed = DataManipulation.pre_process_clean(raw_df)
    
    # logger.info("preprocessed is complete")
    
    
    # customer_df = CreateSubDF.create_Customerdf(spark,preprocessed)
    # DataWrite.write_dataframe(spark,job_run_env,customer_df,"raw_customer.file.path")
    
    # loan_df = CreateSubDF.create_Loandf(spark,preprocessed)
    # DataWrite.write_dataframe(spark,job_run_env,loan_df,"raw_loan.file.path")
    
    # loan_repayment_df = CreateSubDF.create_LoanRepaydf(spark,preprocessed)
    # DataWrite.write_dataframe(spark,job_run_env,loan_repayment_df,"raw_loanrepay.file.path")
    
    # loan_defaulter = CreateSubDF.create_LoanDefaulterdf(spark,preprocessed)
    # DataWrite.write_dataframe(spark,job_run_env,loan_defaulter,"raw_loandefault.file.path")
    
    ##processing customer_data only
    customer_schema = 'member_id string, emp_title string, emp_length string, home_ownership string,annual_inc float, addr_state string,zip_code string,country string,grade string,sub_grade string, verification_status string, tot_hi_cred_lim float, application_type string, annual_inc_joint float,verification_status_joint string'
    customer_df = DataReader.read_csv(spark,job_run_env,customer_schema,"raw_customer.file.path")
    customer_df_cleaned = DataManipulation.customer_data_clean(spark,customer_df)
    DataWrite.write_dataframe(spark,job_run_env,customer_df_cleaned,"cleaned_customer.file.path")
    
    #print(aggregated_results.collect())

    logger.info("this is the end of main")