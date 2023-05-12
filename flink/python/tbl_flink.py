from pyflink.table import TableEnvironment
from pyflink.table import EnvironmentSettings
from pyflink.table import DataTypes
from pyflink.table import CsvTableSource


def main():
    print("My first Apache Flink app")

    env_settings = EnvironmentSettings.new_instance()\
                                      .in_batch_mode()\
                                      .build()
    tbl_env = TableEnvironment.create(env_settings)

    # Using PyFlink Table API -> Method #2 : Using Python Syntax
    # column_names = ['trx_id', 'trx_date', 'src_curr', 'amnt_src_curr', 
    #                 'amnt_gbp', 'user_id', 'user_type', 'user_country']
    
    # column_types = [DataTypes.INT(), DataTypes.DATE(), DataTypes.STRING(), DataTypes.DOUBLE(), 
    #                 DataTypes.DOUBLE(), DataTypes.INT(),DataTypes.STRING(), DataTypes.STRING()]
    
    # source = CsvTableSource('./fin_trxs.csv', column_names, column_types, ignore_first_line=False)

    # tbl_env.register_table_source('financial_trxs', source)
    # tbl = tbl_env.from_path('financial_trxs')

    # #######################################33
    # print('\nRegistered Tables List')
    # print(tbl_env.list_tables())

    # print('\nFinancial Trxs Schema')
    # tbl.print_schema()

    # print('\nFinancial Trxs Data')
    # #print(tbl.to_pandas())
    # tbl.limit(10).execute().print()

    # Using PyFlink Table API -> Method #2 : Using SQL Syntax
    source_path = './fin_trxs.csv'

    source_ddl = f"""
                create table financial_trxs_2 (
                trx_id BIGINT,
                trx_date DATE,
                src_curr VARCHAR,
                amnt_src_curr FLOAT,
                amnt_gbp FLOAT,
                user_id BIGINT,
                user_type VARCHAR,
                user_country VARCHAR
                ) 
                with (
                'connector' = 'filesystem',
                'format' = 'csv',
                'path' = '{source_path}'
                )
                """
    tbl_env.execute_sql(source_ddl)
    tbl = tbl_env.from_path("financial_trxs_2")
    #tbl.limit(10).execute().print()

    col = tbl.select(tbl.trx_id)
    col.limit(10).execute().print()


if __name__ == '__main__' :
    main()