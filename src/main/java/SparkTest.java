import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.ArrayList;
import java.util.List;

public class SparkTest {

    public static void main(String[] args) {
        List<String> statements = new ArrayList<>();
        String sourceCode = "drop table if exists sbg_published.es_plus_sales ;\n" +
                "create table if not exists  sbg_published.es_plus_sales  as\n" +
                "\n" +
                "\n" +
                "with sales_prep as (\n" +
                "    select  *\n" +
                "         ,row_number() over (partition by asset_id order by sale_dt,bill_dt desc, bill_amt,LINE_SALES_ORDER_STS_DSC ) as seq_num\n" +
                "    from sbg_published.es_plus_fact_sale\n" +
                "\n" +
                "    WHERE    (MIGRATION_FLAG NOT IN ('DATA MIGRATION')\n" +
                "        AND   bill_line_item_type NOT IN ('/item/adjustment','/item/dispute','/item/settlement')\n" +
                "        AND SALE_ORDER_LINE_ACTION_TYP = 'ADD'\n" +
                "        AND ORDER_METD_CD='SALES ORDER'\n" +
                "        /*AND SALES_ORDER_STS_DSC='COMPLETE' OCT 24 2014*/\n" +
                "        /*AND LINE_SALES_ORDER_STS_DSC='COMPLETE' APR 3 2015 */\n" +
                "        /* ADDED LINE_SALES_ORDER_STS_DSC  BELOW ON APRIL 3 2015 --SEE PROC SORT BELOW */\n" +
                "        AND LINE_SALES_ORDER_STS_DSC NOT IN ('CANCELLED')\n" +
                "        /*AND SKU_MIGRATION_IND IS MISSING*/\n" +
                "        AND PRICE_TYPE_CD='Recurring'\n" +
                "        AND slbl_item_ind='Y'\n" +
                "\n" +
                "        AND INTU_ITEM_CD NOT IN ('1300051')\n" +
                "        )\n" +
                "       or\n" +
                "        (\n" +
                "\n" +
                "                    ORDER_METD_CD='RETAIL BILL ACTIVATION'\n" +
                "                AND MIGRATION_FLAG NOT IN ('DATA MIGRATION')\n" +
                "                AND LINE_SALES_ORDER_STS_DSC NOT IN ('CANCELLED')\n" +
                "                AND SALE_ORDER_LINE_ACTION_TYP not in ('DELETE','CANCEL','SUSPEND')\n" +
                "                AND bill_line_item_type NOT IN ('/item/adjustment','/item/dispute','/item/settlement')\n" +
                "                AND PRICE_TYPE_CD='Recurring'\n" +
                "                AND slbl_item_ind='Y'\n" +
                "                AND INTU_ITEM_CD NOT IN ('1300051')\n" +
                "\n" +
                "            )\n" +
                "    order by              asset_id\n" +
                "           ,sale_dt\n" +
                "           ,bill_dt desc\n" +
                "           ,bill_amt\n" +
                "           ,LINE_SALES_ORDER_STS_DSC\n" +
                ")\n" +
                "        ,\n" +
                "\n" +
                "     MIGRATIONS AS (\n" +
                "         select  *\n" +
                "              ,row_number() over (partition by asset_id order by sale_dt,bill_dt desc, bill_amt,LINE_SALES_ORDER_STS_DSC ) as seq_num\n" +
                "         from sbg_published.es_plus_fact_sale\n" +
                "\n" +
                "         WHERE MIGRATION_FLAG IN ('DATA MIGRATION')\n" +
                "\n" +
                "         order by              asset_id\n" +
                "                ,sale_dt\n" +
                "                ,bill_dt desc\n" +
                "                ,bill_amt\n" +
                "                ,LINE_SALES_ORDER_STS_DSC\n" +
                "\n" +
                "     )\n" +
                "        ,\n" +
                "\n" +
                "\n" +
                "     SALES_1 AS (\n" +
                "         select * from sales_prep\n" +
                "         where  seq_num=1\n" +
                "\n" +
                "         UNION ALL\n" +
                "\n" +
                "         SELECT * from MIGRATIONS\n" +
                "         where  seq_num=1\n" +
                "     )\n" +
                "        ,\n" +
                "\n" +
                "\n" +
                "\n" +
                "     MIGRATION AS (\n" +
                "\n" +
                "         SELECT\n" +
                "             FACT_ORDER_LINE.SALE_ORDER_NBR,\n" +
                "             DIM_PRODUCT_HIERARCHY.PROD_EDTN_CD,\n" +
                "             DIM_PRODUCT_HIERARCHY.PROD_EDTN_DSC,\n" +
                "             DIM_PRODUCT_HIERARCHY.PROD_EDTN_SHORT_NAM,\n" +
                "             DIM_PRODUCT.FREE_FLAG,\n" +
                "             PART_INTUIT_ITEM.ITEM_DSC\n" +
                "         FROM       ent_cv_dwh.FACT_ORDER_LINE inner join\n" +
                "                    ent_cv_dwh.DIM_ORDER_STATUS  CDMOWNER_DIM_ORDER_STATUSLn\n" +
                "                    on ( FACT_ORDER_LINE.SALE_ORDER_LINE_STS_KEY=CDMOWNER_DIM_ORDER_STATUSLn.DIM_ORDER_STATUS_KEY  )\n" +
                "                                               INNER JOIN\n" +
                "                    ent_cv_dwh.DIM_PRODUCT\n" +
                "                    ON (DIM_PRODUCT.DIM_PRODUCT_KEY=FACT_ORDER_LINE.PROD_KEY)\n" +
                "                                               inner join\n" +
                "                    ent_cv_dwh.DIM_PRODUCT_HIERARCHY\n" +
                "                    on  ( DIM_PRODUCT_HIERARCHY.DIM_PROD_HIER_KEY=FACT_ORDER_LINE.PROD_HIER_KEY  )\n" +
                "                                               inner join ent_cv_dwh.DIM_PRODUCT  PART_INTUIT_ITEM\n" +
                "                                                          on ( FACT_ORDER_LINE.PAR_PROD_KEY=PART_INTUIT_ITEM.DIM_PRODUCT_KEY  )\n" +
                "\n" +
                "                                               inner join\n" +
                "                    sales_1\n" +
                "                    on ( fact_order_line.sale_order_nbr=sales_1.sale_order_nbr\n" +
                "                        and sales_1.INTU_ITEM_CD IN  ('1099577'\n" +
                "                            ,'1101967'\n" +
                "                            ,'1300012'\n" +
                "                            ,'1300013'\n" +
                "                            ,'1300014'\n" +
                "                            ,'1300048'\n" +
                "                            ,'1300049'\n" +
                "                            ,'1300050'\n" +
                "                            ,'1300330'\n" +
                "                            ,'1300331'\n" +
                "                            ,'1300332'\n" +
                "                            ,'1300403'\n" +
                "                            ,'1400045','1400085'\n" +
                "                            )\n" +
                "                        and sales_1.sku_migration_ind is not null\n" +
                "                        )\n" +
                "\n" +
                "         WHERE\n" +
                "\n" +
                "             (\n" +
                "                         FACT_ORDER_LINE.SALE_ORDER_LINE_ACTION_TYP  =  'DELETE'\n" +
                "                     AND\n" +
                "                         CDMOWNER_DIM_ORDER_STATUSLn.SALES_ORDER_STS_DSC  =  'COMPLETE'\n" +
                "                     and PART_INTUIT_ITEM.ITEM_DSC is null\n" +
                "                     and upper(DIM_PRODUCT_HIERARCHY.PROD_EDTN_DSC) like '%PAYROLL%'\n" +
                "                     and to_date(fact_order_line.txtn_dt) >= '2014-10-06'\n" +
                "                 )\n" +
                "\n" +
                "\n" +
                "\n" +
                "     ) -- close migration\n" +
                "\n" +
                "        ,\n" +
                "\n" +
                "     migrations_from_payroll_prep  as (\n" +
                "\n" +
                "\n" +
                "         SELECT\n" +
                "             DIM_SALES_CHANNEL.SALES_CHNL_LEVEL_1_NAM,\n" +
                "             FACT_ORDER_LINE.CUST_ACCT_NBR,\n" +
                "             FACT_ORDER_LINE.SALE_ORDER_NBR,\n" +
                "             FACT_ORDER_LINE.ASSET_ID,\n" +
                "             CASE WHEN (FACT_ORDER_LINE.INTU_RETAIL_FLG = 'Y') THEN 'Retail' else DIM_SALES_CHANNEL.DRVD_SALES_CHNL_NAM end,\n" +
                "             DIM_PRODUCT_HIERARCHY.PROD_EDTN_SHORT_NAM,\n" +
                "             DIM_PRODUCT_HIERARCHY.PRODUCT_EDITION,\n" +
                "             DIM_PRODUCT_HIERARCHY.PROD_EDTN_DSC,\n" +
                "             DIM_PRODUCT_HIERARCHY.PROD_EDTN_CD,\n" +
                "             /*SUM(case when ( DIM_PRODUCT.PRICE_TYPE_CD )='Recurring' then FACT_ORDER_LINE.MIGRATION_CNT end),*/\n" +
                "             case when ( DIM_PRODUCT.PRICE_TYPE_CD )='Recurring' then FACT_ORDER_LINE.MIGRATION_CNT end,\n" +
                "             ORDER_TIME_BY_DAY.CLNDR_544_YEAR_NBR,\n" +
                "             ORDER_TIME_BY_DAY.CLNDR_544_WEEK_NBR,\n" +
                "             FACT_ORDER_LINE.TXTN_DT,\n" +
                "             ORDER_TIME_BY_DAY.CLNDR_544_MONTH_NBR,\n" +
                "             ORDER_TIME_BY_DAY.FISCAL_MONTH_NBR,\n" +
                "             DIM_PRODUCT.COA_GRP_LEVEL_0_CD,\n" +
                "             MIGRATION.PROD_EDTN_CD        AS PREVIOUS_PROD_EDTN_CD,\n" +
                "             MIGRATION.PROD_EDTN_DSC       AS PREVIOUS_PROD_EDTN_DSC,\n" +
                "             MIGRATION.PROD_EDTN_SHORT_NAM AS PREVIOUS_PROD_EDTN_SHORT_NAM,\n" +
                "             DIM_PRODUCT_HIERARCHY.DRVD_USERS,\n" +
                "\n" +
                "             row_number() over (partition by FACT_ORDER_LINE.asset_id order by FACT_ORDER_LINE.txtn_dt ) as seq_num\n" +
                "\n" +
                "         FROM\n" +
                "             ent_cv_dwh.FACT_ORDER_LINE     INNER JOIN\n" +
                "             ent_cv_dwh.DIM_SALES_CHANNEL\n" +
                "             ON ( DIM_SALES_CHANNEL.DIM_SALES_CHNL_KEY=FACT_ORDER_LINE.SALES_CHNL_KEY  ) INNER JOIN\n" +
                "\n" +
                "             ent_cv_dwh.DIM_PRODUCT_HIERARCHY\n" +
                "             ON ( DIM_PRODUCT_HIERARCHY.DIM_PROD_HIER_KEY=FACT_ORDER_LINE.PROD_HIER_KEY  ) INNER JOIN\n" +
                "\n" +
                "             ent_cv_dwh.DIM_PRODUCT\n" +
                "             ON ( DIM_PRODUCT.DIM_PRODUCT_KEY=FACT_ORDER_LINE.PROD_KEY  ) INNER JOIN\n" +
                "\n" +
                "\n" +
                "             ent_cv_dwh.DIM_TIME_BY_DAY  ORDER_TIME_BY_DAY\n" +
                "             ON ( FACT_ORDER_LINE.TXTN_DT_KEY=ORDER_TIME_BY_DAY.DIM_TIME_BY_DAY_KEY  ) INNER JOIN\n" +
                "\n" +
                "             MIGRATION\n" +
                "             ON ( MIGRATION.SALE_ORDER_NBR=FACT_ORDER_LINE.SALE_ORDER_NBR  ) INNER JOIN\n" +
                "\n" +
                "\n" +
                "             ent_cv_dwh.DIM_ORDER_STATUS  CDMOWNER_DIM_ORDER_STATUSLn\n" +
                "             ON ( FACT_ORDER_LINE.SALE_ORDER_LINE_STS_KEY=CDMOWNER_DIM_ORDER_STATUSLn.DIM_ORDER_STATUS_KEY  ) INNER JOIN\n" +
                "             ent_cv_dwh.DIM_ORDER_TYPES\n" +
                "             ON ( FACT_ORDER_LINE.SALE_ORDER_HDR_TYP_KEY=DIM_ORDER_TYPES.DIM_ORDER_TYPE_KEY  ) LEFT OUTER JOIN\n" +
                "\n" +
                "             ent_cv_dwh.DIM_PRODUCT  PART_INTUIT_ITEM\n" +
                "             ON  ( FACT_ORDER_LINE.PAR_PROD_KEY=PART_INTUIT_ITEM.DIM_PRODUCT_KEY)\n" +
                "\n" +
                "         WHERE\n" +
                "             (\n" +
                "\n" +
                "                     (\n" +
                "                             to_date(ORDER_TIME_BY_DAY.CLNDR_DT) >= '2014-10-06'\n" +
                "                         )\n" +
                "                     AND\n" +
                "                     DIM_PRODUCT.COA_GRP_LEVEL_0_CD  IN  ( '054'  )\n" +
                "                     AND\n" +
                "                     DIM_PRODUCT_HIERARCHY.INTU_ITEM_CD IN  ('1099577'\n" +
                "                         ,'1101967'\n" +
                "                         ,'1300012'\n" +
                "                         ,'1300013'\n" +
                "                         ,'1300014'\n" +
                "                         ,'1300048'\n" +
                "                         ,'1300049'\n" +
                "                         ,'1300050'\n" +
                "                         ,'1300330'\n" +
                "                         ,'1300331'\n" +
                "                         ,'1300332'\n" +
                "                         ,'1300403'\n" +
                "                         ,'1400045','1400085'\n" +
                "                         )\n" +
                "                     AND\n" +
                "                     DIM_PRODUCT.INTU_ITEM_CD  NOT IN  ( '1099621','1099639','1099624'  )\n" +
                "                     AND\n" +
                "                     DIM_PRODUCT_HIERARCHY.PROD_EDTN_CD  NOT IN  ( '00001493','00001494','00001495','00001496'  )\n" +
                "                     AND\n" +
                "                     FACT_ORDER_LINE.SALE_ORDER_LINE_ACTION_TYP  =  'ADD'\n" +
                "                     AND\n" +
                "                     CDMOWNER_DIM_ORDER_STATUSLn.SALES_ORDER_STS_DSC  IN  ( 'COMPLETE'  )\n" +
                "                     AND\n" +
                "                     DIM_ORDER_TYPES.SALE_ORDER_TYP_NAM  NOT IN  ( 'Registration'  )\n" +
                "                     AND\n" +
                "                     ( FACT_ORDER_LINE.MIGRATION_CNT=1  )\n" +
                "                     AND\n" +
                "                     DIM_PRODUCT.PRICE_TYPE_CD  =  'Recurring'\n" +
                "                     AND\n" +
                "                     PART_INTUIT_ITEM.ITEM_DSC  Is null\n" +
                "                 )\n" +
                "\n" +
                "\n" +
                "\n" +
                "     ) -- close table migrations_from_payroll\n" +
                "\n" +
                "\n" +
                "        ,\n" +
                "\n" +
                "     migrations_from_payroll  as (\n" +
                "\n" +
                "         select *\n" +
                "         from     migrations_from_payroll_prep\n" +
                "         where    seq_num=1\n" +
                "\n" +
                "\n" +
                "     ) -- close migrations from payroll\n" +
                "\n" +
                "        ,\n" +
                "\n" +
                "     es_plus_sales as  (\n" +
                "\n" +
                "         select  a.*\n" +
                "              ,case when b.asset_id not in ('') then 'Y'\n" +
                "                    else 'N'\n" +
                "             end  as PAYROLL_MIGRATION\n" +
                "         from     sales_1 a\n" +
                "                      left outer join\n" +
                "                  migrations_from_payroll b\n" +
                "                  on      ( a.asset_id=b.asset_id\n" +
                "                      and   a.sale_order_nbr=b.sale_order_nbr\n" +
                "                      and   a.intu_item_cd in ('1099577'\n" +
                "                          ,'1101967'\n" +
                "                          ,'1300002'\n" +
                "                          ,'1300012'\n" +
                "                          ,'1300013'\n" +
                "                          ,'1300014'\n" +
                "                          ,'1300048'\n" +
                "                          ,'1300049'\n" +
                "                          ,'1300050'\n" +
                "                          ,'1300330'\n" +
                "                          ,'1300331'\n" +
                "                          ,'1300332'\n" +
                "                          ,'1300403'\n" +
                "                          ,'1400045','1400085'\n" +
                "                          )\n" +
                "                      )\n" +
                "\n" +
                "\n" +
                "\n" +
                "     ) -- close esplus sales\n" +
                "\n" +
                "SELECT salestb.*\n" +
                "     ,date(from_unixtime(unix_timestamp())) as update_date\n" +
                "FROM   es_plus_SALES salestb\n" +
                ";".toUpperCase();
        ANTLRInputStream input = new ANTLRInputStream(sourceCode);
        SqlBaseLexer lexer = new SqlBaseLexer(input);
        SqlBaseParser sqlBaseParser = new SqlBaseParser(new CommonTokenStream(lexer));
        SparkListener listener = new SparkListener(statements);
        ParseTreeWalker.DEFAULT.walk(listener, sqlBaseParser.singleStatement());
        for(String state: statements){
            System.out.println(state);
        }
    }
}
