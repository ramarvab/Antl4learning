import org.apache.hadoop.hive.ql.parse.ParseException;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;

public class test3 {
    public static void main(String[] args) throws ParseException {
        test1 t= new test1();
       List <String> statements = new ArrayList<>();
        String sql =
        "\n" +
                "/*\n" +
                "Start work on owner mode dashboard. The same logic will be used for the the FTU experiment afterwards.\n" +
                "Things we discussed and want to cover:\n" +
                "--FTU Interview Questions:\n" +
                "--Selection\n" +
                "--Question Completion Rate\n" +
                "-- Land on Homepage Rate\n" +
                "-- Conversion Rate\n" +
                "Product Behavior Metric Sliced by Answers\n" +
                "-- “Standard” MIMO tasks\n" +
                "-- First 5 tasks completed\n" +
                "Time to first task completed\n" +
                "?Help articles views\n" +
                "?Care calls\n" +
                "Global create interaction\n" +
                "First 5 tasks attempted (ie task abandonment rate)\n" +
                "Other Ideas(optional):\n" +
                "RCA for customers who “skip” FTU and land on dashboard\n" +
                "Bank connection & TXN categorization\n" +
                "Page duration\n" +
                "First X page visited\n" +
                "-- Number of tasks/distinct tasks within the first week\n" +
                "Set up guide interaction\n" +
                "Chart of account interaction\n" +
                "Add New users?\n" +
                "Invite accountants?\n" +
                "Note: replace '2019-11-01' with whatever start date of the query\n" +
                "Owner mode traffic:\n" +
                "No appstore, only buynow & trial, new to the franchise, essential/plus/simple start, US only\n" +
                "Weekly traffic 16,000\n" +
                "Assume 43% to 44.5%, Significance level 5%, power 80%\n" +
                "Minimum sample size per bucket: 17,200\n" +
                "15% : 85% split - Control 2,400 & Test 13,600 = 7.2 week\n" +
                "20% : 80% split - Control 3,200 & Test 12,800 = 5.4 week\n" +
                "25% : 75% split - Control 4,000 & Test 12,000 = 4.3 week\n" +
                "Assume 43% to 44%, Significance level 5%, power 80%\n" +
                "Minimum sample size per bucket: 38,600\n" +
                "15% : 85% split - Control 2,400 & Test 13,600 = 16.1 week\n" +
                "20% : 80% split - Control 3,200 & Test 12,800 = 12.1 week\n" +
                "25% : 75% split - Control 4,000 & Test 12,000 = 9.7 week\n" +
                "*/\n" +
                "------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                "--QUERIES ALL IN VERTICA\n" +
                "------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                "\n" +
                "------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                "--getting company base ready\n" +
                "------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                "\n" +
                "TRUNCATE TABLE sbg_sandbox_care.rethink_acct_base;\n" +
                "\n" +
                "INSERT  INTO sbg_sandbox_care.rethink_acct_base\n" +
                "SELECT *\n" +
                "FROM sbg_stable.qbo_company_status\n" +
                "WHERE 1=1\n" +
                "AND qbo_country = 'United States'\n" +
                "--AND QBO_SIGNUP_TYPE_DESC IN ('Trial','Buy Now')\n" +
                "--AND qbo_current_product != 'Advanced'\n" +
                "--AND qbo_migrator_type_description = 'New to the Franchise'\n" +
                "AND qbo_signup_date >= '2019-11-01';\n" +
                "------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                "--getting audit log data ready, may need to work on daily insert\n" +
                "------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                "\n" +
                "TRUNCATE TABLE sbg_sandbox_care.audit_data_rethink_acct_share;\n" +
                "--DROP TABLE IF EXISTS sbg_sandbox_care.audit_data_rethink_acct_share;\n" +
                "INSERT  INTO sbg_sandbox_care.audit_data_rethink_acct_share\n" +
                "--CREATE TABLE IF NOT EXISTS sbg_sandbox_care.audit_data_rethink_acct_share AS\n" +
                "SELECT a.*\n" +
                "FROM SBG_SOURCE.SRC_QBO_COMBINED_AUDITINFO_VW a\n" +
                "INNER JOIN sbg_sandbox_care.rethink_acct_base b ON a.company_id = b.qbo_company_id\n" +
                "WHERE audit_date >= '2019-11-01'\n" +
                "      AND user_id >= 100;\n" +
                "\n" +
                "------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                "--Start work on first 20 task completed\n" +
                "--NOTE: This is at company level. We may need to determine if we want to see this at user or company level\n" +
                "------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                "\n" +
                "TRUNCATE TABLE sbg_sandbox_care.audit_data_first_tasks;\n" +
                "INSERT INTO sbg_sandbox_care.audit_data_first_tasks\n" +
                "WITH\n" +
                "first_audit_filter AS\n" +
                "(\n" +
                "SELECT  company_id,\n" +
                "        --user_id,\n" +
                "        action_type_id,\n" +
                "        list_type_id,\n" +
                "        tx_type_id,\n" +
                "        COALESCE(CAST(action_type_id AS VARCHAR), ' ') || '-'\n" +
                "        || COALESCE(CAST(list_type_id   AS VARCHAR), ' ') || '-'\n" +
                "        || COALESCE(CAST(tx_type_id AS VARCHAR), ' ') as txn_concat,\n" +
                "        audit_date_time\n" +
                "FROM sbg_sandbox_care.audit_data_rethink_acct_share a\n" +
                "INNER JOIN sbg_sandbox_care.rethink_acct_base base ON a.company_id=base.qbo_company_id\n" +
                "                                                 AND (date(audit_date) BETWEEN qbo_signup_date AND qbo_signup_date + 6)\n" +
                "),\n" +
                "\n" +
                "lag_applied AS\n" +
                "(\n" +
                "  SELECT company_id,\n" +
                "         --user_id,\n" +
                "         --LAG(txn_concat,1) OVER (PARTITION BY company_id, user_id ORDER BY audit_date_time) as previous_action,\n" +
                "         LAG(txn_concat,1) OVER (PARTITION BY company_id ORDER BY audit_date_time) as previous_action,\n" +
                "         action_type_id,\n" +
                "         list_type_id,\n" +
                "         tx_type_id,\n" +
                "         txn_concat,\n" +
                "         audit_date_time\n" +
                "  FROM first_audit_filter\n" +
                "  WHERE txn_concat not in ('3-1- ','3-2- ','3-15- ','3-16- ','3-9- ','1- - ','2- - ','86- - ', '16- - ','98-9- ','99-9- ','20- - ','4-9- ','87- - ','36- - ')\n" +
                "  --remove add account,remove add payment method,remove add terms, remove added_user, login, logout, company_info_completed, preference changed, inactivate user, reactivate user, Sales Customization Changed, edit user, Personalization Completed, Online Banking connection set up\n" +
                "),\n" +
                "/* --why we remove events, when I first signed up things were added under my name. Not considered user initiated action.\n" +
                "Janice Shieh\tQuickBooks setup and personalization complete!\n" +
                "Janice Shieh\tSales customization changed\n" +
                "Janice Shieh\tSettings changed\n" +
                "Janice Shieh\tCompleted Company Information\n" +
                "Janice Shieh\tLogged in.\n" +
                "Janice Shieh\tAdded Product or Service: Sales\t\t\t\tView\n" +
                "Janice Shieh\tAdded Product or Service: Hours\t\t\t\tView\n" +
                "Janice Shieh\tAdded Product or Service: Sales\t\t\t\tView\n" +
                "Janice Shieh\tAdded Account: Sales\t\t\t\tView\n" +
                "Janice Shieh\tAdded Account: Sales\t\t\t\tView\n" +
                "Janice Shieh\tAdded Payment Method: Credit Card\t\t\t\tView\n" +
                "Janice Shieh\tAdded Payment Method: Check\t\t\t\tView\n" +
                "Janice Shieh\tAdded Payment Method: Cash\t\t\t\tView\n" +
                "System Administration\tAdded Account: Retained Earnings\t\t\t\tView\n" +
                "System Administration\tAdded Account: Owner's Investment\t\t\t\tView\n" +
                "System Administration\tAdded Account: Owner's Pay & Personal Expenses\t\t\t\tView\n" +
                "System Administration\tAdded Account: Ask My Accountant\t\t\t\tView\n" +
                "System Administration\tAdded Account: Utilities\t\t\t\tView\n" +
                "System Administration\tAdded Account: Travel\t\t\t\tView\n" +
                "System Administration\tAdded Account: Taxes & Licenses\t\t\t\tView\n" +
                "System Administration\tAdded Account: Job Supplies\t\t\t\tView\n" +
                "System Administration\tAdded Account: Repairs & Maintenance\t\t\t\tView\n" +
                "System Administration\tAdded Account: Rent & Lease\t\t\t\tView\n" +
                "System Administration\tAdded Account: Reimbursable Expenses\t\t\t\tView\n" +
                "System Administration\tAdded Account: Office Supplies & Software\t\t\t\tView\n" +
                "System Administration\tAdded Account: Other Business Expenses\t\t\t\tView\n" +
                "System Administration\tAdded Account: Meals & Entertainment\t\t\t\tView\n" +
                "System Administration\tAdded Account: Legal & Professional Services\t\t\t\tView\n" +
                "System Administration\tAdded Account: Interest Paid\t\t\t\tView\n" +
                "System Administration\tAdded Account: Insurance\t\t\t\tView\n" +
                "System Administration\tAdded Account: Contractors\t\t\t\tView\n" +
                "System Administration\tAdded Account: Bank Charges & Fees\t\t\t\tView\n" +
                "System Administration\tAdded Account: Car & Truck\t\t\t\tView\n" +
                "System Administration\tAdded Account: Advertising & Marketing\t\t\t\tView\n" +
                "Janice Shieh\tAdded Term: Net 60\t\t\t\tView\n" +
                "Janice Shieh\tAdded Term: Net 30\t\t\t\tView\n" +
                "System Administration\tAdded Account: Uncategorized Expense\t\t\t\tView\n" +
                "Janice Shieh\tAdded Term: Net 15\t\t\t\tView\n" +
                "System Administration\tAdded Account: Uncategorized Income\t\t\t\tView\n" +
                "Janice Shieh\tAdded Term: Due on receipt\t\t\t\tView\n" +
                "System Administration\tAdded Account: Uncategorized Asset\t\t\t\tView\n" +
                "Janice Shieh\tSales customization changed\n" +
                "Janice Shieh\tSales customization changed\n" +
                "Janice Shieh\tAdded User: Janice Shieh\t\t\t\tView\n" +
                "*/\n" +
                "\n" +
                "add_desc_remove_repeat AS\n" +
                "(\n" +
                "--no timestamp regulation yet, will add 14 days\n" +
                "SELECT\n" +
                "       lag_applied.company_id,\n" +
                "       --lag_applied.user_id,\n" +
                "       lag_applied.previous_action,\n" +
                "       lag_applied.action_type_id,\n" +
                "       lag_applied.list_type_id,\n" +
                "       lag_applied.tx_type_id,\n" +
                "       lag_applied.txn_concat,\n" +
                "       lag_applied.audit_date_time,\n" +
                "       --ROW_NUMBER() OVER (PARTITION BY company_id, user_id ORDER BY audit_date_time ASC) AS action_order,\n" +
                "       ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY audit_date_time ASC) AS action_order,\n" +
                "       COALESCE(actype.enum_value,' ') || ' ' || COALESCE(listtype.enum_value,' ') || ' ' || COALESCE(trtype.enum_value,' ') as action_Desc\n" +
                "\n" +
                "FROM lag_applied\n" +
                "LEFT JOIN SBG_SOURCE.src_qbo_enum_lookup trtype   ON trtype.enum_key   = lag_applied.tx_type_id      AND trtype.table_name = 'auditinfo_vw'   AND trtype.column_name='tx_type_id'\n" +
                "LEFT JOIN SBG_SOURCE.src_qbo_enum_lookup actype   ON actype.enum_key   = lag_applied.action_type_id  AND actype.table_name = 'auditinfo_vw'   AND actype.column_name = 'action_type_id'\n" +
                "LEFT JOIN SBG_SOURCE.src_qbo_enum_lookup listtype ON listtype.enum_key = lag_applied.list_type_id    AND listtype.table_name = 'auditinfo_vw' AND listtype.column_name ='list_type_id'\n" +
                "WHERE previous_action IS NULL OR (previous_action !=txn_concat)\n" +
                "),\n" +
                "\n" +
                "pivot_data AS\n" +
                "(\n" +
                "\n" +
                "SELECT\n" +
                "       company_id,\n" +
                "       --user_id,\n" +
                "       MAX(CASE WHEN action_order=1  THEN action_desc ELSE NULL END) AS action_order_1 ,\n" +
                "       MAX(CASE WHEN action_order=2  THEN action_desc ELSE NULL END) AS action_order_2 ,\n" +
                "       MAX(CASE WHEN action_order=3  THEN action_desc ELSE NULL END) AS action_order_3 ,\n" +
                "       MAX(CASE WHEN action_order=4  THEN action_desc ELSE NULL END) AS action_order_4 ,\n" +
                "       MAX(CASE WHEN action_order=5  THEN action_desc ELSE NULL END) AS action_order_5 ,\n" +
                "       MAX(CASE WHEN action_order=6  THEN action_desc ELSE NULL END) AS action_order_6 ,\n" +
                "       MAX(CASE WHEN action_order=7  THEN action_desc ELSE NULL END) AS action_order_7 ,\n" +
                "       MAX(CASE WHEN action_order=8  THEN action_desc ELSE NULL END) AS action_order_8 ,\n" +
                "       MAX(CASE WHEN action_order=9  THEN action_desc ELSE NULL END) AS action_order_9 ,\n" +
                "       MAX(CASE WHEN action_order=10 THEN action_desc ELSE NULL END) AS action_order_10,\n" +
                "       MAX(CASE WHEN action_order=11 THEN action_desc ELSE NULL END) AS action_order_11,\n" +
                "       MAX(CASE WHEN action_order=12 THEN action_desc ELSE NULL END) AS action_order_12,\n" +
                "       MAX(CASE WHEN action_order=13 THEN action_desc ELSE NULL END) AS action_order_13,\n" +
                "       MAX(CASE WHEN action_order=14 THEN action_desc ELSE NULL END) AS action_order_14,\n" +
                "       MAX(CASE WHEN action_order=15 THEN action_desc ELSE NULL END) AS action_order_15,\n" +
                "       MAX(CASE WHEN action_order=16 THEN action_desc ELSE NULL END) AS action_order_16,\n" +
                "       MAX(CASE WHEN action_order=17 THEN action_desc ELSE NULL END) AS action_order_17,\n" +
                "       MAX(CASE WHEN action_order=18 THEN action_desc ELSE NULL END) AS action_order_18,\n" +
                "       MAX(CASE WHEN action_order=19 THEN action_desc ELSE NULL END) AS action_order_19,\n" +
                "       MAX(CASE WHEN action_order=20 THEN action_desc ELSE NULL END) AS action_order_20,\n" +
                "\n" +
                "       MAX(CASE WHEN action_order=1  THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_1 ,\n" +
                "       MAX(CASE WHEN action_order=2  THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_2 ,\n" +
                "       MAX(CASE WHEN action_order=3  THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_3 ,\n" +
                "       MAX(CASE WHEN action_order=4  THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_4 ,\n" +
                "       MAX(CASE WHEN action_order=5  THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_5 ,\n" +
                "       MAX(CASE WHEN action_order=6  THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_6 ,\n" +
                "       MAX(CASE WHEN action_order=7  THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_7 ,\n" +
                "       MAX(CASE WHEN action_order=8  THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_8 ,\n" +
                "       MAX(CASE WHEN action_order=9  THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_9 ,\n" +
                "       MAX(CASE WHEN action_order=10 THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_10,\n" +
                "       MAX(CASE WHEN action_order=11 THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_11,\n" +
                "       MAX(CASE WHEN action_order=12 THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_12,\n" +
                "       MAX(CASE WHEN action_order=13 THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_13,\n" +
                "       MAX(CASE WHEN action_order=14 THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_14,\n" +
                "       MAX(CASE WHEN action_order=15 THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_15,\n" +
                "       MAX(CASE WHEN action_order=16 THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_16,\n" +
                "       MAX(CASE WHEN action_order=17 THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_17,\n" +
                "       MAX(CASE WHEN action_order=18 THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_18,\n" +
                "       MAX(CASE WHEN action_order=19 THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_19,\n" +
                "       MAX(CASE WHEN action_order=20 THEN CONCAT(action_desc,txn_concat) ELSE NULL END) AS full_action_order_20,\n" +
                "--action order\n" +
                "       MAX(CASE WHEN action_order=1  THEN audit_date_time                ELSE NULL END) AS action_order_ts_1 ,\n" +
                "       MAX(CASE WHEN action_order=2  THEN audit_date_time                ELSE NULL END) AS action_order_ts_2 ,\n" +
                "       MAX(CASE WHEN action_order=3  THEN audit_date_time                ELSE NULL END) AS action_order_ts_3 ,\n" +
                "       MAX(CASE WHEN action_order=4  THEN audit_date_time                ELSE NULL END) AS action_order_ts_4 ,\n" +
                "       MAX(CASE WHEN action_order=5  THEN audit_date_time                ELSE NULL END) AS action_order_ts_5 ,\n" +
                "       MAX(CASE WHEN action_order=6  THEN audit_date_time                ELSE NULL END) AS action_order_ts_6 ,\n" +
                "       MAX(CASE WHEN action_order=7  THEN audit_date_time                ELSE NULL END) AS action_order_ts_7 ,\n" +
                "       MAX(CASE WHEN action_order=8  THEN audit_date_time                ELSE NULL END) AS action_order_ts_8 ,\n" +
                "       MAX(CASE WHEN action_order=9  THEN audit_date_time                ELSE NULL END) AS action_order_ts_9 ,\n" +
                "       MAX(CASE WHEN action_order=10 THEN audit_date_time                ELSE NULL END) AS action_order_ts_10,\n" +
                "       MAX(CASE WHEN action_order=11 THEN audit_date_time                ELSE NULL END) AS action_order_ts_11,\n" +
                "       MAX(CASE WHEN action_order=12 THEN audit_date_time                ELSE NULL END) AS action_order_ts_12,\n" +
                "       MAX(CASE WHEN action_order=13 THEN audit_date_time                ELSE NULL END) AS action_order_ts_13,\n" +
                "       MAX(CASE WHEN action_order=14 THEN audit_date_time                ELSE NULL END) AS action_order_ts_14,\n" +
                "       MAX(CASE WHEN action_order=15 THEN audit_date_time                ELSE NULL END) AS action_order_ts_15,\n" +
                "       MAX(CASE WHEN action_order=16 THEN audit_date_time                ELSE NULL END) AS action_order_ts_16,\n" +
                "       MAX(CASE WHEN action_order=17 THEN audit_date_time                ELSE NULL END) AS action_order_ts_17,\n" +
                "       MAX(CASE WHEN action_order=18 THEN audit_date_time                ELSE NULL END) AS action_order_ts_18,\n" +
                "       MAX(CASE WHEN action_order=19 THEN audit_date_time                ELSE NULL END) AS action_order_ts_19,\n" +
                "       MAX(CASE WHEN action_order=20 THEN audit_date_time                ELSE NULL END) AS action_order_ts_20,\n" +
                "--action order and its timestamp\n" +
                "\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%invoice%'           THEN 1 ELSE 0 END) AS any_invoice_flag,\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%generic_expense%'   THEN 1 ELSE 0 END) AS any_generic_expense_flag,\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%customers%'         THEN 1 ELSE 0 END) AS any_customers_flag,\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%vendors%'           THEN 1 ELSE 0 END) AS any_vendors_flag,\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%employees%'         THEN 1 ELSE 0 END) AS any_employees_flag,\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%time_activiti%'     THEN 1 ELSE 0 END) AS any_time_activities_flag,\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%estimate%'          THEN 1 ELSE 0 END) AS any_estimate_flag,\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%payroll_check%'     THEN 1 ELSE 0 END) AS any_payroll_check_flag,\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%general_journal%'   THEN 1 ELSE 0 END) AS any_general_journal_flag,\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%items%'             THEN 1 ELSE 0 END) AS any_items_flag,\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%budgets%'           THEN 1 ELSE 0 END) AS any_budgets_flag,\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%import%' AND LOWER(action_desc) NOT LIKE '%olb_csv_import_upload%' THEN 1 ELSE 0 END) AS any_import_flag,\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%received_payment%'  THEN 1 ELSE 0 END) AS any_received_payment_flag,\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%tax%'               THEN 1 ELSE 0 END) AS any_tax_flag,\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%accounts%'          THEN 1 ELSE 0 END) AS any_accounts_flag,\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%bill%'              THEN 1 ELSE 0 END) AS any_bill_flag,\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%inventory%'         THEN 1 ELSE 0 END) AS any_inventory_flag,\n" +
                "       MAX(CASE WHEN LOWER(action_desc) LIKE '%sale%'              THEN 1 ELSE 0 END) AS any_sell_flag\n" +
                "FROM add_desc_remove_repeat\n" +
                "WHERE action_order<=20\n" +
                "GROUP BY company_id\n" +
                "         --,user_id\n" +
                ")\n" +
                "SELECT *\n" +
                "FROM pivot_data;\n" +
                "--Drop TABLE If EXISTS sbg_sandbox_care.tracking_ftu_role_base_b;\n" +
                "TRUNCATE TABLE sbg_sandbox_care.tracking_ftu_role_base_b;\n" +
                "INSERT  INTO sbg_sandbox_care.tracking_ftu_role_base_b\n" +
                "--CREATE TABLE IF NOT EXISTS sbg_sandbox_care.tracking_ftu_role_base_b AS\n" +
                "WITH preprocess AS\n" +
                "(\n" +
                "SELECT *,\n" +
                "       CAST(MAPLOOKUP(MapJSONExtractor(event_properties), 'business_persona')          AS VARCHAR) AS biz_persona, --you have to cast as carchar or LIKE function won't work\n" +
                "       COALESCE(CAST(MAPLOOKUP(MapJSONExtractor(event_properties), 'assistance_preference:') AS VARCHAR),CAST(MAPLOOKUP(MapJSONExtractor(event_properties), 'assistance_preference') AS VARCHAR)) AS assist_pref,\n" +
                "       CAST(MAPLOOKUP(MapJSONExtractor(event_properties), 'how_you_make_money')        AS VARCHAR) as How_make_money,\n" +
                "       CAST(MAPLOOKUP(MapJSONExtractor(event_properties), 'how_you_make_money_other')  AS VARCHAR) as How_make_money_other,\n" +
                "       CAST(MAPLOOKUP(MapJSONExtractor(event_properties), 'business_entity')           AS VARCHAR) as what_biz_entity\n" +
                "FROM thrive_dwh.sbseg_clickstream_web as cl\n" +
                "WHERE 1=1\n" +
                "AND DATE(cl.event_timestamp) > '2019-11-01'\n" +
                "AND event IN ('ftu step 1: started','ftu step 1: finished',\n" +
                "              'ftu step 2: started','ftu step 2: finished',\n" +
                "              'ftu step 3: started','ftu step 3: finished',\n" +
                "              'ftu step 4: started','ftu step 4: finished',\n" +
                "              'ftu step 5: started','ftu step 5: finished',\n" +
                "              'preference: updated',\n" +
                "              'user view: saved',\n" +
                "              'user view: edit started',\n" +
                "              'homepage: viewed'\n" +
                "              )\n" +
                ")\n" +
                "\n" +
                "SELECT\n" +
                "      *,\n" +
                "       CASE WHEN (event = 'ftu step 3: finished' OR event = 'ftu step 2: finished') AND (object_detail LIKE '%owner%'      OR biz_persona LIKE '%owner%'     ) THEN 'Owner'\n" +
                "            WHEN (event = 'ftu step 3: finished' OR event = 'ftu step 2: finished') AND (object_detail LIKE '%employee%'   OR biz_persona LIKE '%employee%'  ) THEN 'Employee'\n" +
                "            WHEN (event = 'ftu step 3: finished' OR event = 'ftu step 2: finished') AND (object_detail LIKE '%bookkeeper%' OR biz_persona LIKE '%bookkeeper%') THEN 'Bookkeeper'\n" +
                "            WHEN (event = 'ftu step 3: finished' OR event = 'ftu step 2: finished') AND (object_detail LIKE '%accountant%' OR biz_persona LIKE '%accountant%') THEN 'Accountant'\n" +
                "            WHEN (event = 'ftu step 3: finished' OR event = 'ftu step 2: finished') AND (object_detail LIKE '%Else%' OR biz_persona LIKE '%Else%') THEN 'Something Else'\n" +
                "            ELSE NULL END AS role_answer,\n" +
                "\n" +
                "       CASE WHEN (event = 'ftu step 3: finished' OR event = 'ftu step 2: finished' ) AND (object_detail LIKE '%doItYourself%' OR assist_pref LIKE '%doItYourself%')  THEN 'No, I do it all myself'\n" +
                "            WHEN (event = 'ftu step 3: finished' OR event = 'ftu step 2: finished' ) AND (object_detail LIKE '%doItForMe%'    OR assist_pref LIKE '%doItForMe%'   )  THEN 'Yes, I have help'\n" +
                "            WHEN (event = 'ftu step 3: finished' OR event = 'ftu step 2: finished' ) AND (object_detail LIKE '%doItWithMe%'   OR assist_pref LIKE '%doItWithMe%'  )  THEN 'No, but I would like some help'\n" +
                "            ELSE NULL END AS help_pref_answer,\n" +
                "\n" +
                "       CASE WHEN event = 'ftu step 3: finished' AND object_detail = 'howYouMakeMoney step' AND How_make_money LIKE '%service%'        THEN 1 ELSE 0 END AS money_flag_service,\n" +
                "       CASE WHEN event = 'ftu step 3: finished' AND object_detail = 'howYouMakeMoney step' AND How_make_money LIKE '%product%'        THEN 1 ELSE 0 END AS money_flag_product,\n" +
                "       CASE WHEN event = 'ftu step 3: finished' AND object_detail = 'howYouMakeMoney step' AND How_make_money LIKE '%project%'        THEN 1 ELSE 0 END AS money_flag_project,\n" +
                "       CASE WHEN event = 'ftu step 3: finished' AND object_detail = 'howYouMakeMoney step' AND How_make_money LIKE '%somethingElse%'  THEN 1 ELSE 0 END AS money_flag_else,\n" +
                "\n" +
                "       CASE WHEN event = 'ftu step 4: finished' AND object_detail = 'businessEntity step' AND what_biz_entity LIKE '%sole-proprietor%' THEN 'Sole Proprietor'\n" +
                "            WHEN event = 'ftu step 4: finished' AND object_detail = 'businessEntity step' AND what_biz_entity LIKE '%non-profit%'      THEN 'Non-profit'\n" +
                "            WHEN event = 'ftu step 4: finished' AND object_detail = 'businessEntity step' AND what_biz_entity LIKE '%corporation%'     THEN 'Corporation'\n" +
                "            WHEN event = 'ftu step 4: finished' AND object_detail = 'businessEntity step' AND what_biz_entity LIKE '%partnership%'     THEN 'Partnership'\n" +
                "            WHEN event = 'ftu step 4: finished' AND object_detail = 'businessEntity step' AND what_biz_entity LIKE '%not-sure%'        THEN 'Not Sure'\n" +
                "            ELSE NULL END AS entity_answer\n" +
                "FROM preprocess as cl\n" +
                "WHERE 1=1;".toUpperCase();
       t.mySqlStatementsToConsumer(sql, statements);
        for(String state: statements){
            System.out.println(state);
        }
    }
}
