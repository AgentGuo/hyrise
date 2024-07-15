#include "ch_tpch_queries.hpp"

#include <cstddef>
#include <map>

namespace {

// 参考：https://github.com/citusdata/citus-benchmark/blob/master/ch_benchmark.py

const char* const ch_tpch_query_1 =
    R"(SELECT   OL_NUMBER,
SUM(OL_QUANTITY) AS SUM_QTY,
SUM(OL_AMOUNT) AS SUM_AMOUNT,
AVG(OL_QUANTITY) AS AVG_QTY,
AVG(OL_AMOUNT) AS AVG_AMOUNT,
COUNT(*) AS COUNT_ORDER
FROM	 ORDER_LINE
GROUP BY OL_NUMBER ORDER BY OL_NUMBER;)";

const char* const ch_tpch_query_2 =
    R"(select      s_suppkey, s_name, n_name, I_ID, I_NAME, s_address, s_phone, s_comment
from     ITEM, supplier, STOCK as b, nation, region,
     (select a.S_I_ID as m_i_id,
         min(a.S_QUANTITY) as m_s_quantity
     from     STOCK as a, supplier, nation, region
     where     ((a.S_W_ID*a.S_I_ID)%10000)=s_suppkey
          and s_nationkey=n_nationkey
          and n_regionkey=r_regionkey
          and r_name like 'EUROP%'
     group by a.S_I_ID) m
where      I_ID = b.S_I_ID
     and ((b.S_W_ID * b.S_I_ID)%10000) = s_suppkey
     and s_nationkey = n_nationkey
     and n_regionkey = r_regionkey
     and I_DATA like '%b'
     and r_name like 'EUROP%'
     and I_ID=m_i_id
     and b.S_QUANTITY = m_s_quantity
order by n_name, s_name, I_ID LIMIT 10;)";

const char* const ch_tpch_query_3 =
    R"(SELECT   OL_O_ID, OL_W_ID, OL_D_ID,
SUM(OL_AMOUNT) AS REVENUE, O_ENTRY_D
FROM      CUSTOMER, NEW_ORDER, "ORDER", ORDER_LINE
WHERE      C_STATE LIKE 'a%'
AND C_ID = O_C_ID
AND C_W_ID = O_W_ID
AND C_D_ID = O_D_ID
AND NO_W_ID = O_W_ID
AND NO_D_ID = O_D_ID
AND NO_O_ID = O_ID
AND OL_W_ID = O_W_ID
AND OL_D_ID = O_D_ID
AND OL_O_ID = O_ID
GROUP BY OL_O_ID, OL_W_ID, OL_D_ID, O_ENTRY_D
ORDER BY REVENUE DESC, O_ENTRY_D LIMIT 10;)";

const char* const ch_tpch_query_4 =
    R"(SELECT    O_OL_CNT, COUNT(*) AS ORDER_COUNT
FROM    "ORDER"
    WHERE EXISTS (SELECT *
            FROM ORDER_LINE
            WHERE O_ID = OL_O_ID
            AND O_W_ID = OL_W_ID
            AND O_D_ID = OL_D_ID
            AND OL_DELIVERY_D >= O_ENTRY_D)
GROUP    BY O_OL_CNT
ORDER    BY O_OL_CNT LIMIT 10;)";

const char* const ch_tpch_query_5 =
    R"(SELECT     n_name,
SUM(OL_AMOUNT) AS REVENUE
FROM     CUSTOMER, "ORDER", ORDER_LINE, STOCK, supplier, nation, region
WHERE     C_ID = O_C_ID
AND C_W_ID = O_W_ID
AND C_D_ID = O_D_ID
AND OL_O_ID = O_ID
AND OL_W_ID = O_W_ID
AND OL_D_ID=O_D_ID
AND OL_W_ID = S_W_ID
AND OL_I_ID = S_I_ID
AND ((S_W_ID * S_I_ID)%10000) = s_suppkey
AND s_nationkey = n_nationkey
AND n_regionkey = r_regionkey
AND r_name = 'EUROPE'
AND (C_ID%25) = s_nationkey
GROUP BY n_name
ORDER BY REVENUE DESC LIMIT 10;)";

// AND ASCII(SUBSTR(C_STATE,1,1)) = s_nationkey
// replace to
// AND (C_ID%25) = s_nationkey

const char* const ch_tpch_query_6 =
    R"(SELECT    SUM(OL_AMOUNT) AS REVENUE
FROM ORDER_LINE
WHERE OL_QUANTITY BETWEEN 1 AND 100000 LIMIT 10;)";

const char* const ch_tpch_query_7 =
    R"(SELECT     s_nationkey AS SUPP_NATION,
SUBSTR(C_STATE,1,1) AS CUST_NATION,
((O_ENTRY_D/31536000)+1970) AS L_YEAR,
SUM(OL_AMOUNT) AS REVENUE
FROM     supplier, STOCK, ORDER_LINE, "ORDER", CUSTOMER, nation N1, nation N2
WHERE     OL_SUPPLY_W_ID = S_W_ID
AND OL_I_ID = S_I_ID
AND ((S_W_ID * S_I_ID)%10000) = s_suppkey
AND OL_W_ID = O_W_ID
AND OL_D_ID = O_D_ID
AND OL_O_ID = O_ID
AND C_ID = O_C_ID
AND C_W_ID = O_W_ID
AND C_D_ID = O_D_ID
AND s_nationkey = N1.n_nationkey
AND (C_ID%25) = s_nationkey
AND (
   (N1.n_name = 'GERMANY' AND N2.n_name = 'CAMBODIA')
    OR
   (N1.n_name = 'CAMBODIA' AND N2.n_name = 'GERMANY')
    )
GROUP BY s_nationkey, SUBSTR(C_STATE,1,1), ((O_ENTRY_D/31536000)+1970)
ORDER BY s_nationkey, CUST_NATION, L_YEAR LIMIT 10;)";

// AND ASCII(SUBSTR(C_STATE,1,1)) = N2.n_nationkey

const char* const ch_tpch_query_8 =
    R"(SELECT     ((O_ENTRY_D/31536000)+1970) AS L_YEAR,
SUM(CASE WHEN N2.n_nationkey = 7 THEN OL_AMOUNT ELSE 0.0 END) / SUM(OL_AMOUNT) AS MKT_SHARE
FROM     ITEM, supplier, STOCK, ORDER_LINE, "ORDER", CUSTOMER, nation N1, nation N2, region
WHERE     I_ID = S_I_ID
AND OL_I_ID = S_I_ID
AND OL_SUPPLY_W_ID = S_W_ID
AND ((S_W_ID * S_I_ID)%10000) = s_suppkey
AND OL_W_ID = O_W_ID
AND OL_D_ID = O_D_ID
AND OL_O_ID = O_ID
AND C_ID = O_C_ID
AND C_W_ID = O_W_ID
AND C_D_ID = O_D_ID
AND N1.n_regionkey = r_regionkey
AND OL_I_ID < 1000
AND r_name = 'EUROPE'
AND s_nationkey = N2.n_nationkey
AND I_DATA LIKE '%b'
AND I_ID = OL_I_ID
GROUP BY ((O_ENTRY_D/31536000)+1970)
ORDER BY L_YEAR LIMIT 10;)";



const char* const ch_tpch_query_9 =
    R"(SELECT     n_name, ((O_ENTRY_D/31536000)+1970) AS L_YEAR, SUM(OL_AMOUNT) AS SUM_PROFIT
FROM     ITEM, STOCK, supplier, ORDER_LINE, "ORDER", nation
WHERE     OL_I_ID = S_I_ID
     AND OL_SUPPLY_W_ID = S_W_ID
     AND ((S_W_ID * S_I_ID)%10000) = s_suppkey
     AND OL_W_ID = O_W_ID
     AND OL_D_ID = O_D_ID
     AND OL_O_ID = O_ID
     AND OL_I_ID = I_ID
     AND s_nationkey = n_nationkey
     AND I_DATA LIKE '%bb'
GROUP BY n_name, ((O_ENTRY_D/31536000)+1970)
ORDER BY n_name, L_YEAR DESC LIMIT 10;)";

const char* const ch_tpch_query_10 =
    R"(SELECT     C_ID, C_LAST, SUM(OL_AMOUNT) AS REVENUE, C_CITY, C_PHONE, n_name
FROM     CUSTOMER, "ORDER", ORDER_LINE, nation
WHERE     C_ID = O_C_ID
     AND C_W_ID = O_W_ID
     AND C_D_ID = O_D_ID
     AND OL_W_ID = O_W_ID
     AND OL_D_ID = O_D_ID
     AND OL_O_ID = O_ID
     AND O_ENTRY_D <= OL_DELIVERY_D
     AND (C_ID%25) = n_nationkey
GROUP BY C_ID, C_LAST, C_CITY, C_PHONE, n_name
ORDER BY REVENUE DESC LIMIT 10;)";

const char* const ch_tpch_query_11 =
    R"(SELECT     S_I_ID, SUM(S_ORDER_CNT) AS ORDERCOUNT
FROM     STOCK, supplier, nation
WHERE     ((S_W_ID * S_I_ID)%10000) = s_suppkey
     AND s_nationkey = n_nationkey
     AND n_name = 'GERMANY'
GROUP BY S_I_ID
HAVING   SUM(S_ORDER_CNT) >
        (SELECT SUM(S_ORDER_CNT) * .005
        FROM STOCK, supplier, nation
        WHERE ((S_W_ID * S_I_ID)%10000) = s_suppkey
        AND s_nationkey = n_nationkey
        AND n_name = 'GERMANY')
ORDER BY ORDERCOUNT DESC LIMIT 10;)";

const char* const ch_tpch_query_12 =
    R"(SELECT     O_OL_CNT,
SUM(CASE WHEN O_CARRIER_ID = 1 OR O_CARRIER_ID = 2 THEN 1 ELSE 0 END) AS HIGH_LINE_COUNT,
SUM(CASE WHEN O_CARRIER_ID <> 1 AND O_CARRIER_ID <> 2 THEN 1 ELSE 0 END) AS LOW_LINE_COUNT
FROM     "ORDER", ORDER_LINE
WHERE     OL_W_ID = O_W_ID
AND OL_D_ID = O_D_ID
AND OL_O_ID = O_ID
AND O_ENTRY_D <= OL_DELIVERY_D
GROUP BY O_OL_CNT
ORDER BY O_OL_CNT LIMIT 10;)";

const char* const ch_tpch_query_13 =
    R"(SELECT     C_COUNT, COUNT(*) AS CUSTDIST
FROM     (SELECT C_ID, COUNT(O_ID)
     FROM CUSTOMER LEFT OUTER JOIN "ORDER" ON (
        C_W_ID = O_W_ID
        AND C_D_ID = O_D_ID
        AND C_ID = O_C_ID
        AND O_CARRIER_ID > 8)
     GROUP BY C_ID) AS C_ORDER (C_ID, C_COUNT)
GROUP BY C_COUNT
ORDER BY CUSTDIST DESC, C_COUNT DESC LIMIT 10;)";

const char* const ch_tpch_query_14 =
    R"(SELECT    100.00 * SUM(CASE WHEN I_DATA LIKE 'pr%' THEN OL_AMOUNT ELSE 0 END) / (1+SUM(OL_AMOUNT)) AS PROMO_REVENUE
FROM ORDER_LINE, ITEM
WHERE OL_I_ID = I_ID
    LIMIT 10;)";

const char* const ch_tpch_query_15 =
    R"(WITH     REVENUE AS (
SELECT ((S_W_ID * S_I_ID)%10000) AS SUPPLIER_NO,
   SUM(OL_AMOUNT) AS TOTAL_REVENUE
FROM ORDER_LINE, STOCK
   WHERE OL_I_ID = S_I_ID AND OL_SUPPLY_W_ID = S_W_ID
GROUP BY ((S_W_ID * S_I_ID)%10000))
SELECT     s_suppkey, s_name, s_address, s_phone, TOTAL_REVENUE
FROM     supplier, REVENUE
WHERE     s_suppkey = SUPPLIER_NO
AND TOTAL_REVENUE = (SELECT MAX(TOTAL_REVENUE) FROM REVENUE)
ORDER BY s_suppkey LIMIT 10;)";

const char* const ch_tpch_query_16 =
    R"(SELECT     I_NAME,
SUBSTR(I_DATA, 1, 3) AS BRAND,
I_PRICE,
COUNT(DISTINCT (((S_W_ID * S_I_ID)%10000))) AS SUPPLIER_CNT
FROM     STOCK, ITEM
WHERE     I_ID = S_I_ID
AND I_DATA NOT LIKE 'zz%'
AND (((S_W_ID * S_I_ID)%10000) NOT IN
   (SELECT s_suppkey
    FROM supplier
    WHERE s_comment LIKE '%bad%'))
GROUP BY I_NAME, SUBSTR(I_DATA, 1, 3), I_PRICE
ORDER BY SUPPLIER_CNT DESC LIMIT 10;)";

const char* const ch_tpch_query_17 =
    R"(SELECT    SUM(OL_AMOUNT) / 2.0 AS AVG_YEARLY
FROM ORDER_LINE, (SELECT   I_ID, AVG(OL_QUANTITY) AS A
            FROM     ITEM, ORDER_LINE
            WHERE    I_DATA LIKE '%b'
                 AND OL_I_ID = I_ID
            GROUP BY I_ID) T
WHERE OL_I_ID = T.I_ID
    AND OL_QUANTITY < T.A LIMIT 10;)";

const char* const ch_tpch_query_18 =
    R"(SELECT     C_LAST, C_ID, O_ID, O_ENTRY_D, O_OL_CNT, SUM(OL_AMOUNT)
FROM     CUSTOMER, "ORDER", ORDER_LINE
WHERE     C_ID = O_C_ID
     AND C_W_ID = O_W_ID
     AND C_D_ID = O_D_ID
     AND OL_W_ID = O_W_ID
     AND OL_D_ID = O_D_ID
     AND OL_O_ID = O_ID
GROUP BY O_ID, O_W_ID, O_D_ID, C_ID, C_LAST, O_ENTRY_D, O_OL_CNT
HAVING     SUM(OL_AMOUNT) > 200
ORDER BY SUM(OL_AMOUNT) DESC, O_ENTRY_D LIMIT 10;)";

const char* const ch_tpch_query_19 =

    R"(SELECT    SUM(OL_AMOUNT) AS REVENUE
FROM ORDER_LINE, ITEM
WHERE    (
      OL_I_ID = I_ID
          AND I_DATA LIKE '%a'
          AND OL_QUANTITY >= 1
          AND OL_QUANTITY <= 10
          AND I_PRICE BETWEEN 1 AND 400000
          AND OL_W_ID IN (1,2,3)
    ) OR (
      OL_I_ID = I_ID
      AND I_DATA LIKE '%b'
      AND OL_QUANTITY >= 1
      AND OL_QUANTITY <= 10
      AND I_PRICE BETWEEN 1 AND 400000
      AND OL_W_ID IN (1,2,4)
    ) OR (
      OL_I_ID = I_ID
      AND I_DATA LIKE '%c'
      AND OL_QUANTITY >= 1
      AND OL_QUANTITY <= 10
      AND I_PRICE BETWEEN 1 AND 400000
      AND OL_W_ID IN (1,5,3)
    ) LIMIT 10;)";

const char* const ch_tpch_query_20 =
    R"(SELECT   s_name, s_address
FROM     supplier, nation
WHERE    s_suppkey IN
        (SELECT  ((S_I_ID * S_W_ID)%10000)
        FROM     STOCK, ORDER_LINE
        WHERE    S_I_ID IN
                (SELECT I_ID
                 FROM ITEM
                 WHERE I_DATA LIKE 'co%')
             AND OL_I_ID=S_I_ID
        GROUP BY S_I_ID, S_W_ID, S_QUANTITY
        HAVING   2*S_QUANTITY > SUM(OL_QUANTITY))
     AND s_nationkey = n_nationkey
     AND n_name = 'GERMANY'
ORDER BY s_name LIMIT 10;)";

const char* const ch_tpch_query_21 =
    R"(SELECT     s_name, COUNT(*) AS NUMWAIT
FROM     supplier, ORDER_LINE L1, "ORDER", STOCK, nation
WHERE     OL_O_ID = O_ID
     AND OL_W_ID = O_W_ID
     AND OL_D_ID = O_D_ID
     AND OL_W_ID = S_W_ID
     AND OL_I_ID = S_I_ID
     AND ((S_W_ID * S_I_ID)%10000) = s_suppkey
     AND L1.OL_DELIVERY_D > O_ENTRY_D
     AND NOT EXISTS (SELECT *
             FROM ORDER_LINE L2
             WHERE  L2.OL_O_ID = L1.OL_O_ID
                AND L2.OL_W_ID = L1.OL_W_ID
                AND L2.OL_D_ID = L1.OL_D_ID
                AND L2.OL_DELIVERY_D > L1.OL_DELIVERY_D)
     AND s_nationkey = n_nationkey
     AND n_name = 'GERMANY'
GROUP BY s_name
ORDER BY NUMWAIT DESC, s_name LIMIT 10;)";

const char* const ch_tpch_query_22 =
    R"(SELECT     SUBSTR(C_STATE,1,1) AS COUNTRY,
COUNT(*) AS NUMCUST,
SUM(C_BALANCE) AS TOTACCTBAL
FROM     CUSTOMER
WHERE     SUBSTR(C_PHONE,1,1) IN ('1','2','3','4','5','6','7')
AND C_BALANCE > (SELECT AVG(C_BALANCE)
         FROM      CUSTOMER
         WHERE  C_BALANCE > 0.00
             AND SUBSTR(C_PHONE,1,1) IN ('1','2','3','4','5','6','7'))
AND NOT EXISTS (SELECT *
        FROM    "ORDER"
        WHERE    O_C_ID = C_ID
                AND O_W_ID = C_W_ID
               AND O_D_ID = C_D_ID)
GROUP BY SUBSTR(C_STATE,1,1)
ORDER BY SUBSTR(C_STATE,1,1) LIMIT 10;)";

}  // namespace

namespace hyrise {

const std::map<size_t, const char*> ch_tpch_queries = {
    {1, ch_tpch_query_1},   {2, ch_tpch_query_2},   {3, ch_tpch_query_3},   {4, ch_tpch_query_4},   {5, ch_tpch_query_5},
    {6, ch_tpch_query_6},   {7, ch_tpch_query_7},   {8, ch_tpch_query_8},   {9, ch_tpch_query_9},   {10, ch_tpch_query_10},
    {11, ch_tpch_query_11}, {12, ch_tpch_query_12}, {13, ch_tpch_query_13}, {14, ch_tpch_query_14}, {15, ch_tpch_query_15},
    {16, ch_tpch_query_16}, {17, ch_tpch_query_17}, {18, ch_tpch_query_18}, {19, ch_tpch_query_19}, {20, ch_tpch_query_20},
    {21, ch_tpch_query_21}, {22, ch_tpch_query_22}};

}  // namespace hyrise
