# Analytics engineering with dbt


All queries run in a session with
``` snowflake
USE ROLE TRANSFORMER_DEV;
USE SCHEMA DEV_DB.DBT_JPALLERGMAILCOM;
USE WAREHOUSE TRANSFORMER_DEV_WH;
```

## Week 2 questions:

### Part 1. Models
#### What is our user repeat rate?
``` snowflake
-- using the staging models
WITH USER_METRICS AS (SELECT USER_ID,
                             COUNT(*) AS ORDER_COUNT
                      FROM STG_POSTGRES__ORDERS
                      GROUP BY USER_ID)
SELECT COUNT_IF(ORDER_COUNT > 1) / COUNT(*)
FROM USER_METRICS;

-- using the newly modeled data
SELECT COUNT_IF(ORDER_COUNT > 1) / COUNT(*)
FROM FACT_USER_ORDER;
```
Answer: 80% (0.798387 rounded)

#### What are good indicators of a user who will likely purchase again? What about indicators of users who are likely NOT to purchase again? If you had more data, what features would you want to look into to answer this question?
* What are good indicators of a user who will likely purchase again? 
  * delivery occurred and shipping time was in line with the estimate
  * multiple products purchased
  * they were able to use a discount but it wasn't necessarily a substantial share of the order
  * purchase was made on their first visit to the site
* What about indicators of users who are likely NOT to purchase again?
  * no delivery or long shipping relative to estimate 
  * single product or item
* If you had more data, what features would you want to look into to answer this question?
  * what was inventory at the time of the order?
  * had previously used coupons expired?

#### See code
* greenery/models/intermediate/core/int_order_products.sql
* greenery/models/intermediate/core/int_sessions.sql
* greenery/models/marts/core/dim_products.sql
* greenery/models/marts/core/dim_users.sql
* greenery/models/marts/core/fact_orders.sql
* greenery/models/marts/core/fact_sessions.sql
* greenery/models/marts/marketing/dim_promos.sql
* greenery/models/marts/marketing/fact_user_order.sql
* greenery/models/marts/product/fact_page_views.sql

#### Explain the product mart models you added. Why did you organize the models in the way you did?

 * I added an intermediary model for core purely for general purpose abstraction. 
   * There is no team specific logic or intended usage here
   * Intermediary transformations made it possible to add tests to validate assumptions in the data
     * 1 user per session
     * 0 or 1 order per session
 * I added core, marketing and product marts, partially with an eye towards scope/security
   * general dimensions and granular fact interfaces are exposed in core
   * promo dimensions are only exposed to marketing, along with an interface to explore user interaction across sessions/orders
   * product centric view on interaction and purchases of products is exposed in the product mart
   * addresses are not exposed to anyone for sensitivity until abstracted

### Part 2. Tests
#### Add dbt tests into your dbt project on your existing models from Week 1, and new models from the section above
* What assumptions are you making about each model? (i.e. why are you adding each test?)
  * full set of event types
  * 1:1 user to session ratio
  * 1:1 or missing order to session ratio
  * primary keys are actually unique and not null
* Did you find any “bad” data as you added and ran tests on your models? How did you go about either cleaning the data in the dbt model or adjusting your assumptions/tests?
  * No, I focused more on the tests as a means of validating code and transformations rather than focusing on data issue
  * given the particular data set it would be hard to make assumptions about how to fix the data (except potentially using events to supplement for some missing order data) and with the data set so small dropping rows would seem to distort as much as any subtly wrong data.

#### y. Explain how you would ensure these tests are passing regularly and how you would alert stakeholders about bad data getting through.
I'd create a scheduled job to 
