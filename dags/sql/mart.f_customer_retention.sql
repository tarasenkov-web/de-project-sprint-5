CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
item_id INT NOT NULL PRIMARY KEY,
new_customers_count INT,
returning_customers_count INT,
refunded_customer_count INT,
period_name character(8), 
period_id smallint,
new_customers_revenue INT,
returning_customers_revenue INT,
customers_refunded INT);

with customers as
    (select *
    from mart.f_sales
    join mart.d_calendar on f_sales.date_id = d_calendar.date_id
    where week_of_year = DATE_PART('week', '{{ds}}'::DATE)),
new_customers as
    (select customer_id
    from customers
    where status = 'shipped'
    group by customer_id
    having count(*) = 1),
returning_customers as
    (select customer_id
    from customers
    where status = 'shipped'
    group by customer_id
    having count(*) > 1),
refunded_customers as
    (select customer_id
    from customers
    where status = 'refunded'
    group by customer_id)

INSERT INTO mart.f_customer_retention (item_id, period_id, period_name, new_customers_count, returning_customers_count, refunded_customer_count, new_customers_revenue, returning_customers_revenue, customers_refunded)
select
    coalesce(ncr.item_id,
                rcr.item_id,
                cr.item_id) as item_id, week_of_year as period_id, 'weekly' as period_name,
    count(DISTINCT nc.customer_id) new_customers_count, 
    count(DISTINCT rc.customer_id) returning_customers_count, 
    count(DISTINCT rfc.customer_id) refunded_customer_count,
    max(new_customers_revenue) new_customers_revenue,
    max(returning_customers_revenue) returning_customers_revenue ,
    min(customers_refunded) customers_refunded
    from customers c
    left join new_customers nc on c.customer_id = nc.customer_id
    left join returning_customers rc on c.customer_id = rc.customer_id
    left join refunded_customers rfc on c.customer_id = rfc.customer_id
    left join (select item_id, customer_id, sum(payment_amount) customers_refunded
    from customers c
    where status = 'refunded'
    group by 1,2) cr on c.customer_id = cr.customer_id and c.item_id = cr.item_id
    left join (select item_id, customer_id, sum(payment_amount) new_customers_revenue
    from customers c
    where status = 'shipped'
    group by 1,2
    having count(*) = 1) ncr on c.customer_id = ncr.customer_id and c.item_id = ncr.item_id
    left join (select item_id, customer_id, sum(payment_amount) returning_customers_revenue
    from customers c
    where status = 'shipped'
    group by 1,2
    having count(*) > 1) rcr on c.customer_id = rcr.customer_id and c.item_id = rcr.item_id
    group by 1,2,3
    ON CONFLICT (item_id) DO UPDATE SET
    new_customers_count = EXCLUDED.new_customers_count,
    returning_customers_count = EXCLUDED.returning_customers_count,
    refunded_customer_count = EXCLUDED.refunded_customer_count,
    new_customers_revenue = EXCLUDED.new_customers_revenue,
    returning_customers_revenue = EXCLUDED.returning_customers_revenue,
    customers_refunded = EXCLUDED.customers_refunded;