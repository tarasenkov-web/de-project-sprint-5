DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 
                   FROM information_schema.columns 
                   WHERE table_name='f_sales' 
                   AND column_name='status') THEN
        ALTER TABLE mart.f_sales ADD COLUMN status text;
    END IF;
END $$;


insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
select dc.date_id, item_id, customer_id, city_id, quantity, payment_amount, status from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}';

UPDATE mart.f_sales
SET payment_amount = -payment_amount
WHERE status = 'refunded' AND payment_amount > 0;