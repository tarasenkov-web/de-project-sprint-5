DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 
                   FROM information_schema.columns 
                   WHERE table_name='user_order_log' 
                   AND column_name='status') THEN
        ALTER TABLE staging.user_order_log ADD COLUMN status text;
    END IF;
END $$;


UPDATE staging.user_order_log
SET status = 'shipped';