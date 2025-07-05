-- Required views for RentLok Dashboard
--===========================================================================================================================
--View1:
CREATE OR REPLACE VIEW rentlok_prd.gold.vw_completed_bookings
AS
SELECT 
    d.year, 
    p.property_name, 
    count(*) as total_bookings
FROM rentlok_prd.gold.dim_properties p
LEFT JOIN rentlok_prd.gold.fact_bookings fb ON p.sk_properties = fb.sk_properties
JOIN rentlok_prd.gold.dim_date d            ON fb.sk_move_in_date = d.date_key
WHERE fb.status = 'completed'and p.is_current=TRUE
GROUP BY d.year, p.property_id, p.property_name
ORDER BY p.property_id, d.year DESC;

--View2:
CREATE OR REPLACE VIEW rentlok_prd.gold.vw_monthly_requests
AS
SELECT 
    d.year,
    p.property_name,
    d.month,
    count(*) as total_requests
FROM rentlok_prd.gold.dim_properties p
LEFT JOIN rentlok_prd.gold.dim_requests r ON p.property_id = r.property_id
JOIN rentlok_prd.gold.dim_date d          ON r.request_date=d.date
WHERE p.is_current = TRUE and r.is_current = TRUE
group by d.year, d.month, p.property_name
order by d.year, p.property_name, d.month ;

--View3:
CREATE OR REPLACE VIEW rentlok_prd.gold.vw_total_tenants
AS
SELECT 
    p.property_name,
    count(distinct tenant_id) as tenants
FROM rentlok_prd.gold.dim_properties p
LEFT JOIN rentlok_prd.gold.fact_bookings fb ON p.sk_properties = fb.sk_properties
JOIN rentlok_prd.gold.dim_tenants t ON fb.sk_tenants = t.sk_tenants
GROUP BY p.property_id,p.property_name;

--View4:
CREATE OR REPLACE VIEW rentlok_prd.gold.vw_total_revenue
AS
SELECT
    p.property_name,
    d.year, 
    SUM(fp.amount) as Revenue
FROM rentlok_prd.gold.dim_properties p
LEFT JOIN rentlok_prd.gold.fact_bookings fb ON p.sk_properties = fb.sk_properties
JOIN rentlok_prd.gold.fact_payments fp  ON fb.sk_bookings = fp.sk_bookings
JOIN rentlok_prd.gold.dim_date d         ON fp.sk_payment_date = d.date_key
GROUP BY d.year, p.property_name
ORDER BY p.property_name, d.year DESC ;