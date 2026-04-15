/* Top thị trường có doanh thu cao nhất */
SELECT
  market,
  SUM(total_sales) AS total_sales
FROM kpi_summary
GROUP BY
  market
ORDER BY
  total_sales DESC
LIMIT 5;
/* Top danh mục sản phẩm lợi nhuận cao nhất */
SELECT
  category_name,
  SUM(total_profit) AS total_profit
FROM kpi_summary
GROUP BY
  category_name
ORDER BY
  total_profit DESC
LIMIT 5;
/* Tỷ lệ giao trễ cao nhất theo thị trường */
SELECT
  market,
  AVG(late_delivery_rate) AS avg_late_rate
FROM kpi_summary
GROUP BY
  market
ORDER BY
  avg_late_rate DESC;
/* Danh mục có discount cao nhưng profit thấp */
SELECT
  category_name,
  avg_discount_rate,
  avg_profit_ratio
FROM kpi_summary
ORDER BY
  avg_discount_rate DESC,
  avg_profit_ratio ASC;
/* Xu hướng doanh thu theo tháng */
SELECT
  order_month,
  SUM(monthly_revenue) AS revenue
FROM monthly_financials
GROUP BY
  order_month
ORDER BY
  order_month;
/* Tăng trưởng doanh thu MoM (Month-over-Month) */
SELECT
  order_month,
  monthly_revenue,
  LAG(monthly_revenue) OVER (ORDER BY order_month) AS prev_month,
  ROUND(
    (
      monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY order_month)
    ) / LAG(monthly_revenue) OVER (ORDER BY order_month) * 100,
    2
  ) AS growth_pct
FROM monthly_financials;
/* Phương thức vận chuyển bị trễ nhiều nhất */
SELECT
  shipping_mode,
  SUM(late_shipment_count) AS total_late
FROM shipping_performance
GROUP BY
  shipping_mode
ORDER BY
  total_late DESC;
/* Tỷ lệ giao trễ theo vùng */
SELECT
  order_region,
  AVG(on_time_failure_rate) AS late_rate
FROM shipping_performance
GROUP BY
  order_region
ORDER BY
  late_rate DESC;
/* Phân khúc khách hàng mang lại doanh thu cao nhất */
SELECT
  customer_segment,
  SUM(segment_revenue) AS revenue
FROM customer_analytics
GROUP BY
  customer_segment
ORDER BY
  revenue DESC;
SELECT
  market,
  SUM(total_sales) AS revenue,
  SUM(total_profit) AS profit,
  AVG(late_delivery_rate) AS late_rate
FROM kpi_summary
GROUP BY
  market
ORDER BY
  revenue DESC,
  profit DESC,
  late_rate ASC;
/* Top tháng có hiệu suất tốt nhất */
SELECT
  order_month,
  SUM(monthly_revenue) AS revenue,
  SUM(monthly_profit) AS profit
FROM monthly_financials
GROUP BY
  order_month
ORDER BY
  profit DESC
LIMIT 5