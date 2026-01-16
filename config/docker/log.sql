-- Sample log data for IceGate logs table (Trino + Iceberg compatible)
-- Timestamps spread within 10 minutes from execution time (1 minute apart, going into the past)

INSERT INTO icegate.logs (
    tenant_id,
    cloud_account_id,
    service_name,
    timestamp,
    observed_timestamp,
    ingested_timestamp,
    trace_id,
    span_id,
    severity_text,
    body,
    attributes
) VALUES
-- Row 1: NOW - 9 minutes (oldest)
(
    'default',
    'account-001',
    'api-gateway',
    current_timestamp - INTERVAL '9' MINUTE,
    current_timestamp - INTERVAL '9' MINUTE,
    current_timestamp - INTERVAL '9' MINUTE,
    '0123456789abcdef0123456789abcdef',
    '0123456789abcdef',
    'INFO',
    '{"method": "GET", "path": "/api/v1/users", "status": 200, "latency_ms": 45}',
    MAP(ARRAY['http_method', 'http_status_code', 'http_url', 'environment', 'cloud_account_id', 'service_name', 'trace_id', 'span_id', 'severity_text', 'level'], ARRAY['GET', '200', '/api/v1/users', 'production', 'account-001', 'api-gateway', '0123456789abcdef0123456789abcdef', '0123456789abcdef', 'INFO', 'INFO'])
),
-- Row 2: NOW - 8 minutes
(
    'default',
    'account-001',
    'user-service',
    current_timestamp - INTERVAL '8' MINUTE,
    current_timestamp - INTERVAL '8' MINUTE,
    current_timestamp - INTERVAL '8' MINUTE,
    'fedcba9876543210fedcba9876543210',
    'fedcba9876543210',
    'WARN',
    '{"message": "Database connection pool running low", "available": 2, "max": 10}',
    MAP(ARRAY['db_system', 'db_name', 'pool_available', 'environment', 'cloud_account_id', 'service_name', 'trace_id', 'span_id', 'severity_text', 'level'], ARRAY['postgresql', 'users', '2', 'production', 'account-001', 'user-service', 'fedcba9876543210fedcba9876543210', 'fedcba9876543210', 'WARN', 'WARN'])
),
-- Row 3: NOW - 7 minutes
(
    'default',
    'account-002',
    'payment-service',
    current_timestamp - INTERVAL '7' MINUTE,
    current_timestamp - INTERVAL '7' MINUTE,
    current_timestamp - INTERVAL '7' MINUTE,
    'abcdef0123456789abcdef0123456789',
    'abcdef01234567ab',
    'ERROR',
    '{"error": "Payment gateway timeout", "transaction_id": "txn-12345", "retry_count": 3}',
    MAP(ARRAY['payment_gateway', 'payment_amount', 'payment_currency', 'environment', 'cloud_account_id', 'service_name', 'trace_id', 'span_id', 'severity_text', 'level'], ARRAY['stripe', '99.99', 'USD', 'production', 'account-002', 'payment-service', 'abcdef0123456789abcdef0123456789', 'abcdef01234567ab', 'ERROR', 'ERROR'])
),
-- Row 4: NOW - 6 minutes
(
    'default',
    'account-001',
    'api-gateway',
    current_timestamp - INTERVAL '6' MINUTE,
    current_timestamp - INTERVAL '6' MINUTE,
    current_timestamp - INTERVAL '6' MINUTE,
    '1111111111111111aaaaaaaaaaaaaaaa',
    '1111111111111111',
    'INFO',
    '{"method": "POST", "path": "/api/v1/orders", "status": 201, "latency_ms": 120}',
    MAP(ARRAY['http_method', 'http_status_code', 'http_url', 'environment', 'cloud_account_id', 'service_name', 'trace_id', 'span_id', 'severity_text', 'level'], ARRAY['POST', '201', '/api/v1/orders', 'production', 'account-001', 'api-gateway', '1111111111111111aaaaaaaaaaaaaaaa', '1111111111111111', 'INFO', 'INFO'])
),
-- Row 5: NOW - 5 minutes
(
    'default',
    'account-003',
    'inventory-service',
    current_timestamp - INTERVAL '5' MINUTE,
    current_timestamp - INTERVAL '5' MINUTE,
    current_timestamp - INTERVAL '5' MINUTE,
    '2222222222222222bbbbbbbbbbbbbbbb',
    '2222222222222222',
    'DEBUG',
    '{"action": "stock_check", "product_id": "SKU-789", "quantity": 150}',
    MAP(ARRAY['inventory_sku', 'inventory_warehouse', 'environment', 'cloud_account_id', 'service_name', 'trace_id', 'span_id', 'severity_text', 'level'], ARRAY['SKU-789', 'WH-01', 'staging', 'account-003', 'inventory-service', '2222222222222222bbbbbbbbbbbbbbbb', '2222222222222222', 'DEBUG', 'DEBUG'])
),
-- Row 6: NOW - 4 minutes
(
    'default',
    'account-001',
    'notification-service',
    current_timestamp - INTERVAL '4' MINUTE,
    current_timestamp - INTERVAL '4' MINUTE,
    current_timestamp - INTERVAL '4' MINUTE,
    '3333333333333333cccccccccccccccc',
    '3333333333333333',
    'INFO',
    '{"type": "email", "recipient": "user@example.com", "template": "order_confirmation", "sent": true}',
    MAP(ARRAY['notification_type', 'notification_status', 'environment', 'cloud_account_id', 'service_name', 'trace_id', 'span_id', 'severity_text', 'level'], ARRAY['email', 'sent', 'production', 'account-001', 'notification-service', '3333333333333333cccccccccccccccc', '3333333333333333', 'INFO', 'INFO'])
),
-- Row 7: NOW - 3 minutes
(
    'default',
    'account-003',
    'auth-service',
    current_timestamp - INTERVAL '3' MINUTE,
    current_timestamp - INTERVAL '3' MINUTE,
    current_timestamp - INTERVAL '3' MINUTE,
    '4444444444444444dddddddddddddddd',
    '4444444444444444',
    'WARN',
    '{"event": "failed_login", "user_id": "user-456", "ip": "192.168.1.100", "attempts": 3}',
    MAP(ARRAY['auth_event', 'auth_ip', 'auth_user_agent', 'environment', 'cloud_account_id', 'service_name', 'trace_id', 'span_id', 'severity_text', 'level'], ARRAY['failed_login', '192.168.1.100', 'Mozilla/5.0', 'production', 'account-003', 'auth-service', '4444444444444444dddddddddddddddd', '4444444444444444', 'WARN', 'WARN'])
),
-- Row 8: NOW - 2 minutes
(
    'default',
    'account-002',
    'search-service',
    current_timestamp - INTERVAL '2' MINUTE,
    current_timestamp - INTERVAL '2' MINUTE,
    current_timestamp - INTERVAL '2' MINUTE,
    '5555555555555555eeeeeeeeeeeeeeee',
    '5555555555555555',
    'INFO',
    '{"query": "laptop", "results": 245, "took_ms": 32, "filters": ["category:electronics"]}',
    MAP(ARRAY['search_query', 'search_results_count', 'search_index', 'environment', 'cloud_account_id', 'service_name', 'trace_id', 'span_id', 'severity_text', 'level'], ARRAY['laptop', '245', 'products', 'production', 'account-002', 'search-service', '5555555555555555eeeeeeeeeeeeeeee', '5555555555555555', 'INFO', 'INFO'])
),
-- Row 9: NOW - 1 minute
(
    'default',
    'account-001',
    'api-gateway',
    current_timestamp - INTERVAL '1' MINUTE,
    current_timestamp - INTERVAL '1' MINUTE,
    current_timestamp - INTERVAL '1' MINUTE,
    '6666666666666666ffffffffffffffff',
    '6666666666666666',
    'ERROR',
    '{"method": "GET", "path": "/api/v1/products/999", "status": 500, "error": "Internal server error"}',
    MAP(ARRAY['http_method', 'http_status_code', 'http_url', 'error_type', 'environment', 'cloud_account_id', 'service_name', 'trace_id', 'span_id', 'severity_text', 'level'], ARRAY['GET', '500', '/api/v1/products/999', 'InternalServerError', 'production', 'account-001', 'api-gateway', '6666666666666666ffffffffffffffff', '6666666666666666', 'ERROR', 'ERROR'])
),
-- Row 10: NOW (most recent)
(
    'default',
    'account-004',
    'billing-service',
    current_timestamp,
    current_timestamp,
    current_timestamp,
    '7777777777777777000000000000000a',
    '7777777777777777',
    'INFO',
    '{"action": "invoice_generated", "invoice_id": "INV-2024-001", "amount": 1250.00, "customer_id": "cust-789"}',
    MAP(ARRAY['billing_invoice_id', 'billing_amount', 'billing_currency', 'environment', 'cloud_account_id', 'service_name', 'trace_id', 'span_id', 'severity_text', 'level'], ARRAY['INV-2024-001', '1250.00', 'USD', 'production', 'account-004', 'billing-service', '7777777777777777000000000000000a', '7777777777777777', 'INFO', 'INFO'])
);

SELECT * FROM icegate.logs;

-- DELETE FROM icegate.logs WHERE 1=1;  -- Commented out to preserve sample data

