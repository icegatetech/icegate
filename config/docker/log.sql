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
    severity_number,
    severity_text,
    body,
    attributes,
    flags,
    dropped_attributes_count
) VALUES
-- Row 1: NOW - 9 minutes (oldest)
(
    'anonymous',
    'account-001',
    'api-gateway',
    current_timestamp - INTERVAL '9' MINUTE,
    current_timestamp - INTERVAL '9' MINUTE,
    current_timestamp - INTERVAL '9' MINUTE,
    from_hex('0123456789abcdef0123456789abcdef'),
    from_hex('0123456789abcdef'),
    9,
    'INFO',
    '{"method": "GET", "path": "/api/v1/users", "status": 200, "latency_ms": 45}',
    MAP(ARRAY['http_method', 'http_status_code', 'http_url', 'environment'], ARRAY['GET', '200', '/api/v1/users', 'production']),
    0,
    0
),
-- Row 2: NOW - 8 minutes
(
    'anonymous',
    'account-001',
    'user-service',
    current_timestamp - INTERVAL '8' MINUTE,
    current_timestamp - INTERVAL '8' MINUTE,
    current_timestamp - INTERVAL '8' MINUTE,
    from_hex('fedcba9876543210fedcba9876543210'),
    from_hex('fedcba9876543210'),
    13,
    'WARN',
    '{"message": "Database connection pool running low", "available": 2, "max": 10}',
    MAP(ARRAY['db_system', 'db_name', 'pool_available', 'environment'], ARRAY['postgresql', 'users', '2', 'production']),
    0,
    0
),
-- Row 3: NOW - 7 minutes
(
    'anonymous',
    'account-002',
    'payment-service',
    current_timestamp - INTERVAL '7' MINUTE,
    current_timestamp - INTERVAL '7' MINUTE,
    current_timestamp - INTERVAL '7' MINUTE,
    from_hex('abcdef0123456789abcdef0123456789'),
    from_hex('abcdef01234567ab'),
    17,
    'ERROR',
    '{"error": "Payment gateway timeout", "transaction_id": "txn-12345", "retry_count": 3}',
    MAP(ARRAY['payment_gateway', 'payment_amount', 'payment_currency', 'environment'], ARRAY['stripe', '99.99', 'USD', 'production']),
    0,
    0
),
-- Row 4: NOW - 6 minutes
(
    'anonymous',
    'account-001',
    'api-gateway',
    current_timestamp - INTERVAL '6' MINUTE,
    current_timestamp - INTERVAL '6' MINUTE,
    current_timestamp - INTERVAL '6' MINUTE,
    from_hex('1111111111111111aaaaaaaaaaaaaaaa'),
    from_hex('1111111111111111'),
    9,
    'INFO',
    '{"method": "POST", "path": "/api/v1/orders", "status": 201, "latency_ms": 120}',
    MAP(ARRAY['http_method', 'http_status_code', 'http_url', 'environment'], ARRAY['POST', '201', '/api/v1/orders', 'production']),
    0,
    0
),
-- Row 5: NOW - 5 minutes
(
    'anonymous',
    'account-003',
    'inventory-service',
    current_timestamp - INTERVAL '5' MINUTE,
    current_timestamp - INTERVAL '5' MINUTE,
    current_timestamp - INTERVAL '5' MINUTE,
    from_hex('2222222222222222bbbbbbbbbbbbbbbb'),
    from_hex('2222222222222222'),
    5,
    'DEBUG',
    '{"action": "stock_check", "product_id": "SKU-789", "quantity": 150}',
    MAP(ARRAY['inventory_sku', 'inventory_warehouse', 'environment'], ARRAY['SKU-789', 'WH-01', 'staging']),
    0,
    0
),
-- Row 6: NOW - 4 minutes
(
    'anonymous',
    'account-001',
    'notification-service',
    current_timestamp - INTERVAL '4' MINUTE,
    current_timestamp - INTERVAL '4' MINUTE,
    current_timestamp - INTERVAL '4' MINUTE,
    from_hex('3333333333333333cccccccccccccccc'),
    from_hex('3333333333333333'),
    9,
    'INFO',
    '{"type": "email", "recipient": "user@example.com", "template": "order_confirmation", "sent": true}',
    MAP(ARRAY['notification_type', 'notification_status', 'environment'], ARRAY['email', 'sent', 'production']),
    0,
    0
),
-- Row 7: NOW - 3 minutes
(
    'anonymous',
    'account-003',
    'auth-service',
    current_timestamp - INTERVAL '3' MINUTE,
    current_timestamp - INTERVAL '3' MINUTE,
    current_timestamp - INTERVAL '3' MINUTE,
    from_hex('4444444444444444dddddddddddddddd'),
    from_hex('4444444444444444'),
    13,
    'WARN',
    '{"event": "failed_login", "user_id": "user-456", "ip": "192.168.1.100", "attempts": 3}',
    MAP(ARRAY['auth_event', 'auth_ip', 'auth_user_agent', 'environment'], ARRAY['failed_login', '192.168.1.100', 'Mozilla/5.0', 'production']),
    0,
    0
),
-- Row 8: NOW - 2 minutes
(
    'anonymous',
    'account-002',
    'search-service',
    current_timestamp - INTERVAL '2' MINUTE,
    current_timestamp - INTERVAL '2' MINUTE,
    current_timestamp - INTERVAL '2' MINUTE,
    from_hex('5555555555555555eeeeeeeeeeeeeeee'),
    from_hex('5555555555555555'),
    9,
    'INFO',
    '{"query": "laptop", "results": 245, "took_ms": 32, "filters": ["category:electronics"]}',
    MAP(ARRAY['search_query', 'search_results_count', 'search_index', 'environment'], ARRAY['laptop', '245', 'products', 'production']),
    0,
    0
),
-- Row 9: NOW - 1 minute
(
    'anonymous',
    'account-001',
    'api-gateway',
    current_timestamp - INTERVAL '1' MINUTE,
    current_timestamp - INTERVAL '1' MINUTE,
    current_timestamp - INTERVAL '1' MINUTE,
    from_hex('6666666666666666ffffffffffffffff'),
    from_hex('6666666666666666'),
    17,
    'ERROR',
    '{"method": "GET", "path": "/api/v1/products/999", "status": 500, "error": "Internal server error"}',
    MAP(ARRAY['http_method', 'http_status_code', 'http_url', 'error_type', 'environment'], ARRAY['GET', '500', '/api/v1/products/999', 'InternalServerError', 'production']),
    0,
    0
),
-- Row 10: NOW (most recent)
(
    'anonymous',
    'account-004',
    'billing-service',
    current_timestamp,
    current_timestamp,
    current_timestamp,
    from_hex('7777777777777777000000000000000a'),
    from_hex('7777777777777777'),
    9,
    'INFO',
    '{"action": "invoice_generated", "invoice_id": "INV-2024-001", "amount": 1250.00, "customer_id": "cust-789"}',
    MAP(ARRAY['billing_invoice_id', 'billing_amount', 'billing_currency', 'environment'], ARRAY['INV-2024-001', '1250.00', 'USD', 'production']),
    0,
    0
);

SELECT * FROM icegate.logs;
