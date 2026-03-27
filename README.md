```
// ============================================
// USAGE EXAMPLE
// ============================================

// Configuration for different databases
$config = [
    // MySQL Example with API Key authentication
    'driver' => 'mysql',
    'host' => 'localhost',
    'port' => 3306,
    'database' => 'your_database',
    'username' => 'your_username',
    'password' => 'your_password',
    'charset' => 'utf8mb4',
    
    // Authentication configuration (optional)
    // Option 1: API Key authentication
    'auth' => [
        'type' => 'api_key',
        'table' => 'api_keys',      // Table containing API keys
        'column' => 'key_value'     // Column name for API key
    ],
    
    // Option 2: User/Password authentication
    /*
    'auth' => [
        'type' => 'user_pass',
        'table' => 'users',         // Table containing users
        'user_column' => 'username', // Column name for username
        'pass_column' => 'password'  // Column name for password hash
    ],
    */
    
    // Rate limiting configuration (optional)
    // Authentication rate limiting (for login attempts)
    'auth_rate_limit' => [
        'max_attempts' => 5,        // Maximum failed login attempts
        'time_window' => 300,       // Time window in seconds (5 minutes)
        'lockout_time' => 900       // Lockout duration in seconds (15 minutes)
    ],
    
    // Request rate limiting (for all API requests/queries)
    'request_rate_limit' => [
        'max_requests' => 100,      // Maximum requests allowed
        'time_window' => 60         // Time window in seconds (1 minute)
    ],
    
    // Logging configuration (optional)
    // Option 1: Use the same database for logging
    'logging' => [
        'type' => 'database',
        'connection' => 'same',     // Use same database connection
        'rate_limit_table' => 'rate_limits',
        'auth_log_table' => 'auth_failures'
    ],
    
    // Option 2: Use a separate database for logging
    /*
    'logging' => [
        'type' => 'database',
        'connection' => [
            'driver' => 'mysql',
            'host' => 'localhost',
            'database' => 'logging_db',
            'username' => 'log_user',
            'password' => 'log_password',
            'charset' => 'utf8mb4'
        ],
        'rate_limit_table' => 'rate_limits',
        'auth_log_table' => 'auth_failures'
    ],
    */
    
    // Log file path (used as fallback if database logging fails, or if logging type is not 'database')
    'log_file' => __DIR__ . '/auth_failures.log'
];

/*
// PostgreSQL Example
$config = [
    'driver' => 'pgsql',
    'host' => 'localhost',
    'port' => 5432,
    'database' => 'your_database',
    'username' => 'your_username',
    'password' => 'your_password'
];

// SQLite Example
$config = [
    'driver' => 'sqlite',
    'database' => '/path/to/database.sqlite'
];

// SQL Server Example
$config = [
    'driver' => 'sqlsrv',
    'host' => 'localhost',
    'port' => 1433,
    'database' => 'your_database',
    'username' => 'your_username',
    'password' => 'your_password'
];
*/

// Initialize and handle request
$db = new DatabaseWrapper($config);
$db->handleRequest();

/*
EXAMPLE POST REQUESTS:

With API Key Authentication:
POST: {
    "api_key": "your-secret-api-key-here",
    "sql": "SELECT * FROM users WHERE id = ?",
    "params": [1]
}

With User/Password Authentication:
POST: {
    "username": "admin",
    "password": "admin_password",
    "sql": "SELECT * FROM products LIMIT 10"
}

1. SELECT query with parameters:
POST: {
    "api_key": "your-api-key",
    "sql": "SELECT * FROM users WHERE id = ? AND status = ?",
    "params": [1, "active"]
}

2. SELECT query without parameters:
POST: {
    "username": "admin",
    "password": "password123",
    "sql": "SELECT * FROM products LIMIT 10"
}

3. INSERT query:
POST: {
    "api_key": "your-api-key",
    "sql": "INSERT INTO users (name, email) VALUES (?, ?)",
    "params": ["John Doe", "john@example.com"]
}

4. UPDATE query:
POST: {
    "username": "admin",
    "password": "password123",
    "sql": "UPDATE users SET status = ? WHERE id = ?",
    "params": ["inactive", 5]
}

5. DELETE query:
POST: {
    "api_key": "your-api-key",
    "sql": "DELETE FROM users WHERE id = ?",
    "params": [5]
}

DATABASE SETUP EXAMPLES:

For API Key Authentication, create a table:
CREATE TABLE api_keys (
    id INT AUTO_INCREMENT PRIMARY KEY,
    key_value VARCHAR(255) UNIQUE NOT NULL,
    description VARCHAR(255),
    max_requests INT DEFAULT NULL,       -- NULL = use global limit, 0 = unlimited, >0 = specific limit
    time_window INT DEFAULT NULL,        -- Time window in seconds, NULL = use global setting
    expires_at DATETIME DEFAULT NULL,    -- NULL = never expires, otherwise expiration datetime
    table_permissions JSON DEFAULT NULL, -- JSON: {"SELECT":["table1","table2"],"INSERT":["table1"],"UPDATE":[],"DELETE":[]}
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO api_keys (key_value, description, max_requests, time_window, expires_at, table_permissions) VALUES 
('full-access-key', 'Admin Key - Full Access', 0, NULL, NULL, NULL),  -- NULL = full access to all tables
('readonly-key', 'Read-Only Key', 1000, 60, NULL, '{"SELECT":["products","orders","customers","analytics"],"INSERT":[],"UPDATE":[],"DELETE":[]}'),
('data-entry-key', 'Data Entry - Insert/Update Only', 500, 60, NULL, '{"SELECT":["products","orders"],"INSERT":["orders","order_items"],"UPDATE":["orders","order_items"],"DELETE":[]}'),
('analytics-key', 'Analytics - Read Only Reports', 100, 60, NULL, '{"SELECT":["analytics","reports","logs"],"INSERT":[],"UPDATE":[],"DELETE":[]}'),
('public-api-key', 'Public API - Products Only', NULL, NULL, NULL, '{"SELECT":["products","categories"],"INSERT":[],"UPDATE":[],"DELETE":[]}'),
('maintenance-key', 'Maintenance - Full CRUD on specific tables', 0, NULL, '2026-12-31 23:59:59', '{"SELECT":["orders","products","inventory"],"INSERT":["inventory"],"UPDATE":["orders","products","inventory"],"DELETE":["orders"]}');

For User/Password Authentication, create a table:
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    max_requests INT DEFAULT NULL,       -- NULL = use global limit, 0 = unlimited, >0 = specific limit
    time_window INT DEFAULT NULL,        -- Time window in seconds, NULL = use global setting
    expires_at DATETIME DEFAULT NULL,    -- NULL = never expires, otherwise expiration datetime
    table_permissions JSON DEFAULT NULL, -- JSON: {"SELECT":["table1","table2"],"INSERT":["table1"],"UPDATE":[],"DELETE":[]}
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert users with different permissions
-- Password: 'mypassword123'
INSERT INTO users (username, password, max_requests, time_window, expires_at, table_permissions) VALUES 
('admin', '$2y$10$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi', 0, NULL, NULL, NULL),  -- Full access
('manager', '$2y$10$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi', 500, 60, NULL, '{"SELECT":["products","orders","customers","inventory"],"INSERT":["orders","products"],"UPDATE":["orders","products","inventory"],"DELETE":["orders"]}'),
('analyst', '$2y$10$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi', 100, 60, NULL, '{"SELECT":["analytics","reports","orders","products"],"INSERT":[],"UPDATE":[],"DELETE":[]}'),
('customer_service', '$2y$10$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi', 200, 60, NULL, '{"SELECT":["orders","customers","products"],"INSERT":[],"UPDATE":["orders"],"DELETE":[]}'),
('data_entry', '$2y$10$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi', NULL, NULL, NULL, '{"SELECT":["products"],"INSERT":["products","inventory"],"UPDATE":["products","inventory"],"DELETE":[]}'),
('readonly_user', '$2y$10$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi', NULL, NULL, NULL, '{"SELECT":["products","orders","customers"],"INSERT":[],"UPDATE":[],"DELETE":[]}');

For Database Logging (tables will be auto-created, but here's the schema):

-- Rate limiting table (updated schema with identifier column)
CREATE TABLE rate_limits (
    id INT AUTO_INCREMENT PRIMARY KEY,
    ip_address VARCHAR(45) NOT NULL,
    identifier VARCHAR(255) DEFAULT NULL,  -- Format: 'api_key:key_value' or 'user:username' or 'ip:address'
    limit_type VARCHAR(20) NOT NULL DEFAULT 'auth',  -- 'auth' or 'request'
    attempt_count INT DEFAULT 0,
    locked_until INT DEFAULT NULL,
    last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_ip_type (ip_address, limit_type),
    INDEX idx_identifier_type (identifier, limit_type),
    INDEX idx_locked (locked_until),
    INDEX idx_last_attempt (last_attempt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Authentication failures log table
CREATE TABLE auth_failures (
    id INT AUTO_INCREMENT PRIMARY KEY,
    ip_address VARCHAR(45) NOT NULL,
    identifier VARCHAR(255),
    reason VARCHAR(255),
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ip (ip_address),
    INDEX idx_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

QUERYING LOGS FROM DATABASE:

-- View recent authentication failures
SELECT * FROM auth_failures ORDER BY created_at DESC LIMIT 100;

-- View failures by IP
SELECT ip_address, COUNT(*) as failure_count, MAX(created_at) as last_failure
FROM auth_failures 
GROUP BY ip_address 
ORDER BY failure_count DESC;

-- View currently locked IPs (authentication)
SELECT ip_address, FROM_UNIXTIME(locked_until) as locked_until_datetime, attempt_count
FROM rate_limits 
WHERE limit_type = 'auth' AND locked_until > UNIX_TIMESTAMP()
ORDER BY locked_until DESC;

-- View request rate limiting stats by user/api-key
SELECT identifier, COUNT(*) as request_count, 
       MIN(last_attempt) as first_request, 
       MAX(last_attempt) as last_request
FROM rate_limits 
WHERE limit_type = 'request' AND last_attempt > DATE_SUB(NOW(), INTERVAL 1 HOUR)
GROUP BY identifier
ORDER BY request_count DESC;

-- View top requesting users/API keys in the last hour
SELECT identifier, COUNT(*) as requests
FROM rate_limits
WHERE limit_type = 'request' AND last_attempt > DATE_SUB(NOW(), INTERVAL 1 HOUR)
GROUP BY identifier
ORDER BY requests DESC
LIMIT 20;

-- View API key permissions with detailed breakdown
SELECT 
    key_value, 
    description,
    CASE 
        WHEN table_permissions IS NULL THEN 'Full access to all tables'
        ELSE CONCAT(
            'SELECT: ', COALESCE(JSON_UNQUOTE(JSON_EXTRACT(table_permissions, '$.SELECT')), '[]'), ' | ',
            'INSERT: ', COALESCE(JSON_UNQUOTE(JSON_EXTRACT(table_permissions, '$.INSERT')), '[]'), ' | ',
            'UPDATE: ', COALESCE(JSON_UNQUOTE(JSON_EXTRACT(table_permissions, '$.UPDATE')), '[]'), ' | ',
            'DELETE: ', COALESCE(JSON_UNQUOTE(JSON_EXTRACT(table_permissions, '$.DELETE')), '[]')
        )
    END as permissions,
    CASE 
        WHEN max_requests IS NULL THEN 'Global limit'
        WHEN max_requests = 0 THEN 'Unlimited'
        ELSE CONCAT(max_requests, ' per ', time_window, 's')
    END as rate_limit,
    CASE 
        WHEN expires_at IS NULL THEN 'Never expires'
        WHEN expires_at > NOW() THEN CONCAT('Expires: ', expires_at)
        ELSE CONCAT('EXPIRED: ', expires_at)
    END as expiration_status
FROM api_keys;

-- View user permissions with detailed breakdown
SELECT 
    username,
    CASE 
        WHEN table_permissions IS NULL THEN 'Full access to all tables'
        ELSE CONCAT(
            'SELECT: ', COALESCE(JSON_UNQUOTE(JSON_EXTRACT(table_permissions, '$.SELECT')), '[]'), ' | ',
            'INSERT: ', COALESCE(JSON_UNQUOTE(JSON_EXTRACT(table_permissions, '$.INSERT')), '[]'), ' | ',
            'UPDATE: ', COALESCE(JSON_UNQUOTE(JSON_EXTRACT(table_permissions, '$.UPDATE')), '[]'), ' | ',
            'DELETE: ', COALESCE(JSON_UNQUOTE(JSON_EXTRACT(table_permissions, '$.DELETE')), '[]')
        )
    END as permissions,
    CASE 
        WHEN max_requests IS NULL THEN 'Global limit'
        WHEN max_requests = 0 THEN 'Unlimited'
        ELSE CONCAT(max_requests, ' per ', time_window, 's')
    END as rate_limit,
    CASE 
        WHEN expires_at IS NULL THEN 'Never expires'
        WHEN expires_at > NOW() THEN CONCAT('Expires: ', expires_at)
        ELSE CONCAT('EXPIRED: ', expires_at)
    END as expiration_status
FROM users;

-- Find API keys with SELECT permission on specific table
SELECT key_value, description
FROM api_keys
WHERE JSON_CONTAINS(table_permissions, '"products"', '$.SELECT');

-- Find users with INSERT permission on specific table
SELECT username
FROM users
WHERE JSON_CONTAINS(table_permissions, '"orders"', '$.INSERT');

-- Find users with no DELETE permissions on any table
SELECT username
FROM users
WHERE JSON_LENGTH(table_permissions, '$.DELETE') = 0;

-- Update permissions examples:

-- Grant SELECT access to a new table
UPDATE api_keys
SET table_permissions = JSON_ARRAY_APPEND(table_permissions, '$.SELECT', 'new_table')
WHERE key_value = 'readonly-key';

-- Remove DELETE permission from all tables
UPDATE users
SET table_permissions = JSON_SET(table_permissions, '$.DELETE', JSON_ARRAY())
WHERE username = 'data_entry';

-- Add full CRUD permissions for a specific table
UPDATE api_keys
SET table_permissions = JSON_SET(
    table_permissions,
    '$.SELECT', JSON_ARRAY_APPEND(COALESCE(JSON_EXTRACT(table_permissions, '$.SELECT'), JSON_ARRAY()), '

-- Find expired API keys
SELECT key_value, description, expires_at
FROM api_keys
WHERE expires_at IS NOT NULL AND expires_at < NOW();

-- Find expired users
SELECT username, expires_at
FROM users
WHERE expires_at IS NOT NULL AND expires_at < NOW();

-- Find API keys expiring soon (within 7 days)
SELECT key_value, description, expires_at, 
       DATEDIFF(expires_at, NOW()) as days_until_expiry
FROM api_keys
WHERE expires_at IS NOT NULL 
  AND expires_at > NOW() 
  AND expires_at < DATE_ADD(NOW(), INTERVAL 7 DAY)
ORDER BY expires_at;

-- Find users expiring soon (within 7 days)
SELECT username, expires_at,
       DATEDIFF(expires_at, NOW()) as days_until_expiry
FROM users
WHERE expires_at IS NOT NULL 
  AND expires_at > NOW() 
  AND expires_at < DATE_ADD(NOW(), INTERVAL 7 DAY)
ORDER BY expires_at;

-- Clean up expired API keys (optional maintenance task)
DELETE FROM api_keys 
WHERE expires_at IS NOT NULL AND expires_at < DATE_SUB(NOW(), INTERVAL 30 DAY);

-- Clean up expired users (optional maintenance task)
DELETE FROM users 
WHERE expires_at IS NOT NULL AND expires_at < DATE_SUB(NOW(), INTERVAL 30 DAY);

-- Clean up old logs (older than 30 days)
DELETE FROM auth_failures WHERE created_at < DATE_SUB(NOW(), INTERVAL 30 DAY);
DELETE FROM rate_limits WHERE limit_type = 'request' AND last_attempt < DATE_SUB(NOW(), INTERVAL 1 DAY);
*/
?>, 'inventory'),
    '$.INSERT', JSON_ARRAY_APPEND(COALESCE(JSON_EXTRACT(table_permissions, '$.INSERT'), JSON_ARRAY()), '

-- Find expired API keys
SELECT key_value, description, expires_at
FROM api_keys
WHERE expires_at IS NOT NULL AND expires_at < NOW();

-- Find expired users
SELECT username, expires_at
FROM users
WHERE expires_at IS NOT NULL AND expires_at < NOW();

-- Find API keys expiring soon (within 7 days)
SELECT key_value, description, expires_at, 
       DATEDIFF(expires_at, NOW()) as days_until_expiry
FROM api_keys
WHERE expires_at IS NOT NULL 
  AND expires_at > NOW() 
  AND expires_at < DATE_ADD(NOW(), INTERVAL 7 DAY)
ORDER BY expires_at;

-- Find users expiring soon (within 7 days)
SELECT username, expires_at,
       DATEDIFF(expires_at, NOW()) as days_until_expiry
FROM users
WHERE expires_at IS NOT NULL 
  AND expires_at > NOW() 
  AND expires_at < DATE_ADD(NOW(), INTERVAL 7 DAY)
ORDER BY expires_at;

-- Clean up expired API keys (optional maintenance task)
DELETE FROM api_keys 
WHERE expires_at IS NOT NULL AND expires_at < DATE_SUB(NOW(), INTERVAL 30 DAY);

-- Clean up expired users (optional maintenance task)
DELETE FROM users 
WHERE expires_at IS NOT NULL AND expires_at < DATE_SUB(NOW(), INTERVAL 30 DAY);

-- Clean up old logs (older than 30 days)
DELETE FROM auth_failures WHERE created_at < DATE_SUB(NOW(), INTERVAL 30 DAY);
DELETE FROM rate_limits WHERE limit_type = 'request' AND last_attempt < DATE_SUB(NOW(), INTERVAL 1 DAY);
*/
?>, 'inventory'),
    '$.UPDATE', JSON_ARRAY_APPEND(COALESCE(JSON_EXTRACT(table_permissions, '$.UPDATE'), JSON_ARRAY()), '

-- Find expired API keys
SELECT key_value, description, expires_at
FROM api_keys
WHERE expires_at IS NOT NULL AND expires_at < NOW();

-- Find expired users
SELECT username, expires_at
FROM users
WHERE expires_at IS NOT NULL AND expires_at < NOW();

-- Find API keys expiring soon (within 7 days)
SELECT key_value, description, expires_at, 
       DATEDIFF(expires_at, NOW()) as days_until_expiry
FROM api_keys
WHERE expires_at IS NOT NULL 
  AND expires_at > NOW() 
  AND expires_at < DATE_ADD(NOW(), INTERVAL 7 DAY)
ORDER BY expires_at;

-- Find users expiring soon (within 7 days)
SELECT username, expires_at,
       DATEDIFF(expires_at, NOW()) as days_until_expiry
FROM users
WHERE expires_at IS NOT NULL 
  AND expires_at > NOW() 
  AND expires_at < DATE_ADD(NOW(), INTERVAL 7 DAY)
ORDER BY expires_at;

-- Clean up expired API keys (optional maintenance task)
DELETE FROM api_keys 
WHERE expires_at IS NOT NULL AND expires_at < DATE_SUB(NOW(), INTERVAL 30 DAY);

-- Clean up expired users (optional maintenance task)
DELETE FROM users 
WHERE expires_at IS NOT NULL AND expires_at < DATE_SUB(NOW(), INTERVAL 30 DAY);

-- Clean up old logs (older than 30 days)
DELETE FROM auth_failures WHERE created_at < DATE_SUB(NOW(), INTERVAL 30 DAY);
DELETE FROM rate_limits WHERE limit_type = 'request' AND last_attempt < DATE_SUB(NOW(), INTERVAL 1 DAY);
*/
?>, 'inventory'),
    '$.DELETE', JSON_ARRAY_APPEND(COALESCE(JSON_EXTRACT(table_permissions, '$.DELETE'), JSON_ARRAY()), '

-- Find expired API keys
SELECT key_value, description, expires_at
FROM api_keys
WHERE expires_at IS NOT NULL AND expires_at < NOW();

-- Find expired users
SELECT username, expires_at
FROM users
WHERE expires_at IS NOT NULL AND expires_at < NOW();

-- Find API keys expiring soon (within 7 days)
SELECT key_value, description, expires_at, 
       DATEDIFF(expires_at, NOW()) as days_until_expiry
FROM api_keys
WHERE expires_at IS NOT NULL 
  AND expires_at > NOW() 
  AND expires_at < DATE_ADD(NOW(), INTERVAL 7 DAY)
ORDER BY expires_at;

-- Find users expiring soon (within 7 days)
SELECT username, expires_at,
       DATEDIFF(expires_at, NOW()) as days_until_expiry
FROM users
WHERE expires_at IS NOT NULL 
  AND expires_at > NOW() 
  AND expires_at < DATE_ADD(NOW(), INTERVAL 7 DAY)
ORDER BY expires_at;

-- Clean up expired API keys (optional maintenance task)
DELETE FROM api_keys 
WHERE expires_at IS NOT NULL AND expires_at < DATE_SUB(NOW(), INTERVAL 30 DAY);

-- Clean up expired users (optional maintenance task)
DELETE FROM users 
WHERE expires_at IS NOT NULL AND expires_at < DATE_SUB(NOW(), INTERVAL 30 DAY);

-- Clean up old logs (older than 30 days)
DELETE FROM auth_failures WHERE created_at < DATE_SUB(NOW(), INTERVAL 30 DAY);
DELETE FROM rate_limits WHERE limit_type = 'request' AND last_attempt < DATE_SUB(NOW(), INTERVAL 1 DAY);
*/
?>, 'inventory')
)
WHERE key_value = 'maintenance-key';

-- Find expired API keys
SELECT key_value, description, expires_at
FROM api_keys
WHERE expires_at IS NOT NULL AND expires_at < NOW();

-- Find expired users
SELECT username, expires_at
FROM users
WHERE expires_at IS NOT NULL AND expires_at < NOW();

-- Find API keys expiring soon (within 7 days)
SELECT key_value, description, expires_at, 
       DATEDIFF(expires_at, NOW()) as days_until_expiry
FROM api_keys
WHERE expires_at IS NOT NULL 
  AND expires_at > NOW() 
  AND expires_at < DATE_ADD(NOW(), INTERVAL 7 DAY)
ORDER BY expires_at;

-- Find users expiring soon (within 7 days)
SELECT username, expires_at,
       DATEDIFF(expires_at, NOW()) as days_until_expiry
FROM users
WHERE expires_at IS NOT NULL 
  AND expires_at > NOW() 
  AND expires_at < DATE_ADD(NOW(), INTERVAL 7 DAY)
ORDER BY expires_at;

-- Clean up expired API keys (optional maintenance task)
DELETE FROM api_keys 
WHERE expires_at IS NOT NULL AND expires_at < DATE_SUB(NOW(), INTERVAL 30 DAY);

-- Clean up expired users (optional maintenance task)
DELETE FROM users 
WHERE expires_at IS NOT NULL AND expires_at < DATE_SUB(NOW(), INTERVAL 30 DAY);

-- Clean up old logs (older than 30 days)
DELETE FROM auth_failures WHERE created_at < DATE_SUB(NOW(), INTERVAL 30 DAY);
DELETE FROM rate_limits WHERE limit_type = 'request' AND last_attempt < DATE_SUB(NOW(), INTERVAL 1 DAY);
*/
```
