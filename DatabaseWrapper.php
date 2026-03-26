<?php
/**
 * PDO Database Wrapper Class
 * Handles database connections and POST request query execution
 */
class DatabaseWrapper
{
    private $pdo;
    private $logPdo;
    private $config;
    private $logFile;
    private $rateLimitFile;
    private $useDbLogging;
    private $authenticatedIdentifier;

    /**
     * Constructor - Initialize database connection
     * 
     * @param array $config Configuration array with the following keys:
     *   - driver: Database driver (mysql, pgsql, sqlite, sqlsrv, etc.)
     *   - host: Database host (not needed for sqlite)
     *   - port: Database port (optional)
     *   - database: Database name or file path for sqlite
     *   - username: Database username (not needed for sqlite)
     *   - password: Database password (not needed for sqlite)
     *   - charset: Character set (optional, default: utf8mb4)
     *   - options: PDO options array (optional)
     *   - auth: Authentication configuration (optional) with one of:
     *       API Key method: ['type' => 'api_key', 'table' => 'table_name', 'column' => 'api_key_column']
     *       User/Password method: ['type' => 'user_pass', 'table' => 'table_name', 'user_column' => 'username', 'pass_column' => 'password']
     *   - auth_rate_limit: Rate limiting for authentication attempts (optional)
     *       ['max_attempts' => 5, 'time_window' => 300, 'lockout_time' => 900]
     *   - request_rate_limit: Rate limiting for all requests/queries (optional)
     *       ['max_requests' => 100, 'time_window' => 60]
     *   - log_file: Path to log file for failed authentication attempts (optional, default: 'auth_failures.log')
     *   - rate_limit_file: Path to rate limit data file (optional, default: 'rate_limit.json')
     *   - logging: Database logging configuration (optional)
     *       ['type' => 'database', 'connection' => 'same' or separate database config array,
     *        'rate_limit_table' => 'rate_limits', 'auth_log_table' => 'auth_failures', 'request_log_table' => 'request_logs']
     */
    public function __construct(array $config)
    {
        $this->config = $config;
        $this->logFile = $config['log_file'] ?? __DIR__ . '/auth_failures.log';
        $this->rateLimitFile = $config['rate_limit_file'] ?? __DIR__ . '/rate_limit.json';
        $this->useDbLogging = isset($config['logging']) && $config['logging']['type'] === 'database';
        
        $this->connect();
        
        // Initialize logging database connection if configured
        if ($this->useDbLogging) {
            $this->connectLoggingDatabase();
            $this->createLoggingTables();
        }
    }

    /**
     * Establish database connection
     */
    private function connect()
    {
        try {
            $driver = $this->config['driver'] ?? 'mysql';
            $charset = $this->config['charset'] ?? 'utf8mb4';
            
            // Build DSN based on driver
            switch ($driver) {
                case 'mysql':
                    $dsn = sprintf(
                        "mysql:host=%s;dbname=%s;charset=%s",
                        $this->config['host'],
                        $this->config['database'],
                        $charset
                    );
                    if (isset($this->config['port'])) {
                        $dsn .= ";port=" . $this->config['port'];
                    }
                    break;

                case 'pgsql':
                    $dsn = sprintf(
                        "pgsql:host=%s;dbname=%s",
                        $this->config['host'],
                        $this->config['database']
                    );
                    if (isset($this->config['port'])) {
                        $dsn .= ";port=" . $this->config['port'];
                    }
                    break;

                case 'sqlite':
                    $dsn = "sqlite:" . $this->config['database'];
                    break;

                case 'sqlsrv':
                    $dsn = sprintf(
                        "sqlsrv:Server=%s;Database=%s",
                        $this->config['host'],
                        $this->config['database']
                    );
                    if (isset($this->config['port'])) {
                        $dsn = sprintf(
                            "sqlsrv:Server=%s,%s;Database=%s",
                            $this->config['host'],
                            $this->config['port'],
                            $this->config['database']
                        );
                    }
                    break;

                default:
                    throw new Exception("Unsupported database driver: {$driver}");
            }

            // Default PDO options
            $options = [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::ATTR_EMULATE_PREPARES => false,
            ];

            // Merge with custom options if provided
            if (isset($this->config['options']) && is_array($this->config['options'])) {
                $options = array_replace($options, $this->config['options']);
            }

            // Create PDO connection
            $this->pdo = new PDO(
                $dsn,
                $this->config['username'] ?? null,
                $this->config['password'] ?? null,
                $options
            );

        } catch (PDOException $e) {
            $this->sendError("Database connection failed: " . $e->getMessage(), 500);
        }
    }

    /**
     * Connect to logging database
     */
    private function connectLoggingDatabase()
    {
        try {
            $loggingConfig = $this->config['logging'];
            
            // Use same database connection
            if ($loggingConfig['connection'] === 'same') {
                $this->logPdo = $this->pdo;
                return;
            }
            
            // Create separate database connection
            $logDbConfig = $loggingConfig['connection'];
            $driver = $logDbConfig['driver'] ?? 'mysql';
            $charset = $logDbConfig['charset'] ?? 'utf8mb4';
            
            // Build DSN based on driver
            switch ($driver) {
                case 'mysql':
                    $dsn = sprintf(
                        "mysql:host=%s;dbname=%s;charset=%s",
                        $logDbConfig['host'],
                        $logDbConfig['database'],
                        $charset
                    );
                    if (isset($logDbConfig['port'])) {
                        $dsn .= ";port=" . $logDbConfig['port'];
                    }
                    break;

                case 'pgsql':
                    $dsn = sprintf(
                        "pgsql:host=%s;dbname=%s",
                        $logDbConfig['host'],
                        $logDbConfig['database']
                    );
                    if (isset($logDbConfig['port'])) {
                        $dsn .= ";port=" . $logDbConfig['port'];
                    }
                    break;

                case 'sqlite':
                    $dsn = "sqlite:" . $logDbConfig['database'];
                    break;

                case 'sqlsrv':
                    $dsn = sprintf(
                        "sqlsrv:Server=%s;Database=%s",
                        $logDbConfig['host'],
                        $logDbConfig['database']
                    );
                    if (isset($logDbConfig['port'])) {
                        $dsn = sprintf(
                            "sqlsrv:Server=%s,%s;Database=%s",
                            $logDbConfig['host'],
                            $logDbConfig['port'],
                            $logDbConfig['database']
                        );
                    }
                    break;

                default:
                    throw new Exception("Unsupported logging database driver: {$driver}");
            }

            $options = [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::ATTR_EMULATE_PREPARES => false,
            ];

            $this->logPdo = new PDO(
                $dsn,
                $logDbConfig['username'] ?? null,
                $logDbConfig['password'] ?? null,
                $options
            );

        } catch (PDOException $e) {
            // Fall back to file logging if database connection fails
            $this->useDbLogging = false;
            error_log("Logging database connection failed: " . $e->getMessage());
        }
    }

    /**
     * Create logging tables if they don't exist
     */
    private function createLoggingTables()
    {
        try {
            $loggingConfig = $this->config['logging'];
            $rateLimitTable = $loggingConfig['rate_limit_table'] ?? 'rate_limits';
            $authLogTable = $loggingConfig['auth_log_table'] ?? 'auth_failures';
            
            $driver = $this->logPdo->getAttribute(PDO::ATTR_DRIVER_NAME);
            
            // Create rate_limits table
            if ($driver === 'mysql') {
                $sql = "CREATE TABLE IF NOT EXISTS {$rateLimitTable} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ip_address VARCHAR(45) NOT NULL,
                    identifier VARCHAR(255) DEFAULT NULL,
                    limit_type VARCHAR(20) NOT NULL DEFAULT 'auth',
                    attempt_count INT DEFAULT 0,
                    locked_until INT DEFAULT NULL,
                    last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_ip_type (ip_address, limit_type),
                    INDEX idx_identifier_type (identifier, limit_type),
                    INDEX idx_locked (locked_until),
                    INDEX idx_last_attempt (last_attempt)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
            } elseif ($driver === 'pgsql') {
                $sql = "CREATE TABLE IF NOT EXISTS {$rateLimitTable} (
                    id SERIAL PRIMARY KEY,
                    ip_address VARCHAR(45) NOT NULL,
                    identifier VARCHAR(255) DEFAULT NULL,
                    limit_type VARCHAR(20) NOT NULL DEFAULT 'auth',
                    attempt_count INT DEFAULT 0,
                    locked_until INT DEFAULT NULL,
                    last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_ip_type ON {$rateLimitTable}(ip_address, limit_type);
                CREATE INDEX IF NOT EXISTS idx_identifier_type ON {$rateLimitTable}(identifier, limit_type);
                CREATE INDEX IF NOT EXISTS idx_locked ON {$rateLimitTable}(locked_until);
                CREATE INDEX IF NOT EXISTS idx_last_attempt ON {$rateLimitTable}(last_attempt)";
            } elseif ($driver === 'sqlite') {
                $sql = "CREATE TABLE IF NOT EXISTS {$rateLimitTable} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ip_address VARCHAR(45) NOT NULL,
                    identifier VARCHAR(255) DEFAULT NULL,
                    limit_type VARCHAR(20) NOT NULL DEFAULT 'auth',
                    attempt_count INT DEFAULT 0,
                    locked_until INT DEFAULT NULL,
                    last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_ip_type ON {$rateLimitTable}(ip_address, limit_type);
                CREATE INDEX IF NOT EXISTS idx_identifier_type ON {$rateLimitTable}(identifier, limit_type);
                CREATE INDEX IF NOT EXISTS idx_locked ON {$rateLimitTable}(locked_until);
                CREATE INDEX IF NOT EXISTS idx_last_attempt ON {$rateLimitTable}(last_attempt)";
            } else {
                $sql = "CREATE TABLE {$rateLimitTable} (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    ip_address VARCHAR(45) NOT NULL,
                    identifier VARCHAR(255) DEFAULT NULL,
                    limit_type VARCHAR(20) NOT NULL DEFAULT 'auth',
                    attempt_count INT DEFAULT 0,
                    locked_until INT DEFAULT NULL,
                    last_attempt DATETIME DEFAULT GETDATE()
                )";
            }
            
            $this->logPdo->exec($sql);
            
            // Create auth_failures table
            if ($driver === 'mysql') {
                $sql = "CREATE TABLE IF NOT EXISTS {$authLogTable} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ip_address VARCHAR(45) NOT NULL,
                    identifier VARCHAR(255),
                    reason VARCHAR(255),
                    user_agent TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_ip (ip_address),
                    INDEX idx_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
            } elseif ($driver === 'pgsql') {
                $sql = "CREATE TABLE IF NOT EXISTS {$authLogTable} (
                    id SERIAL PRIMARY KEY,
                    ip_address VARCHAR(45) NOT NULL,
                    identifier VARCHAR(255),
                    reason VARCHAR(255),
                    user_agent TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_ip_auth ON {$authLogTable}(ip_address);
                CREATE INDEX IF NOT EXISTS idx_created_auth ON {$authLogTable}(created_at)";
            } elseif ($driver === 'sqlite') {
                $sql = "CREATE TABLE IF NOT EXISTS {$authLogTable} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ip_address VARCHAR(45) NOT NULL,
                    identifier VARCHAR(255),
                    reason VARCHAR(255),
                    user_agent TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_ip_auth ON {$authLogTable}(ip_address);
                CREATE INDEX IF NOT EXISTS idx_created_auth ON {$authLogTable}(created_at)";
            } else {
                $sql = "CREATE TABLE {$authLogTable} (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    ip_address VARCHAR(45) NOT NULL,
                    identifier VARCHAR(255),
                    reason VARCHAR(255),
                    user_agent TEXT,
                    created_at DATETIME DEFAULT GETDATE()
                )";
            }
            
            $this->logPdo->exec($sql);
            
        } catch (PDOException $e) {
            // If table creation fails, fall back to file logging
            $this->useDbLogging = false;
            error_log("Failed to create logging tables: " . $e->getMessage());
        }
    }

    /**
     * Handle POST requests and execute queries
     */
    public function handleRequest()
    {
        // Only accept POST requests
        if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
            $this->sendError("Only POST requests are allowed", 405);
            return;
        }

        // Get POST data
        $contentType = $_SERVER['CONTENT_TYPE'] ?? '';
        
        if (strpos($contentType, 'application/json') !== false) {
            $input = json_decode(file_get_contents('php://input'), true);
        } else {
            $input = $_POST;
        }

        $clientIp = $this->getClientIp();

        // Check request rate limit (applies to all requests)
        if (isset($this->config['request_rate_limit'])) {
            if ($this->isRequestRateLimited($clientIp)) {
                $this->logFailedAuth($clientIp, 'Request rate limit exceeded');
                $this->sendError("Too many requests. Please slow down.", 429);
                return;
            }
        }

        // Authenticate if authentication is configured
        if (isset($this->config['auth'])) {
            // Check auth rate limit before authentication
            if (isset($this->config['auth_rate_limit']) && $this->isAuthRateLimited($clientIp)) {
                $this->logFailedAuth($clientIp, 'Authentication rate limit exceeded');
                $this->sendError("Too many failed authentication attempts. Please try again later.", 429);
                return;
            }

            if (!$this->authenticate($input)) {
                $this->logFailedAuth($clientIp, 'Authentication failed', $input);
                $this->recordAuthFailedAttempt($clientIp);
                $this->sendError("Authentication failed", 401);
                return;
            }

            // Reset auth failed attempts on successful authentication
            $this->resetAuthFailedAttempts($clientIp);
        }

        // Validate required SQL parameter
        if (!isset($input['sql']) || empty(trim($input['sql']))) {
            $this->sendError("Missing required 'sql' parameter", 400);
            return;
        }

        $sql = trim($input['sql']);
        $params = $input['params'] ?? [];

        // Validate params is array
        if (!is_array($params)) {
            $this->sendError("'params' must be an array", 400);
            return;
        }

        // Record request for rate limiting
        if (isset($this->config['request_rate_limit'])) {
            $this->recordRequest($clientIp);
        }

        // Execute query
        $this->executeQuery($sql, $params);
    }

    /**
     * Authenticate the request based on configuration
     * 
     * @param array $input Request input data
     * @return bool True if authenticated, false otherwise
     */
    private function authenticate($input)
    {
        $authConfig = $this->config['auth'];
        $authType = $authConfig['type'] ?? null;

        try {
            if ($authType === 'api_key') {
                return $this->authenticateApiKey($input, $authConfig);
            } elseif ($authType === 'user_pass') {
                return $this->authenticateUserPassword($input, $authConfig);
            } else {
                $this->sendError("Invalid authentication type configured", 500);
                return false;
            }
        } catch (PDOException $e) {
            $this->sendError("Authentication error: " . $e->getMessage(), 500);
            return false;
        }
    }

    /**
     * Authenticate using API key
     * 
     * @param array $input Request input data
     * @param array $authConfig Authentication configuration
     * @return bool True if authenticated
     */
    private function authenticateApiKey($input, $authConfig)
    {
        // Check for API key in input
        if (!isset($input['api_key']) || empty($input['api_key'])) {
            return false;
        }

        $table = $authConfig['table'] ?? null;
        $column = $authConfig['column'] ?? null;

        if (!$table || !$column) {
            $this->sendError("Invalid API key authentication configuration", 500);
            return false;
        }

        // Query database to check API key and get rate limit settings
        $sql = "SELECT {$column}, max_requests, time_window, expires_at FROM {$table} WHERE {$column} = ?";
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute([$input['api_key']]);
        $result = $stmt->fetch();

        if ($result) {
            // Check if API key has expired
            if ($result['expires_at'] !== null) {
                $expiresAt = strtotime($result['expires_at']);
                if ($expiresAt !== false && $expiresAt < time()) {
                    return false; // API key has expired
                }
            }
            
            // Store the authenticated identifier and rate limit settings
            $this->authenticatedIdentifier = [
                'type' => 'api_key',
                'value' => $input['api_key'],
                'max_requests' => $result['max_requests'] ?? null,
                'time_window' => $result['time_window'] ?? null
            ];
            return true;
        }

        return false;
    }

    /**
     * Authenticate using username and password
     * 
     * @param array $input Request input data
     * @param array $authConfig Authentication configuration
     * @return bool True if authenticated
     */
    private function authenticateUserPassword($input, $authConfig)
    {
        // Check for username and password in input
        if (!isset($input['username']) || !isset($input['password'])) {
            return false;
        }

        if (empty($input['username']) || empty($input['password'])) {
            return false;
        }

        $table = $authConfig['table'] ?? null;
        $userColumn = $authConfig['user_column'] ?? null;
        $passColumn = $authConfig['pass_column'] ?? null;

        if (!$table || !$userColumn || !$passColumn) {
            $this->sendError("Invalid user/password authentication configuration", 500);
            return false;
        }

        // Query database to get user's password hash and rate limit settings
        $sql = "SELECT {$passColumn} as password, max_requests, time_window, expires_at FROM {$table} WHERE {$userColumn} = ?";
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute([$input['username']]);
        $result = $stmt->fetch();

        if (!$result) {
            return false;
        }

        // Check if user account has expired
        if ($result['expires_at'] !== null) {
            $expiresAt = strtotime($result['expires_at']);
            if ($expiresAt !== false && $expiresAt < time()) {
                return false; // User account has expired
            }
        }

        // Verify password (assumes password_hash() was used to store passwords)
        if (password_verify($input['password'], $result['password'])) {
            // Store the authenticated identifier and rate limit settings
            $this->authenticatedIdentifier = [
                'type' => 'user',
                'value' => $input['username'],
                'max_requests' => $result['max_requests'] ?? null,
                'time_window' => $result['time_window'] ?? null
            ];
            return true;
        }

        return false;
    }

    /**
     * Get client IP address
     * 
     * @return string Client IP address
     */
    private function getClientIp()
    {
        $ipKeys = [
            'HTTP_CF_CONNECTING_IP', // Cloudflare
            'HTTP_X_FORWARDED_FOR',
            'HTTP_X_REAL_IP',
            'REMOTE_ADDR'
        ];

        foreach ($ipKeys as $key) {
            if (!empty($_SERVER[$key])) {
                $ip = $_SERVER[$key];
                // Handle comma-separated IPs (X-Forwarded-For can have multiple)
                if (strpos($ip, ',') !== false) {
                    $ips = explode(',', $ip);
                    $ip = trim($ips[0]);
                }
                return $ip;
            }
        }

        return 'unknown';
    }

    /**
     * Check if IP is rate limited
     * 
     * @param string $ip Client IP address
     * @return bool True if rate limited
     */
    private function isAuthRateLimited($ip)
    {
        if (!isset($this->config['auth_rate_limit'])) {
            return false;
        }

        $maxAttempts = $this->config['auth_rate_limit']['max_attempts'] ?? 5;
        $timeWindow = $this->config['auth_rate_limit']['time_window'] ?? 300;
        $lockoutTime = $this->config['auth_rate_limit']['lockout_time'] ?? 900;

        if ($this->useDbLogging) {
            return $this->isAuthRateLimitedDb($ip, $maxAttempts, $timeWindow);
        } else {
            return $this->isAuthRateLimitedFile($ip, $maxAttempts, $timeWindow, $lockoutTime);
        }
    }

    /**
     * Check if IP is auth rate limited (database version)
     * 
     * @param string $ip Client IP address
     * @param int $maxAttempts Maximum attempts allowed
     * @param int $timeWindow Time window in seconds
     * @return bool True if rate limited
     */
    private function isAuthRateLimitedDb($ip, $maxAttempts, $timeWindow)
    {
        try {
            $loggingConfig = $this->config['logging'];
            $rateLimitTable = $loggingConfig['rate_limit_table'] ?? 'rate_limits';
            $currentTime = time();
            
            // Clean up expired records
            $sql = "DELETE FROM {$rateLimitTable} WHERE locked_until IS NOT NULL AND locked_until < ?";
            $stmt = $this->logPdo->prepare($sql);
            $stmt->execute([$currentTime]);
            
            // Check if IP is locked out
            $sql = "SELECT locked_until, attempt_count, last_attempt FROM {$rateLimitTable} WHERE ip_address = ? AND limit_type = 'auth'";
            $stmt = $this->logPdo->prepare($sql);
            $stmt->execute([$ip]);
            $result = $stmt->fetch();
            
            if (!$result) {
                return false;
            }
            
            // Check if currently locked
            if ($result['locked_until'] && $result['locked_until'] > $currentTime) {
                return true;
            }
            
            // Check if attempts are within time window
            $lastAttempt = strtotime($result['last_attempt']);
            if (($currentTime - $lastAttempt) < $timeWindow) {
                return $result['attempt_count'] >= $maxAttempts;
            }
            
            return false;
            
        } catch (PDOException $e) {
            error_log("Auth rate limit check failed: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Check if IP is auth rate limited (file version)
     * 
     * @param string $ip Client IP address
     * @param int $maxAttempts Maximum attempts allowed
     * @param int $timeWindow Time window in seconds
     * @param int $lockoutTime Lockout duration in seconds (unused but kept for consistency)
     * @return bool True if rate limited
     */
    private function isAuthRateLimitedFile($ip, $maxAttempts, $timeWindow, $lockoutTime)
    {
        $rateLimitData = $this->loadRateLimitData();
        $currentTime = time();

        if (!isset($rateLimitData['auth'][$ip])) {
            return false;
        }

        $ipData = $rateLimitData['auth'][$ip];

        // Check if IP is currently locked out
        if (isset($ipData['locked_until']) && $ipData['locked_until'] > $currentTime) {
            return true;
        }

        // Clean up old attempts outside the time window
        if (isset($ipData['attempts'])) {
            $ipData['attempts'] = array_filter($ipData['attempts'], function($timestamp) use ($currentTime, $timeWindow) {
                return ($currentTime - $timestamp) < $timeWindow;
            });
        }

        // Check if attempts exceed limit
        $attemptCount = isset($ipData['attempts']) ? count($ipData['attempts']) : 0;
        return $attemptCount >= $maxAttempts;
    }

    /**
     * Check if IP is request rate limited
     * 
     * @param string $ip Client IP address
     * @return bool True if rate limited
     */
    private function isRequestRateLimited($ip)
    {
        // Get rate limit settings (per-user/api-key or global)
        $rateLimitSettings = $this->getRequestRateLimitSettings();
        
        // If max_requests is 0, unlimited requests allowed
        if ($rateLimitSettings['max_requests'] === 0) {
            return false;
        }
        
        // If no rate limit configured, don't limit
        if ($rateLimitSettings['max_requests'] === null) {
            return false;
        }

        $maxRequests = $rateLimitSettings['max_requests'];
        $timeWindow = $rateLimitSettings['time_window'];

        if ($this->useDbLogging) {
            return $this->isRequestRateLimitedDb($ip, $maxRequests, $timeWindow);
        } else {
            return $this->isRequestRateLimitedFile($ip, $maxRequests, $timeWindow);
        }
    }

    /**
     * Get request rate limit settings for current user/api-key
     * 
     * @return array Array with max_requests and time_window
     */
    private function getRequestRateLimitSettings()
    {
        // Check if we have authenticated user/api-key specific limits
        if (isset($this->authenticatedIdentifier)) {
            $maxRequests = $this->authenticatedIdentifier['max_requests'];
            $timeWindow = $this->authenticatedIdentifier['time_window'];
            
            // If user/api-key has specific limits set (not null)
            if ($maxRequests !== null) {
                // Use user-specific time window or default to global
                if ($timeWindow === null && isset($this->config['request_rate_limit'])) {
                    $timeWindow = $this->config['request_rate_limit']['time_window'] ?? 60;
                }
                
                return [
                    'max_requests' => (int)$maxRequests,
                    'time_window' => (int)$timeWindow
                ];
            }
        }
        
        // Fallback to global request rate limit
        if (isset($this->config['request_rate_limit'])) {
            return [
                'max_requests' => $this->config['request_rate_limit']['max_requests'] ?? 100,
                'time_window' => $this->config['request_rate_limit']['time_window'] ?? 60
            ];
        }
        
        // No rate limit configured
        return [
            'max_requests' => null,
            'time_window' => null
        ];
    }

    /**
     * Check if IP is request rate limited (database version)
     * 
     * @param string $ip Client IP address
     * @param int $maxRequests Maximum requests allowed
     * @param int $timeWindow Time window in seconds
     * @return bool True if rate limited
     */
    private function isRequestRateLimitedDb($ip, $maxRequests, $timeWindow)
    {
        try {
            $loggingConfig = $this->config['logging'];
            $rateLimitTable = $loggingConfig['rate_limit_table'] ?? 'rate_limits';
            $currentTime = time();
            
            // Get identifier for this request
            $identifier = $this->getIdentifierForRateLimit();
            
            // Count requests within time window for this identifier
            $sql = "SELECT COUNT(*) as request_count FROM {$rateLimitTable} 
                    WHERE identifier = ? AND limit_type = 'request' 
                    AND last_attempt > ?";
            $stmt = $this->logPdo->prepare($sql);
            $stmt->execute([$identifier, date('Y-m-d H:i:s', $currentTime - $timeWindow)]);
            $result = $stmt->fetch();
            
            return $result['request_count'] >= $maxRequests;
            
        } catch (PDOException $e) {
            error_log("Request rate limit check failed: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Check if IP is request rate limited (file version)
     * 
     * @param string $ip Client IP address
     * @param int $maxRequests Maximum requests allowed
     * @param int $timeWindow Time window in seconds
     * @return bool True if rate limited
     */
    private function isRequestRateLimitedFile($ip, $maxRequests, $timeWindow)
    {
        $rateLimitData = $this->loadRateLimitData();
        $currentTime = time();
        
        // Get identifier for this request
        $identifier = $this->getIdentifierForRateLimit();

        if (!isset($rateLimitData['request'][$identifier])) {
            return false;
        }

        // Clean up old requests outside the time window
        $rateLimitData['request'][$identifier] = array_filter($rateLimitData['request'][$identifier], function($timestamp) use ($currentTime, $timeWindow) {
            return ($currentTime - $timestamp) < $timeWindow;
        });

        // Check if requests exceed limit
        $requestCount = count($rateLimitData['request'][$identifier]);
        return $requestCount >= $maxRequests;
    }

    /**
     * Get identifier for rate limiting (user, api_key, or IP)
     * 
     * @return string Identifier for rate limiting
     */
    private function getIdentifierForRateLimit()
    {
        if (isset($this->authenticatedIdentifier)) {
            $type = $this->authenticatedIdentifier['type'];
            $value = $this->authenticatedIdentifier['value'];
            return "{$type}:{$value}";
        }
        
        // Fallback to IP-based if no authentication
        return "ip:" . $this->getClientIp();
    }

    /**
     * Record a failed authentication attempt
     * 
     * @param string $ip Client IP address
     */
    private function recordAuthFailedAttempt($ip)
    {
        if (!isset($this->config['auth_rate_limit'])) {
            return;
        }

        $maxAttempts = $this->config['auth_rate_limit']['max_attempts'] ?? 5;
        $timeWindow = $this->config['auth_rate_limit']['time_window'] ?? 300;
        $lockoutTime = $this->config['auth_rate_limit']['lockout_time'] ?? 900;

        if ($this->useDbLogging) {
            $this->recordAuthFailedAttemptDb($ip, $maxAttempts, $timeWindow, $lockoutTime);
        } else {
            $this->recordAuthFailedAttemptFile($ip, $maxAttempts, $timeWindow, $lockoutTime);
        }
    }

    /**
     * Record failed auth attempt in database
     * 
     * @param string $ip Client IP address
     * @param int $maxAttempts Maximum attempts allowed
     * @param int $timeWindow Time window in seconds
     * @param int $lockoutTime Lockout duration in seconds
     */
    private function recordAuthFailedAttemptDb($ip, $maxAttempts, $timeWindow, $lockoutTime)
    {
        try {
            $loggingConfig = $this->config['logging'];
            $rateLimitTable = $loggingConfig['rate_limit_table'] ?? 'rate_limits';
            $currentTime = time();
            
            // Get current record
            $sql = "SELECT id, attempt_count, last_attempt FROM {$rateLimitTable} WHERE ip_address = ? AND limit_type = 'auth'";
            $stmt = $this->logPdo->prepare($sql);
            $stmt->execute([$ip]);
            $result = $stmt->fetch();
            
            if ($result) {
                $lastAttempt = strtotime($result['last_attempt']);
                $attemptCount = $result['attempt_count'];
                
                // Reset counter if outside time window
                if (($currentTime - $lastAttempt) >= $timeWindow) {
                    $attemptCount = 0;
                }
                
                $attemptCount++;
                
                // Check if should lock
                $lockedUntil = null;
                if ($attemptCount >= $maxAttempts) {
                    $lockedUntil = $currentTime + $lockoutTime;
                }
                
                // Update record
                $sql = "UPDATE {$rateLimitTable} SET attempt_count = ?, locked_until = ?, last_attempt = CURRENT_TIMESTAMP WHERE id = ?";
                $stmt = $this->logPdo->prepare($sql);
                $stmt->execute([$attemptCount, $lockedUntil, $result['id']]);
                
            } else {
                // Insert new record
                $sql = "INSERT INTO {$rateLimitTable} (ip_address, limit_type, attempt_count, locked_until) VALUES (?, 'auth', 1, NULL)";
                $stmt = $this->logPdo->prepare($sql);
                $stmt->execute([$ip]);
            }
            
        } catch (PDOException $e) {
            error_log("Failed to record auth rate limit attempt: " . $e->getMessage());
        }
    }

    /**
     * Record failed auth attempt in file
     * 
     * @param string $ip Client IP address
     * @param int $maxAttempts Maximum attempts allowed
     * @param int $timeWindow Time window in seconds
     * @param int $lockoutTime Lockout duration in seconds
     */
    private function recordAuthFailedAttemptFile($ip, $maxAttempts, $timeWindow, $lockoutTime)
    {
        $rateLimitData = $this->loadRateLimitData();
        $currentTime = time();

        if (!isset($rateLimitData['auth'])) {
            $rateLimitData['auth'] = [];
        }

        if (!isset($rateLimitData['auth'][$ip])) {
            $rateLimitData['auth'][$ip] = ['attempts' => []];
        }

        // Add current attempt
        $rateLimitData['auth'][$ip]['attempts'][] = $currentTime;

        // Clean up old attempts
        $rateLimitData['auth'][$ip]['attempts'] = array_filter($rateLimitData['auth'][$ip]['attempts'], function($timestamp) use ($currentTime, $timeWindow) {
            return ($currentTime - $timestamp) < $timeWindow;
        });

        // If max attempts reached, lock out the IP
        if (count($rateLimitData['auth'][$ip]['attempts']) >= $maxAttempts) {
            $rateLimitData['auth'][$ip]['locked_until'] = $currentTime + $lockoutTime;
        }

        $this->saveRateLimitData($rateLimitData);
    }

    /**
     * Record a request for rate limiting
     * 
     * @param string $ip Client IP address
     */
    private function recordRequest($ip)
    {
        if (!isset($this->config['request_rate_limit'])) {
            return;
        }

        if ($this->useDbLogging) {
            $this->recordRequestDb($ip);
        } else {
            $this->recordRequestFile($ip);
        }
    }

    /**
     * Record request in database
     * 
     * @param string $ip Client IP address
     */
    private function recordRequestDb($ip)
    {
        try {
            $loggingConfig = $this->config['logging'];
            $rateLimitTable = $loggingConfig['rate_limit_table'] ?? 'rate_limits';
            
            // Get identifier for this request
            $identifier = $this->getIdentifierForRateLimit();
            
            // Insert new request record
            $sql = "INSERT INTO {$rateLimitTable} (ip_address, identifier, limit_type, attempt_count) VALUES (?, ?, 'request', 1)";
            $stmt = $this->logPdo->prepare($sql);
            $stmt->execute([$ip, $identifier]);
            
            // Clean up old records
            $rateLimitSettings = $this->getRequestRateLimitSettings();
            $timeWindow = $rateLimitSettings['time_window'] ?? 60;
            $sql = "DELETE FROM {$rateLimitTable} WHERE limit_type = 'request' AND last_attempt < ?";
            $stmt = $this->logPdo->prepare($sql);
            $stmt->execute([date('Y-m-d H:i:s', time() - $timeWindow - 3600)]); // Keep extra hour for safety
            
        } catch (PDOException $e) {
            error_log("Failed to record request: " . $e->getMessage());
        }
    }

    /**
     * Record request in file
     * 
     * @param string $ip Client IP address
     */
    private function recordRequestFile($ip)
    {
        $rateLimitData = $this->loadRateLimitData();
        $currentTime = time();
        
        // Get identifier for this request
        $identifier = $this->getIdentifierForRateLimit();
        
        $rateLimitSettings = $this->getRequestRateLimitSettings();
        $timeWindow = $rateLimitSettings['time_window'] ?? 60;

        if (!isset($rateLimitData['request'])) {
            $rateLimitData['request'] = [];
        }

        if (!isset($rateLimitData['request'][$identifier])) {
            $rateLimitData['request'][$identifier] = [];
        }

        // Add current request
        $rateLimitData['request'][$identifier][] = $currentTime;

        // Clean up old requests
        $rateLimitData['request'][$identifier] = array_filter($rateLimitData['request'][$identifier], function($timestamp) use ($currentTime, $timeWindow) {
            return ($currentTime - $timestamp) < $timeWindow;
        });

        $this->saveRateLimitData($rateLimitData);
    }

    /**
     * Reset failed attempts for an IP
     * 
     * @param string $ip Client IP address
     */
    private function resetAuthFailedAttempts($ip)
    {
        if ($this->useDbLogging) {
            $this->resetAuthFailedAttemptsDb($ip);
        } else {
            $this->resetAuthFailedAttemptsFile($ip);
        }
    }

    /**
     * Reset auth failed attempts in database
     * 
     * @param string $ip Client IP address
     */
    private function resetAuthFailedAttemptsDb($ip)
    {
        try {
            $loggingConfig = $this->config['logging'];
            $rateLimitTable = $loggingConfig['rate_limit_table'] ?? 'rate_limits';
            
            $sql = "DELETE FROM {$rateLimitTable} WHERE ip_address = ? AND limit_type = 'auth'";
            $stmt = $this->logPdo->prepare($sql);
            $stmt->execute([$ip]);
            
        } catch (PDOException $e) {
            error_log("Failed to reset auth rate limit: " . $e->getMessage());
        }
    }

    /**
     * Reset auth failed attempts in file
     * 
     * @param string $ip Client IP address
     */
    private function resetAuthFailedAttemptsFile($ip)
    {
        $rateLimitData = $this->loadRateLimitData();
        
        if (isset($rateLimitData['auth'][$ip])) {
            unset($rateLimitData['auth'][$ip]);
            $this->saveRateLimitData($rateLimitData);
        }
    }

    /**
     * Load rate limit data from file
     * 
     * @return array Rate limit data
     */
    private function loadRateLimitData()
    {
        if (!file_exists($this->rateLimitFile)) {
            return [];
        }

        $data = file_get_contents($this->rateLimitFile);
        $decoded = json_decode($data, true);
        
        return is_array($decoded) ? $decoded : [];
    }

    /**
     * Save rate limit data to file
     * 
     * @param array $data Rate limit data
     */
    private function saveRateLimitData($data)
    {
        // Clean up expired lockouts and old data
        $currentTime = time();
        foreach ($data as $ip => $ipData) {
            if (isset($ipData['locked_until']) && $ipData['locked_until'] < $currentTime) {
                unset($data[$ip]['locked_until']);
            }
            if (empty($data[$ip]['attempts']) && !isset($data[$ip]['locked_until'])) {
                unset($data[$ip]);
            }
        }

        file_put_contents($this->rateLimitFile, json_encode($data, JSON_PRETTY_PRINT));
    }

    /**
     * Log failed authentication attempt
     * 
     * @param string $ip Client IP address
     * @param string $reason Failure reason
     * @param array $input Request input data (optional)
     */
    private function logFailedAuth($ip, $reason, $input = [])
    {
        $identifier = '';

        if (isset($input['api_key'])) {
            $identifier = 'API Key: ' . substr($input['api_key'], 0, 10) . '...';
        } elseif (isset($input['username'])) {
            $identifier = 'Username: ' . $input['username'];
        }

        $userAgent = $_SERVER['HTTP_USER_AGENT'] ?? 'Unknown';

        if ($this->useDbLogging) {
            $this->logFailedAuthDb($ip, $identifier, $reason, $userAgent);
        } else {
            $this->logFailedAuthFile($ip, $identifier, $reason, $userAgent);
        }
    }

    /**
     * Log failed authentication to database
     * 
     * @param string $ip Client IP address
     * @param string $identifier Username or API key identifier
     * @param string $reason Failure reason
     * @param string $userAgent User agent string
     */
    private function logFailedAuthDb($ip, $identifier, $reason, $userAgent)
    {
        try {
            $loggingConfig = $this->config['logging'];
            $authLogTable = $loggingConfig['auth_log_table'] ?? 'auth_failures';
            
            $sql = "INSERT INTO {$authLogTable} (ip_address, identifier, reason, user_agent) VALUES (?, ?, ?, ?)";
            $stmt = $this->logPdo->prepare($sql);
            $stmt->execute([$ip, $identifier, $reason, $userAgent]);
            
        } catch (PDOException $e) {
            error_log("Failed to log authentication failure: " . $e->getMessage());
            // Fall back to file logging
            $this->logFailedAuthFile($ip, $identifier, $reason, $userAgent);
        }
    }

    /**
     * Log failed authentication to file
     * 
     * @param string $ip Client IP address
     * @param string $identifier Username or API key identifier
     * @param string $reason Failure reason
     * @param string $userAgent User agent string
     */
    private function logFailedAuthFile($ip, $identifier, $reason, $userAgent)
    {
        $timestamp = date('Y-m-d H:i:s');
        
        $logEntry = sprintf(
            "[%s] IP: %s | %s | Reason: %s | User-Agent: %s\n",
            $timestamp,
            $ip,
            $identifier,
            $reason,
            $userAgent
        );

        file_put_contents($this->logFile, $logEntry, FILE_APPEND | LOCK_EX);
    }

    /**
     * Execute SQL query with optional parameters
     * 
     * @param string $sql SQL query
     * @param array $params Query parameters
     */
    private function executeQuery($sql, $params = [])
    {
        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute($params);

            // Determine query type
            $queryType = strtoupper(substr(ltrim($sql), 0, 6));

            if ($queryType === 'SELECT' || $queryType === 'SHOW' || strpos(strtoupper($sql), 'RETURNING') !== false) {
                // Fetch all results for SELECT queries
                $data = $stmt->fetchAll();
                $this->sendSuccess([
                    'rows' => $data,
                    'count' => count($data)
                ]);
            } else {
                // For INSERT, UPDATE, DELETE, etc.
                $this->sendSuccess([
                    'affected_rows' => $stmt->rowCount(),
                    'last_insert_id' => $this->pdo->lastInsertId()
                ]);
            }

        } catch (PDOException $e) {
            $this->sendError("Query execution failed: " . $e->getMessage(), 400);
        }
    }

    /**
     * Send success JSON response
     * 
     * @param mixed $data Response data
     */
    private function sendSuccess($data)
    {
        header('Content-Type: application/json');
        http_response_code(200);
        echo json_encode([
            'success' => true,
            'data' => $data
        ], JSON_PRETTY_PRINT);
        exit;
    }

    /**
     * Send error JSON response
     * 
     * @param string $message Error message
     * @param int $code HTTP status code
     */
    private function sendError($message, $code = 400)
    {
        header('Content-Type: application/json');
        http_response_code($code);
        echo json_encode([
            'success' => false,
            'error' => $message
        ], JSON_PRETTY_PRINT);
        exit;
    }
}
